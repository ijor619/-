"""Телеграм-бот: слежение за ценами акций Мосбиржи.

Запуск:
    export BOT_TOKEN="123456:ABC..."   (или положить в .env)
    python bot.py
"""
from __future__ import annotations

import asyncio
from typing import Optional
import logging
import os
from datetime import datetime, timedelta
import re
import sys

import aiohttp
from aiohttp import ClientTimeout
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (BotCommand, BotCommandScopeAllPrivateChats,
                           BotCommandScopeDefault, BufferedInputFile,
                           CallbackQuery, InputMediaPhoto, KeyboardButton, Message,
                           ReplyKeyboardMarkup)

import charts
import cleaner
import screener as scrmod
import clusters as clu
import cscalp
import events as evmod
import market
import moex
import setup as stp
import tape
import tinkoff
from flow import FlowMonitor
from journal import Journal
from ideas import IdeaJournal, card
from news import NewsMonitor, NEWS_CHANNEL
from keyboards import (book_kb, chart_kb, cluster_kb, report_kb, setup_kb, tape_kb, screener_kb,
                       scr_pick_kb, idea_kb)
from config import (BOT_TOKEN, CHECK_INTERVAL_SEC, DATA_FILE,
                    DEFAULT_REPORT_MIN, DEFAULT_THRESHOLD_PCT)
from formatting import cur_symbol, esc, fmt_pct, fmt_price
from monitor import Monitor
from store import Store

log = logging.getLogger(__name__)

BUILD_VERSION = "4.1.1-stability-2026-11-07"

router = Router(name="stockbot")

# ------------------------------------------------------------ приватность
# Бот личный: отвечает только Telegram-ID из OWNER_ID (через запятую можно
# несколько). Если переменная не задана — бот открыт для всех (как раньше).
_ALLOWED = {int(x) for x in os.getenv("OWNER_ID", "").replace(";", ",").split(",")
            if x.strip().lstrip("-").isdigit()}
_denied_logged: set[int] = set()


@router.message.outer_middleware()
@router.callback_query.outer_middleware()
async def _private_guard(handler, event, data):
    if not _ALLOWED:
        return await handler(event, data)
    user = getattr(event, "from_user", None)
    if user and user.id in _ALLOWED:
        return await handler(event, data)
    uid = user.id if user else 0
    if uid not in _denied_logged:
        _denied_logged.add(uid)
        log.warning("доступ запрещён: id=%s @%s", uid, getattr(user, "username", None))
    try:
        if isinstance(event, CallbackQuery):
            await event.answer("Бот приватный.", show_alert=True)
        else:
            await event.answer("🔒 Это личный бот, доступ закрыт.")
    except Exception:
        pass
    return None

TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,9}$")

HELP = (
    f"📈 Я слежу за акциями Мосбиржи и присылаю изменения цен.\n"
    f"<i>Сборка {BUILD_VERSION}</i>\n\n"
    "<b>Команды</b>\n"
    "/watch TICKER [TICKER …] — добавить акции, напр. /watch SBER GAZP\n"
    "/unwatch TICKER — убрать из списка\n"
    "/screener [15m|30m|1h|4h|1d|vol] — скринер: топ-10 роста/падения; vol — всплески объёма\n"
    "/market — рынок сейчас: IMOEX, RTS, юань, доллар, Brent\n"
    "/calendar [дней] — дивиденды, заседания ЦБ, отчётности по списку; /event add|del — свои события\n"
    "/list — список с текущими ценами\n"
    "/alert N — алерт, если цена уйдёт более чем на N% за день (0.1–50)\n"
    "/quiet N — мин. пауза между повторными алертами, мин\n"
    "/report N — сводка каждые N минут, 0 — выключить\n"
    "/chart TICKER [1m|5m|15m|30m|1h|4h|1d] — свечной график\n"
    "/book TICKER — стакан, /tape TICKER — лента сделок\n"
    "/ideas [30] [SBER] — активные идеи и статистика цель/стоп/отмена\n"
    "/risk 1000 — риск на одну идею в рублях (для расчёта лотов)\n"
    "/diag — версия и состояние источников данных\n"
    "/flow on|off — торговые идеи; /flow raw on — диагностические события ленты\n"
    "/stats [7] [TICKER] — точность сигналов за N дней\n"
    "/backtest TICKER — прогнать детекторы по ленте за час\n"
    "/news — статус парсера новостей и последние релевантные\n"
    "/quiethours 23 9 — тихие часы (МСК), off — выключить\n"
    "/clusters SBER [5m|15m|30m|1h|4h|1d] — кластеры: объём по ценам и времени, перевес покупок/продаж\n"
    "/clear — удалить сообщения бота в этом чате сейчас; /autoclear 3|off — ежедневная очистка в N часов МСК\n"
    "/cscalp SBER — открыть бумагу в CScalp на вашем ПК (нужен мостик, см. README)\n"
    "/setup SBER — сетап: уровни по объёму и их тесты, дельта, VWAP, стакан, сила к рынку, итог за/против\n\n"
    "Просто напиши тикер сообщением — добавлю в список.\n"
    "Цены — из официального ISS API Мосбиржи, обновление каждую минуту."
)


def _help_text(store: Store, user_id: int) -> str:
    p = store.get(user_id)
    return (
        HELP
        + f"\n\n<b>Твои настройки</b>\n"
        f"Список: {', '.join(p.watchlist) or '—'}\n"
        f"Порог алерта: {p.threshold_pct:g}%\n"
        f"Пауза между алертами: {p.cooldown_min:g} мин\n"
        f"Сводка: "
        + (f"каждые {p.report_min:g} мин" if p.report_min > 0 else "выключена")
        + "\nСигналы ленты/стакана: "
        + ("нет T-Invest токена" if not tinkoff.enabled()
           else "включены" if p.flow_alerts else "выключены")
        + f"\nТорговые идеи: {'включены' if p.idea_alerts else 'выключены'}"
        + f" · мин. оценка {p.idea_min_conf}/100 · риск {p.risk_rub:g} ₽"
        + f"\nСырые события ленты: {'включены' if p.raw_flow_alerts else 'выключены'}"
        + "\nТихие часы: "
        + (f"{p.quiet_from:02d}:00–{p.quiet_to:02d}:00 МСК" if p.quiet_from >= 0 else "нет")
        + "\nАвтоочистка чата: "
        + (f"ежедневно в {p.clear_hour:02d}:00 МСК" if p.clear_hour >= 0 else "выключена")
    )


async def _watch(m: Message, store: Store, sess: aiohttp.ClientSession,
                 tickers: list[str]) -> str:
    uid = m.from_user.id
    prof = store.get(uid)
    lines = []
    for raw in tickers:
        t = raw.upper().strip()
        if not TICKER_RE.match(t):
            lines.append(f"❓ «{esc(raw)}» — похоже, это не тикер (латиница, до 10 символов)")
            continue
        try:
            info = await moex.get_security_info(sess, t)
        except Exception:
            log.exception("ошибка проверки тикера %s", t)
            info = None
        if info is None:
            lines.append(f"❌ <b>{esc(t)}</b> — не найден на Мосбирже")
            continue
        if t not in prof.watchlist:
            prof.watchlist.append(t)
        lines.append(f"✅ <b>{t}</b> — {esc(info.name)}")
    store.save()
    lines.append(f"\nСледим за: {', '.join(prof.watchlist) or '—'}")
    return "\n".join(lines)


async def _list(m, store: Store, sess: aiohttp.ClientSession, tk=None) -> str:
    prof = store.get(m.from_user.id)
    if not prof.watchlist:
        return "Список пуст. Добавь: /watch SBER GAZP"
    lines = [f"📋 <b>Твои акции</b> · {moex.now_msk().strftime('%d.%m %H:%M')} МСК"]
    infos = {}
    for t in prof.watchlist:
        try:
            info = await moex.get_security_info(sess, t)
        except Exception:
            info = None
        if info:
            infos[t] = info
    quotes = await moex.fetch_quotes(sess, infos, {}, tk if tinkoff.enabled() else None)
    for t in prof.watchlist:
        q = quotes.get(t)
        if q is None:
            lines.append(f"⚪ <b>{esc(t)}</b> — нет данных")
        else:
            cur = cur_symbol(q.info.currency)
            arrow = ("⬆️" if q.change_pct > 0.005
                     else "⬇️" if q.change_pct < -0.005 else "⚪")
            lines.append(
                f"{arrow} <b>{t}</b> · {fmt_price(q.price, q.info.decimals)} {cur} · "
                f"{fmt_pct(q.change_pct)}\n{q.details()}"
            )
    return "\n".join(lines)


# id единственного сообщения-графика в каждом чате: chat_id -> message_id
_chart_msg: dict[int, int] = {}


async def show_chart(bot: Bot, chat_id: int, sess: aiohttp.ClientSession,
                     store: Store, t: str, period: str,
                     current: Message | None = None, tk=None,
                     reply_to: int | None = None, flow: "FlowMonitor | None" = None) -> str | None:
    """Показать график, не плодя сообщений.

    Если `current` — уже сообщение с графиком, картинка заменяется на месте.
    Иначе предыдущее сообщение-график в чате удаляется и присылается новое.
    Возвращает текст ошибки или None.
    """
    if period not in charts.PERIODS:
        period = charts.DEFAULT_PERIOD
    info = await moex.get_security_info(sess, t)
    if info is None:
        return f"❌ <b>{esc(t)}</b> — не найден на Мосбирже"
    vol_levels = None
    if flow is not None and tinkoff.enabled():
        try:
            price = flow._last_price.get(t) or info.prev_close
            vol_levels = [(l.price, l.vol, l.buy_pct, sum(1 for x in l.tests if x.held is True))
                          for l in flow.levels(t, price, info.decimals)]
        except Exception:
            log.exception("chart levels %s", t)
    png = await charts.build_chart(sess, info, period,
                                   tk if tinkoff.enabled() else None, vol_levels=vol_levels)
    if png is None:
        return f"Нет данных для графика {esc(t)}."
    file = BufferedInputFile(png, f"{t}_{period}.png")
    kb = chart_kb(t, period, store.get(chat_id).watchlist)

    if current is not None and current.photo:
        try:
            await current.edit_media(InputMediaPhoto(media=file), reply_markup=kb)
            _chart_msg[chat_id] = current.message_id
            return None
        except TelegramBadRequest as e:
            if "not modified" in str(e):
                return None
            log.warning("edit_media %s: %s — шлю новое сообщение", t, e)
        except Exception:
            log.exception("edit_media %s", t)

    if reply_to is not None:
        # график в ответ на новость/сигнал: цитатой, старые не трогаем
        await bot.send_photo(chat_id, file, reply_markup=kb,
                             reply_to_message_id=reply_to)
        return None
    old = _chart_msg.get(chat_id)
    if old is not None:
        try:
            await bot.delete_message(chat_id, old)
        except Exception:
            pass  # уже удалено / старше 48 ч
    msg = await bot.send_photo(chat_id, file, reply_markup=kb)
    _chart_msg[chat_id] = msg.message_id
    return None


# ------------------------------------------------------------------ команды

@router.message(CommandStart())
async def cmd_start(m: Message, store: Store) -> None:
    store.get(m.from_user.id)
    store.save()
    await m.answer(
        f"Привет! Я слежу за ценами акций на Мосбирже 📈\n"
        f"<i>Сборка {BUILD_VERSION}</i>\n\n"
        "Добавь тикеры: /watch SBER GAZP\n"
        "Или просто напиши тикер обычным сообщением.\n\n"
        "Вся информация — /help. Кнопки внизу — частые действия, "
        "полный список команд — в кнопке «Меню» слева от поля ввода.",
        reply_markup=main_kb(store.get(m.from_user.id).watchlist),
    )


@router.message(Command("help"))
async def cmd_help(m: Message, store: Store) -> None:
    await m.answer(_help_text(store, m.from_user.id),
                   reply_markup=main_kb(store.get(m.from_user.id).watchlist))


@router.message(Command("version"))
async def cmd_version(m: Message) -> None:
    await m.answer(f"🧱 <b>StockBot {BUILD_VERSION}</b>\n"
                   "Команды идей: /ideas · /risk · /flow raw on")


@router.message(Command("diag"))
async def cmd_diag(m: Message, store: Store, flow: FlowMonitor,
                   scr: scrmod.Screener) -> None:
    """Короткая самодиагностика без раскрытия токенов и OWNER_ID."""
    prof = store.get(m.from_user.id)
    data_dir = os.path.dirname(DATA_FILE) or "."
    stream = flow.stream
    stream_state = ("работает" if stream.healthy() else
                    "доступен, ждёт данные" if stream.available else
                    "REST-режим")
    updated = scr.updated.strftime("%d.%m %H:%M:%S МСК") if scr.updated else "ещё нет"
    lines = [
        f"🧱 <b>StockBot {BUILD_VERSION}</b>",
        f"Команд в меню: <b>{len(BOT_COMMANDS)}</b>",
        f"Бумаги: <b>{', '.join(prof.watchlist) or 'список пуст'}</b>",
        f"T‑Invest: <b>{'подключён' if tinkoff.enabled() else 'нет токена'}</b>",
        f"Поток: <b>{stream_state}</b>",
        f"Скринер: <b>{len(scr.secs)} бумаг</b> · обновление {updated}",
        f"Хранилище: <b>{'доступно' if os.access(data_dir, os.W_OK) else 'нет записи'}</b>",
    ]
    if scr.error:
        lines.append(f"Ошибка скринера: <code>{esc(scr.error[:300])}</code>")
    if stream.last_error:
        lines.append(f"Ошибка потока: <code>{esc(stream.last_error[:300])}</code>")
    await m.answer("\n".join(lines))


@router.message(Command("watch"))
async def cmd_watch(m: Message, store: Store, sess: aiohttp.ClientSession) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        await m.answer("Использование: /watch SBER GAZP\nПросто напиши тикер — тоже сработает.")
        return
    await m.answer(await _watch(m, store, sess, args))


@router.message(Command("unwatch"))
async def cmd_unwatch(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        await m.answer("Использование: /unwatch SBER")
        return
    prof = store.get(m.from_user.id)
    lines = []
    for raw in args:
        t = raw.upper().strip()
        if t in prof.watchlist:
            prof.watchlist.remove(t)
            lines.append(f"🗑 <b>{t}</b> — убран")
        else:
            lines.append(f"❓ <b>{esc(t)}</b> — не в списке")
    store.save()
    lines.append(f"\nСледим за: {', '.join(prof.watchlist) or '—'}")
    await m.answer("\n".join(lines))


@router.message(Command("list"))
async def cmd_list(m: Message, store: Store, sess: aiohttp.ClientSession,
                   tk: tinkoff.TinkoffClient) -> None:
    prof = store.get(m.from_user.id)
    await m.answer(await _list(m, store, sess, tk),
                   reply_markup=report_kb(prof.watchlist) if prof.watchlist else None)


@router.message(Command("chart"))
async def cmd_chart(m: Message, store: Store, sess: aiohttp.ClientSession,
                    tk: tinkoff.TinkoffClient, flow: FlowMonitor) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        await m.answer("Использование: /chart SBER [1m|5m|15m|30m|1h|4h|1d]")
        return
    t = args[0].upper()
    period = args[1].lower() if len(args) > 1 else charts.DEFAULT_PERIOD
    if not TICKER_RE.match(t):
        await m.answer("Не похоже на тикер.")
        return
    try:
        err = await show_chart(m.bot, m.chat.id, sess, store, t, period, tk=tk, flow=flow)
    except Exception as e:
        log.exception("chart %s", t)
        err = f"⚠️ Не удалось построить график {esc(t)}:\n<code>{esc(e)}</code>"
    if err:
        await m.answer(err)


@router.message(Command("alert"))
async def cmd_alert(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        p = store.get(m.from_user.id)
        await m.answer(
            f"Сейчас порог: {p.threshold_pct:g}%.\n"
            "Установить: /alert 5 — алерт при изменении ≥ 5% за день."
        )
        return
    try:
        v = float(args[0].replace(",", "."))
    except ValueError:
        await m.answer("Нужно число, например: /alert 5 или /alert 2.5")
        return
    if not (0.1 <= v <= 50):
        await m.answer("Допустимый диапазон: 0.1–50.")
        return
    p = store.get(m.from_user.id)
    p.threshold_pct = v
    store.save()
    await m.answer(f"🔔 Порог алерта: {v:g}%. Для позиции берётся дневная доходность GetPortfolio; для бумаги вне портфеля — опубликованное закрытие T‑Invest. База подписана в каждом алерте.")


@router.message(Command("quiet"))
async def cmd_quiet(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        p = store.get(m.from_user.id)
        await m.answer(f"Сейчас пауза между повторными алертами: {p.cooldown_min:g} мин.\nУстановить: /quiet 30")
        return
    try:
        v = float(args[0].replace(",", "."))
    except ValueError:
        await m.answer("Нужно число минут, например: /quiet 30")
        return
    if not (5 <= v <= 1440):
        await m.answer("Допустимый диапазон: 5–1440 минут.")
        return
    p = store.get(m.from_user.id)
    p.cooldown_min = v
    store.save()
    await m.answer(f"⏸ Пауза между повторными алертами: {v:g} мин.")


@router.message(Command("report"))
async def cmd_report(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        p = store.get(m.from_user.id)
        state = f"каждые {p.report_min:g} мин" if p.report_min > 0 else "выключена"
        await m.answer(f"Сейчас сводка: {state}.\nУстановить: /report 60 (или /report 0 — выключить)")
        return
    try:
        v = float(args[0].replace(",", "."))
    except ValueError:
        await m.answer("Нужно число минут, например: /report 60")
        return
    if not (0 <= v <= 1440):
        await m.answer("Допустимый диапазон: 0–1440 минут (0 — выключить).")
        return
    p = store.get(m.from_user.id)
    p.report_min = v
    store.save()
    await m.answer(
        f"📊 Сводка: {'выключена' if v == 0 else f'каждые {v:g} мин'}."
    )


@router.message(Command("settings"))
async def cmd_settings(m: Message, store: Store) -> None:
    await m.answer(_help_text(store, m.from_user.id))


# ------------------------------------------------------ стакан / лента

async def _book_text(sess, tk: tinkoff.TinkoffClient, t: str) -> str:
    try:
        inst = await tk.instrument(t)
    except Exception as e:
        return f"⚠️ T-Invest API недоступен: {esc(e)}"
    if inst is None:
        return f"❌ <b>{esc(t)}</b> — нет такого тикера на TQBR (Т-Банк)"
    info = await moex.get_security_info(sess, t)
    dec = info.decimals if info else 2
    ob = await tk.order_book(inst, depth=20)
    if ob is None:
        return f"📚 <b>{t}</b> — стакан пуст (торгов нет)"
    return tape.book_text(t, inst.name, ob, inst.lot, dec)


async def _tape_text(sess, tk: tinkoff.TinkoffClient, t: str) -> str:
    try:
        inst = await tk.instrument(t)
    except Exception as e:
        return f"⚠️ T-Invest API недоступен: {esc(e)}"
    if inst is None:
        return f"❌ <b>{esc(t)}</b> — нет такого тикера на TQBR (Т-Банк)"
    info = await moex.get_security_info(sess, t)
    dec = info.decimals if info else 2
    trades = await tk.last_trades(inst, minutes=15)
    text = tape.tape_text(t, trades, inst.lot, dec)
    sigs = tape.analyze_trades(t, trades, inst.lot, dec)
    if sigs:
        text += "\n\n<b>Сигналы:</b>\n" + "\n".join(s.text.split("\n")[0] for s in sigs)
    return text


NO_TK = ("Стакан и лента доступны через T-Invest API. "
         "Задай переменную окружения TINKOFF_TOKEN (токен «только чтение»).")


@router.message(Command("book"))
async def cmd_book(m: Message, sess: aiohttp.ClientSession, tk: tinkoff.TinkoffClient) -> None:
    args = (m.text or "").split()[1:]
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    if not args:
        await m.answer("Использование: /book SBER"); return
    t = args[0].upper()
    try:
        text = await _book_text(sess, tk, t)
    except Exception as e:
        log.exception("book %s", t); text = f"⚠️ Не удалось получить стакан {t}: {esc(e)}"
    await m.answer(text, reply_markup=book_kb(t))


@router.message(Command("tape"))
async def cmd_tape(m: Message, sess: aiohttp.ClientSession, tk: tinkoff.TinkoffClient) -> None:
    args = (m.text or "").split()[1:]
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    if not args:
        await m.answer("Использование: /tape SBER"); return
    t = args[0].upper()
    try:
        text = await _tape_text(sess, tk, t)
    except Exception as e:
        log.exception("tape %s", t); text = f"⚠️ Не удалось получить ленту {t}: {esc(e)}"
    await m.answer(text, reply_markup=tape_kb(t))


# ------------------------------------------------------------- кластеры

async def _cluster_view(sess, tk: tinkoff.TinkoffClient, cstore: clu.ClusterStore,
                        t: str, window: str) -> tuple[str, bytes | None]:
    """Текст + PNG кластерного анализа. Перед расчётом подтягивает свежую ленту."""
    if window not in clu.WINDOWS:
        window = clu.DEFAULT_WINDOW
    try:
        inst = await tk.instrument(t)
    except Exception as e:
        return f"⚠️ T-Invest API недоступен: {esc(e)}", None
    if inst is None:
        return f"❌ <b>{esc(t)}</b> — нет такого тикера на TQBR (Т-Банк)", None
    info = await moex.get_security_info(sess, t)
    dec = info.decimals if info else 2
    name = info.name if info else inst.name
    try:
        trades = await tk.last_trades(inst, minutes=60)
        cstore.ingest(t, trades)
    except Exception as e:
        log.warning("clusters: лента %s: %s", t, e)
    minutes = clu.WINDOWS[window][1]
    an = clu.analyze(t, window, cstore.window(t, minutes), inst.lot, dec)
    text = clu.cluster_text(an, t, window, name, cstore.coverage(t), cstore.started_at)
    png = None
    if an is not None and an.n >= 3:
        try:
            png = await asyncio.to_thread(clu.render, an, name)
        except Exception:
            log.exception("clusters: render %s", t)
    return text, png


async def _send_clusters(bot: Bot, chat_id: int, sess, tk, cstore, t: str, window: str,
                         reply_to: int | None = None, current: Message | None = None) -> None:
    text, png = await _cluster_view(sess, tk, cstore, t, window)
    kb = cluster_kb(t, window)
    if png:
        # Telegram ограничивает caption 1024 символами. Не режем HTML
        # посередине <pre>: таблицы уже есть на самой картинке.
        caption = text.split("\n<b>Крупнейшие кластеры</b>", 1)[0]
        media = InputMediaPhoto(media=BufferedInputFile(png, f"{t}_clusters_{window}.png"),
                                caption=caption[:1024])
        if current is not None and current.photo:
            try:
                await current.edit_media(media, reply_markup=kb); return
            except TelegramBadRequest as e:
                if "not modified" in str(e): return
                log.warning("clusters edit_media %s: %s", t, e)
        if current is not None:
            try:
                await current.delete()
            except Exception:
                pass
        await bot.send_photo(chat_id, media.media, caption=caption[:1024], reply_markup=kb,
                             reply_to_message_id=reply_to)
        return
    if current is not None and not current.photo:
        try:
            await current.edit_text(text, reply_markup=kb); return
        except TelegramBadRequest as e:
            if "not modified" in str(e): return
    if current is not None:
        try:
            await current.delete()
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb, reply_to_message_id=reply_to)


@router.message(Command("clusters"))
async def cmd_clusters(m: Message, store: Store, sess: aiohttp.ClientSession,
                       tk: tinkoff.TinkoffClient, cstore: clu.ClusterStore) -> None:
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    args = (m.text or "").split()[1:]
    if not args:
        wl = store.get(m.from_user.id).watchlist
        if len(wl) == 1:
            args = [wl[0]]
        else:
            await m.answer("Использование: /clusters SBER [5m|15m|30m|1h|4h|1d]"
                           + (f"\nТвой список: {', '.join(wl)}" if wl else ""))
            return
    t = args[0].upper()
    window = args[1].lower() if len(args) > 1 else clu.DEFAULT_WINDOW
    window = {"5м": "5m", "15м": "15m", "30м": "30m", "1ч": "1h", "4ч": "4h", "1д": "1d",
              "day": "1d", "d": "1d", "день": "1d"}.get(window, window)
    if window not in clu.WINDOWS:
        await m.answer("Окно: 5m, 15m, 30m, 1h, 4h или 1d"); return
    await _send_clusters(m.bot, m.chat.id, sess, tk, cstore, t, window)


@router.callback_query(F.data.startswith("clu:"))
async def cb_clusters(c: CallbackQuery, sess: aiohttp.ClientSession,
                      tk: tinkoff.TinkoffClient, cstore: clu.ClusterStore) -> None:
    parts = c.data.split(":")
    t, window = parts[1], (parts[2] if len(parts) > 2 else clu.DEFAULT_WINDOW)
    await c.answer("Считаю кластеры…")
    if not tinkoff.enabled():
        await c.bot.send_message(c.from_user.id, NO_TK); return
    # кнопка под самими кластерами (подпись начинается с 🧮) — обновляем на месте
    cap = (c.message.caption or c.message.text or "") if c.message else ""
    own = cap.startswith("🧮")
    if own:
        chat_id, reply_to, current = c.message.chat.id, None, c.message
    else:
        chat_id, reply_to = _reply_ctx(c)
        current = None
    try:
        await _send_clusters(c.bot, chat_id, sess, tk, cstore, t, window,
                             reply_to=reply_to, current=current)
    except Exception as e:
        log.exception("clusters %s", t)
        try:
            await c.bot.send_message(chat_id, f"⚠️ Не удалось посчитать кластеры {esc(t)}:\n<code>{esc(e)}</code>",
                                     reply_to_message_id=reply_to)
        except Exception:
            pass


# ---------------------------------------------------------------- сетап

async def _setup_text(sess, tk: tinkoff.TinkoffClient, cstore: clu.ClusterStore,
                      flow: FlowMonitor, t: str) -> str:
    try:
        inst = await tk.instrument(t)
    except Exception as e:
        return f"⚠️ T-Invest API недоступен: {esc(e)}"
    if inst is None:
        return f"❌ <b>{esc(t)}</b> — нет такого тикера на TQBR (Т-Банк)"
    info = await moex.get_security_info(sess, t)
    dec = info.decimals if info else 2
    name = info.name if info else inst.name
    trades, ob, ds, im = await asyncio.gather(
        tk.last_trades(inst, minutes=60), tk.order_book(inst, depth=20),
        flow._daystats(sess, t), flow.imoex(sess), return_exceptions=True)
    if isinstance(trades, list):
        cstore.ingest(t, trades)
    if isinstance(ob, BaseException):
        ob = None
    price = (trades[-1].price if isinstance(trades, list) and trades
             else ob.last if ob else flow._last_price.get(t, 0.0))
    if not price:
        return f"По {esc(t)} сейчас нет сделок — сетап считать не по чему."
    cells = cstore.window(t, stp.LEVEL_LOOKBACK_MIN)
    if not cells:
        return (f"🎯 <b>{esc(t)}</b> — в базе пока нет ленты. Добавь бумагу в список "
                f"(/watch {esc(t)}), через несколько минут появятся данные.")
    ch15 = flow.price_change(t, 15)
    day_chg = (price - info.prev_close) / info.prev_close * 100 if info and info.prev_close else None
    im = im if isinstance(im, dict) else {}
    rs15 = stp.rel_strength(ch15, im.get("15m"), "15 мин")
    rsd = stp.rel_strength(day_chg, im.get("day"), "к закрытию MOEX")
    hi, lo, vwap = (ds.high, ds.low, ds.vwap) if not isinstance(ds, BaseException) else (0, 0, 0)
    s = await asyncio.to_thread(
        stp.build_setup, t, price, info.prev_close if info else 0.0, hi, lo, vwap, ob,
        cells, dec, inst.lot, flow.recent_signals(t), rs15, rsd)
    cov = cstore.coverage(t)
    text = stp.setup_text(s, name, dec)
    text += "\n<i>Дневной контекст сетапа — к официальному закрытию MOEX; H/L и VWAP — по минутным свечам MOEX.</i>"
    try:
        ml = market.line(await market.snapshot(sess))
        if ml:
            text += "\n\n" + ml
    except Exception as e:
        log.debug("market line: %s", e)
    if cov and (datetime.now() - cov) < timedelta(hours=6):
        text += f"\n<i>История ленты копится с {cov:%d.%m %H:%M} МСК — уровни и тесты пока неполные.</i>"
    return text


@router.message(Command("setup"))
async def cmd_setup(m: Message, store: Store, sess: aiohttp.ClientSession,
                    tk: tinkoff.TinkoffClient, cstore: clu.ClusterStore, flow: FlowMonitor) -> None:
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    args = (m.text or "").split()[1:]
    if not args:
        wl = store.get(m.from_user.id).watchlist
        if len(wl) == 1:
            args = [wl[0]]
        else:
            await m.answer("Использование: /setup SBER" + (f"\nТвой список: {', '.join(wl)}" if wl else ""))
            return
    t = args[0].upper()
    wait = await m.answer("Собираю сетап…")
    try:
        text = await _setup_text(sess, tk, cstore, flow, t)
    except Exception as e:
        log.exception("setup %s", t); text = f"⚠️ Не удалось собрать сетап {esc(t)}: <code>{esc(e)}</code>"
    try:
        await wait.edit_text(text, reply_markup=setup_kb(t))
    except TelegramBadRequest:
        await m.answer(text, reply_markup=setup_kb(t))


@router.callback_query(F.data.startswith("setup:"))
async def cb_setup(c: CallbackQuery, sess: aiohttp.ClientSession, tk: tinkoff.TinkoffClient,
                   cstore: clu.ClusterStore, flow: FlowMonitor) -> None:
    parts = c.data.split(":")
    t = parts[1]
    await c.answer("Собираю сетап…")
    if not tinkoff.enabled():
        await c.bot.send_message(c.from_user.id, NO_TK); return
    try:
        text = await _setup_text(sess, tk, cstore, flow, t)
    except Exception as e:
        log.exception("setup %s", t); text = f"⚠️ Не удалось собрать сетап {esc(t)}: <code>{esc(e)}</code>"
    if len(parts) > 2 and parts[2] == "r" and c.message and not c.message.photo:
        try:
            await c.message.edit_text(text, reply_markup=setup_kb(t)); return
        except TelegramBadRequest as e:
            if "not modified" in str(e): return
    chat_id, reply_to = _reply_ctx(c)
    await c.bot.send_message(chat_id, text, reply_markup=setup_kb(t), reply_to_message_id=reply_to)


# ---------------------------------------------------------------- CScalp

def _cscalp_allowed(uid: int) -> bool:
    return cscalp.enabled() and (not cscalp.OWNER_ID or uid == cscalp.OWNER_ID)


async def _cscalp_send(sess, csq: cscalp.CScalpQueue, t: str) -> tuple[str, Optional[str]]:
    """Отправить тикер мостику. Возвращает (текст для мгновенного ответа, id команды
    для фонового ожидания подтверждения или None)."""
    if cscalp.HTTP_MODE:
        csq.push(t)
        return (f"⚡ {t} → CScalp" if csq.online else
                f"{t} поставлен в очередь, но мостик не на связи (ПК выключен или скрипт не запущен)"), None
    cid, err = await csq.relay_push(sess, t)
    if err:
        return f"⚠️ {t}: {err}", None
    return f"⚡ {t} → CScalp", cid


async def _cscalp_confirm(bot: Bot, chat_id: int, sess, csq: cscalp.CScalpQueue, t: str, cid: str) -> None:
    """Фон: ждём подтверждение мостика до 20 с; беспокоим пользователя только при проблеме."""
    res = await csq.relay_wait_ack(sess, cid, timeout=20.0)
    if res is None:
        txt = f"⚠️ {t}: мостик CScalp не ответил за 20 с (ПК выключен или скрипт не запущен)"
    elif res != "ok":
        txt = f"⚠️ {t}: {res}"
    else:
        return
    try:
        m = await bot.send_message(chat_id, esc(txt))
        await asyncio.sleep(15)
        await m.delete()
    except Exception:
        pass


@router.message(Command("cscalp"))
async def cmd_cscalp(m: Message, csq: cscalp.CScalpQueue, sess) -> None:
    if not cscalp.enabled():
        await m.answer("Мостик CScalp не настроен: задай CSCALP_KEY (и OWNER_ID) у бота, "
                       "запусти cscalp_bridge.py на ПК с CScalp."); return
    if not _cscalp_allowed(m.from_user.id):
        await m.answer("Кнопка CScalp доступна только владельцу бота."); return
    args = (m.text or "").split()[1:]
    if not args:
        await m.answer(f"Использование: /cscalp SBER\nСтатус: {esc(csq.status_text())}"); return
    t = args[0].upper()
    txt, cid = await _cscalp_send(sess, csq, t)
    await m.answer(esc(txt))
    if cid:
        asyncio.create_task(_cscalp_confirm(m.bot, m.chat.id, sess, csq, t, cid))


@router.callback_query(F.data.startswith("cscalp:"))
async def cb_cscalp(c: CallbackQuery, csq: cscalp.CScalpQueue, sess) -> None:
    t = c.data.split(":")[1]
    if not _cscalp_allowed(c.from_user.id):
        await c.answer("Только для владельца бота", show_alert=True); return
    txt, cid = await _cscalp_send(sess, csq, t)
    await c.answer(txt, show_alert=not txt.startswith("⚡"))
    if cid and c.message:
        asyncio.create_task(_cscalp_confirm(c.bot, c.message.chat.id, sess, csq, t, cid))


# --------------------------------------------------------------- скринер

_scr_msg: dict[int, int] = {}   # chat_id -> message_id открытого скринера (одно на чат)


async def show_screener(bot: Bot, chat_id: int, scr: scrmod.Screener, win: str,
                        current: Optional[Message] = None, mine: bool = False,
                        watchlist: Optional[list[str]] = None) -> None:
    """Одно сообщение скринера на чат: картинка-таблица; переключение окна
    редактирует его на месте (как график); новая команда удаляет старое.
    mine — только бумаги из списка пользователя."""
    if win not in scrmod.WINDOWS and win != scrmod.VOL_WIN:
        win = scrmod.DEFAULT_WINDOW
    only = set(watchlist or []) if mine else None
    ups, downs, n = scr.rows(win, only)
    # кнопки лидеров: топ-3 роста + топ-3 падения (или топ-6 по объёму)
    leaders = [r.ticker for r in ups[:3]] + [r.ticker for r in downs[:3]] if win != scrmod.VOL_WIN \
        else [r.ticker for r in ups[:6]]
    kb = screener_kb(win, mine, leaders)
    if not scr.secs or n == 0 or (win == scrmod.VOL_WIN and not ups):
        # данных нет — короткий текст вместо пустой картинки
        text = scrmod.format_screener(scr, win, esc, only)
        if win == scrmod.VOL_WIN and not ups and n:
            text = (f"🔎 <b>Скринер MOEX — {esc(scrmod.WINDOW_TITLES[win])}</b>\n\n"
                    f"Сейчас нет бумаг с оборотом за 15 мин выше ×{scrmod.VOL_MIN_RATIO:g} от обычного "
                    f"для этого часа (в расчёте {n}).")
        if current is not None and not current.photo:
            try:
                await current.edit_text(text, reply_markup=kb); return
            except TelegramBadRequest as e:
                if "not modified" in str(e):
                    return
        old = _scr_msg.pop(chat_id, None)
        if old and (current is None or old != current.message_id):
            try:
                await bot.delete_message(chat_id, old)
            except Exception:
                pass
        if current is not None:
            try:
                await current.delete()
            except Exception:
                pass
        m = await bot.send_message(chat_id, text, reply_markup=kb)
        _scr_msg[chat_id] = m.message_id
        return
    try:
        png = await asyncio.to_thread(scrmod.render_png, scr, win, only)
    except Exception as e:
        log.exception("screener render")
        text = (scrmod.format_screener(scr, win, esc, only) +
                f"\n<i>(картинка недоступна: {esc(e)})</i>")
        if current is not None and not current.photo:
            try:
                await current.edit_text(text, reply_markup=kb); return
            except TelegramBadRequest:
                pass
        old = _scr_msg.pop(chat_id, None)
        if old and (current is None or old != current.message_id):
            try:
                await bot.delete_message(chat_id, old)
            except Exception:
                pass
        if current is not None:
            try:
                await current.delete()
            except Exception:
                pass
        m = await bot.send_message(chat_id, text, reply_markup=kb)
        _scr_msg[chat_id] = m.message_id
        return
    file = BufferedInputFile(png, f"screener_{win}.png")
    caption = f"🔎 Скринер MOEX — {scrmod.WINDOW_TITLES[win]}" + (" · мои бумаги" if mine else "")
    if current is not None and current.photo:
        try:
            await current.edit_media(InputMediaPhoto(media=file, caption=caption), reply_markup=kb)
            return
        except TelegramBadRequest as e:
            if "not modified" in str(e):
                return
            log.warning("screener edit_media: %s", e)
    old = _scr_msg.pop(chat_id, None)
    if old and (current is None or old != current.message_id):
        try:
            await bot.delete_message(chat_id, old)
        except Exception:
            pass
    if current is not None:
        try:
            await current.delete()
        except Exception:
            pass
    m = await bot.send_photo(chat_id, file, caption=caption, reply_markup=kb)
    _scr_msg[chat_id] = m.message_id


@router.message(Command("screener"))
async def cmd_screener(m: Message, scr: scrmod.Screener, store: Store) -> None:
    args = (m.text or "").split()[1:]
    win = args[0].lower() if args else scrmod.DEFAULT_WINDOW
    if win not in scrmod.WINDOWS and win != scrmod.VOL_WIN:
        await m.answer("Использование: /screener [15m|30m|1h|4h|1d|vol]"); return
    await show_screener(m.bot, m.chat.id, scr, win, watchlist=store.get(m.from_user.id).watchlist)


@router.callback_query(F.data == "scr:close")
async def cb_scr_close(c: CallbackQuery) -> None:
    await c.answer()
    _scr_msg.pop(c.message.chat.id, None)
    try:
        await c.message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("scr:pick:"))
async def cb_scr_pick(c: CallbackQuery, store: Store, scr: scrmod.Screener) -> None:
    t = c.data.split(":")[2]
    await c.answer()
    prof = store.get(c.from_user.id)
    s = scr.secs.get(t)
    head = f"<b>{esc(t)}</b>" + (f" — {esc(s.name)}" if s else "")
    if s and s.last:
        ch = s.change("1d")
        head += f"\n{esc(scrmod._fmt_price(s.last, s.decimals))} ₽"
        if ch is not None:
            head += f"  ({ch:+.2f}% за день)".replace(".", ",")
        chs = []
        for w in ("15m", "1h"):
            v = s.change(w)
            if v is not None:
                chs.append(f"{scrmod.WINDOW_SHORT[w]} {v:+.2f}%".replace(".", ","))
        r = s.vol_ratio()
        if r:
            chs.append(f"объём ×{r:.1f}")
        if chs:
            head += "\n" + " · ".join(chs)
    await c.message.answer(head, reply_markup=scr_pick_kb(t, t in prof.watchlist))


@router.callback_query(F.data.startswith("watch:"))
async def cb_watch(c: CallbackQuery, store: Store, sess: aiohttp.ClientSession) -> None:
    t = c.data.split(":")[1]
    await c.answer()
    m = c.message.model_copy(update={"from_user": c.from_user})
    await c.message.answer(await _watch(m, store, sess, [t]))


@router.callback_query(F.data == "noop")
async def cb_noop(c: CallbackQuery) -> None:
    await c.answer()


@router.callback_query(F.data.startswith("scr:"))
async def cb_scr(c: CallbackQuery, scr: scrmod.Screener, store: Store) -> None:
    parts = c.data.split(":")
    win = parts[1]
    flags = set(parts[2:])
    await c.answer("Обновляю…" if "r" in flags else None)
    try:
        await show_screener(c.bot, c.message.chat.id, scr, win, current=c.message,
                            mine="my" in flags, watchlist=store.get(c.from_user.id).watchlist)
    except Exception as e:
        log.exception("screener button %s", win)
        await c.bot.send_message(c.message.chat.id,
                                 f"⚠️ Не удалось обновить скринер: <code>{esc(e)}</code>")


# ------------------------------------------------------------ рынок/календарь

@router.message(Command("market"))
async def cmd_market(m: Message, sess: aiohttp.ClientSession, store: Store,
                     scr: scrmod.Screener) -> None:
    snap = await market.snapshot(sess, force=True)
    rows = []
    for t in store.get(m.from_user.id).watchlist:
        s = scr.secs.get(t)
        if s is None or not s.last:
            rows.append((t, None, None))
        else:
            rows.append((t, s.change("15m"), s.change("1d")))
    await m.answer(market.text(snap) + market.rs_text(snap, rows))


@router.message(Command("calendar"))
async def cmd_calendar(m: Message, store: Store, cal: evmod.Calendar) -> None:
    args = (m.text or "").split()[1:]
    days = int(args[0]) if args and args[0].isdigit() else evmod.HORIZON_DAYS
    days = max(1, min(days, 90))
    wl = store.get(m.from_user.id).watchlist
    evs = await cal.upcoming(wl, days=days)
    await m.answer(cal.text(evs, wl, esc, days=days))


@router.message(Command("event"))
async def cmd_event(m: Message, store: Store, cal: evmod.Calendar) -> None:
    usage = ("Использование:\n"
             "/event add SBER 2026-11-05 МСФО 3 кв.\n"
             "/event add - 2026-10-23 Заседание ЦБ   (— без бумаги)\n"
             "/event del 3   (номер из /calendar)\n"
             "/event list")
    args = (m.text or "").split(maxsplit=4)[1:]
    if not args:
        await m.answer(usage); return
    op = args[0].lower()
    if op == "add" and len(args) >= 4:
        t = "" if args[1] in ("-", "—") else args[1].upper()
        dt = args[2]
        try:
            datetime.strptime(dt, "%Y-%m-%d")
        except ValueError:
            await m.answer("Дата в формате ГГГГ-ММ-ДД, например 2026-11-05"); return
        e = cal.add(dt, t, args[3])
        await m.answer(f"📌 Добавлено #{e.id}: {esc(dt)} {esc(t)} {esc(e.text)}"); return
    if op == "del" and len(args) >= 2 and args[1].isdigit():
        ok = cal.remove(int(args[1]))
        await m.answer("🗑 Удалено" if ok else "Нет события с таким номером"); return
    if op == "list":
        if not cal.manual:
            await m.answer("Ручных событий нет."); return
        lines = [f"#{e.id} {e.date} {e.ticker or '—'} {esc(e.text)}" for e in sorted(cal.manual, key=lambda e: e.date)]
        await m.answer("📌 <b>Ручные события</b>\n" + "\n".join(lines)); return
    await m.answer(usage)


# --------------------------------------------------------------- очистка

@router.message(Command("clear"))
async def cmd_clear(m: Message, sent: cleaner.SentLog) -> None:
    args = (m.text or "").split()[1:]
    hours = 0.0
    if args and args[0].replace(".", "", 1).isdigit():
        hours = float(args[0])
    n = await cleaner.clear_chat(m.bot, sent, m.chat.id, hours * 3600)
    note = await m.answer(f"🧹 Удалено {n} сообщений бота" + (f" старше {hours:g} ч" if hours else "")
                          + ".\nСвои сообщения (команды) Telegram удалять боту не даёт.")
    await asyncio.sleep(5)
    try:
        await note.delete()
        sent.take(m.chat.id, keep=None)  # note уже учтён и удалён
    except Exception:
        pass


@router.message(Command("autoclear"))
async def cmd_autoclear(m: Message, store: Store) -> None:
    prof = store.get(m.from_user.id)
    args = (m.text or "").split()[1:]
    if not args:
        cur = f"каждый день в {prof.clear_hour:02d}:00 МСК" if prof.clear_hour >= 0 else "выключена"
        await m.answer(f"Автоочистка: {cur}.\n/autoclear 3 — час МСК, /autoclear off — выключить.")
        return
    if args[0].lower() in ("off", "выкл", "0ff"):
        prof.clear_hour = -1
        store.save()
        await m.answer("Автоочистка выключена."); return
    if not args[0].isdigit() or not 0 <= int(args[0]) <= 23:
        await m.answer("Укажи час 0–23 МСК или off."); return
    prof.clear_hour = int(args[0])
    store.save()
    await m.answer(f"🧹 Буду чистить чат ежедневно в {prof.clear_hour:02d}:00 МСК "
                   f"(удаляются только сообщения бота, не старше 48 ч).")


@router.message(Command("flow"))
async def cmd_flow(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    p = store.get(m.from_user.id)
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    if len(args) >= 2 and args[0].lower() == "raw" and args[1].lower() in ("on", "off"):
        p.raw_flow_alerts = args[1].lower() == "on"
        store.save()
    elif args and args[0].lower() in ("on", "off"):
        enabled_ = args[0].lower() == "on"
        p.flow_alerts = enabled_
        p.idea_alerts = enabled_
        store.save()
    await m.answer(
        f"💡 Торговые идеи: <b>{'включены' if p.idea_alerts else 'выключены'}</b>\n"
        f"🧪 Сырые события ленты: <b>{'включены' if p.raw_flow_alerts else 'выключены'}</b>\n\n"
        "По умолчанию киты, ритм, стены и всплески не шлются отдельно: они лишь контекст идеи.\n"
        "/flow on|off · /flow raw on|off")


@router.message(Command("ideas"))
async def cmd_ideas(m: Message, idea_journal: IdeaJournal, store: Store) -> None:
    args = (m.text or "").split()[1:]
    p = store.get(m.from_user.id)
    if args and args[0].lower() in ("on", "off"):
        p.idea_alerts = args[0].lower() == "on"
        store.save()
        await m.answer(f"💡 Идеи: {'включены' if p.idea_alerts else 'выключены'}.")
        return
    days, ticker = 30, ""
    for a in args:
        if a.isdigit():
            days = max(1, min(int(a), 180))
        elif TICKER_RE.match(a.upper()):
            ticker = a.upper()
    active = [x for x in idea_journal.active(ticker or None)
              if x.delivery(m.from_user.id)]
    if active:
        await m.answer("💡 <b>Активные идеи</b>\n" + "\n".join(
            f"• {x.ticker} {x.side} · {x.status} · триггер {x.trigger:g}" for x in active))
    await m.answer(idea_journal.stats(m.from_user.id, days, ticker))


@router.message(Command("risk"))
async def cmd_risk(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    p = store.get(m.from_user.id)
    if not args:
        await m.answer(f"Риск на идею: {p.risk_rub:g} ₽.\nЗадать: /risk 1000 · убрать расчёт: /risk 0")
        return
    try:
        value = float(args[0].replace(",", "."))
    except ValueError:
        await m.answer("Нужна сумма в рублях, например /risk 1000")
        return
    if not 0 <= value <= 10_000_000:
        await m.answer("Допустимо: 0–10 000 000 ₽.")
        return
    p.risk_rub = value
    store.save()
    await m.answer(f"💰 Риск на одну идею: <b>{value:g} ₽</b>. В карточке появится расчёт лотов.")


@router.message(Command("quiethours"))
async def cmd_quiethours(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    p = store.get(m.from_user.id)
    if args and args[0].lower() == "off":
        p.quiet_from = p.quiet_to = -1
        store.save()
        await m.answer("🔔 Тихие часы выключены.")
        return
    if len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        a, b_ = int(args[0]), int(args[1])
        if 0 <= a <= 23 and 0 <= b_ <= 23:
            p.quiet_from, p.quiet_to = a, b_
            store.save()
            await m.answer(f"🌙 Тихие часы: с {a:02d}:00 до {b_:02d}:00 МСК — "
                           "ни алертов, ни сигналов, ни сводок.")
            return
    state = (f"с {p.quiet_from:02d}:00 до {p.quiet_to:02d}:00 МСК"
             if p.quiet_from >= 0 else "выключены")
    await m.answer(f"Тихие часы сейчас: {state}.\n"
                   "Задать: /quiethours 23 9 · выключить: /quiethours off")


@router.message(Command("backtest"))
async def cmd_backtest(m: Message, sess: aiohttp.ClientSession,
                       tk: tinkoff.TinkoffClient) -> None:
    """Прогнать детекторы ленты по последнему часу (всё, что отдаёт T-Invest)."""
    args = (m.text or "").split()[1:]
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    if not args:
        await m.answer("Использование: /backtest SBER — прогон детекторов по ленте за последний час"); return
    t = args[0].upper()
    try:
        inst = await tk.instrument(t)
        if inst is None:
            await m.answer(f"❌ {esc(t)} — нет на TQBR"); return
        info = await moex.get_security_info(sess, t)
        dec = info.decimals if info else 2
        trades = await tk.last_trades(inst, minutes=60)
    except Exception as e:
        await m.answer(f"⚠️ {esc(e)}"); return
    if len(trades) < 20:
        await m.answer(f"🧪 {t}: за последний час всего {len(trades)} сделок — торгов нет."); return
    # скользящее окно 10 мин с шагом 1 мин, как это делает монитор
    from datetime import timedelta as _td
    base = tape.Baseline()
    found: dict[str, tape.Signal] = {}
    t0, t1 = trades[0].ts, trades[-1].ts
    cur = t0 + _td(minutes=10)
    while cur <= t1:
        win = [x for x in trades if cur - _td(minutes=10) <= x.ts <= cur]
        base.update(win, inst.lot)
        for s in tape.analyze_trades(t, win, inst.lot, dec, base):
            found.setdefault(s.key, s)
        cur += _td(minutes=1)
    if not found:
        await m.answer(f"🧪 <b>{t}</b>: {len(trades)} сделок за {(t1 - t0).seconds // 60} мин — "
                       "ни один детектор не сработал. Пороги для этой бумаги, возможно, высоки."); return
    from collections import Counter
    cnt = Counter(s.kind for s in found.values())
    from journal import KIND_EMOJI, KIND_NAME
    summary = " · ".join(f"{KIND_EMOJI.get(k, '')} {KIND_NAME.get(k, k)} ×{v}" for k, v in cnt.most_common())
    lines = [f"🧪 <b>Бэктест {t}</b> · {len(trades)} сделок за {(t1 - t0).seconds // 60} мин\n{summary}\n"]
    for s in list(found.values())[:6]:
        lines.append(s.text)
    if len(found) > 6:
        lines.append(f"… и ещё {len(found) - 6}")
    await m.answer("\n\n".join(lines))


@router.message(Command("news"))
async def cmd_news(m: Message, sess: aiohttp.ClientSession, newsmon: NewsMonitor) -> None:
    """Статус парсера + последние релевантные новости прямо сейчас (в личку)."""
    args = (m.text or "").split()[1:]
    if not NEWS_CHANNEL:
        await m.answer("Канал новостей не настроен: задай переменную NEWS_CHANNEL "
                       "(@username канала или его id вида -100…), бот должен быть админом.")
        return
    from news import classify, format_item, is_relevant, SOURCES
    from keyboards import news_kb
    items = await newsmon.fetch_all(sess)
    watch = newsmon.watch_dict()
    rel = []
    for it in items:
        classify(it, watch)
        if is_relevant(it, "all" if args and args[0] == "all" else newsmon.mode):
            rel.append(it)
    ok = len({i.source_id for i in items})
    head = (f"📰 Источники: {ok}/{len(SOURCES)} отвечают · всего {len(items)} новостей · "
            f"релевантных {len(rel)} · канал {esc(NEWS_CHANNEL)}")
    await m.answer(head)
    for it in rel[-3:]:
        await m.answer(format_item(it), reply_markup=news_kb(it.tickers),
                       disable_web_page_preview=True)


@router.message(Command("stats"))
async def cmd_stats(m: Message, journal: Journal) -> None:
    args = (m.text or "").split()[1:]
    days, ticker = 7, None
    for a in args:
        if a.isdigit():
            days = max(1, min(int(a), 14))
        elif TICKER_RE.match(a.upper()):
            ticker = a.upper()
    try:
        text = journal.stats(m.from_user.id, days, ticker)
    except Exception as e:
        log.exception("stats")
        await m.answer(f"⚠️ Не удалось собрать статистику: <code>{esc(e)}</code>"); return
    for chunk in _split_html(text):
        await m.answer(chunk)


def _split_html(text: str, limit: int = 3900) -> list[str]:
    """Разбить длинный ответ по строкам, не ломая блоки <pre>."""
    if len(text) <= limit:
        return [text]
    out, cur, in_pre = [], "", False
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 + (6 if in_pre else 0) > limit:
            out.append(cur + ("</pre>" if in_pre else ""))
            cur = ("<pre>" if in_pre else "")
        cur += ("\n" if cur and not cur.endswith("<pre>") else "") + line
        if "<pre>" in line:
            in_pre = True
        if "</pre>" in line:
            in_pre = False
    if cur:
        out.append(cur)
    return out


@router.message(Command("mutes"))
async def cmd_mutes(m: Message, journal: Journal, store: Store) -> None:
    """Какие пары «бумага × тип сигнала» сейчас отключены авто-фильтром."""
    prof = store.get(m.from_user.id)
    await m.answer(journal.mutes_text(prof.watchlist))


@router.callback_query(F.data.startswith("book:"))
async def cb_book(c: CallbackQuery, sess: aiohttp.ClientSession, tk: tinkoff.TinkoffClient) -> None:
    parts = c.data.split(":")
    t = parts[1]
    await c.answer("Загружаю стакан…")
    if not tinkoff.enabled():
        await c.message.answer(NO_TK); return
    try:
        text = await _book_text(sess, tk, t)
    except Exception as e:
        log.exception("book %s", t); text = f"Не удалось получить стакан {t}: {esc(e)}"
    chat_id, reply_to = _reply_ctx(c)
    if len(parts) > 2 and parts[2] == "r":
        try:
            await c.message.edit_text(text, reply_markup=book_kb(t)); return
        except TelegramBadRequest as e:
            if "not modified" in str(e): return
    await c.bot.send_message(chat_id, text, reply_markup=book_kb(t),
                             reply_to_message_id=reply_to)


@router.callback_query(F.data.startswith("tape:"))
async def cb_tape(c: CallbackQuery, sess: aiohttp.ClientSession, tk: tinkoff.TinkoffClient) -> None:
    parts = c.data.split(":")
    t = parts[1]
    await c.answer("Загружаю ленту…")
    if not tinkoff.enabled():
        await c.message.answer(NO_TK); return
    try:
        text = await _tape_text(sess, tk, t)
    except Exception as e:
        log.exception("tape %s", t); text = f"Не удалось получить ленту {t}: {esc(e)}"
    chat_id, reply_to = _reply_ctx(c)
    if len(parts) > 2 and parts[2] == "r":
        try:
            await c.message.edit_text(text, reply_markup=tape_kb(t)); return
        except TelegramBadRequest as e:
            if "not modified" in str(e): return
    await c.bot.send_message(chat_id, text, reply_markup=tape_kb(t),
                             reply_to_message_id=reply_to)


@router.callback_query(F.data == "flow:off")
async def cb_flow_off(c: CallbackQuery, store: Store) -> None:
    p = store.get(c.from_user.id)
    p.flow_alerts = False
    store.save()
    await c.answer("Сигналы ленты выключены")
    await c.message.answer("🔕 Сигналы ленты/стакана выключены. Включить: /flow on")


@router.callback_query(F.data.startswith("idea:"))
async def cb_idea(c: CallbackQuery, idea_journal: IdeaJournal, store: Store) -> None:
    _, action, idea_id = c.data.split(":", 2)
    idea = idea_journal.find(idea_id)
    delivery = idea.delivery(c.from_user.id) if idea else None
    if idea is None or delivery is None:
        await c.answer("Идея не найдена", show_alert=True)
        return
    if action in ("take", "skip"):
        idea_journal.set_decision(idea_id, c.from_user.id,
                                  "taken" if action == "take" else "skipped")
        await c.answer("Отмечено: взял" if action == "take" else "Отмечено: пропустил")
        return
    if action == "cancel":
        if idea.status not in ("prepared", "active"):
            await c.answer("Идея уже закрыта", show_alert=True)
            return
        idea.status = "cancelled"
        idea.closed_ts = datetime.now().timestamp()
        idea.close_reason = "отменена вручную"
        idea_journal.save()
        await c.answer("Идея отменена")
        try:
            await c.message.edit_text(card(idea, store.get(c.from_user.id).risk_rub),
                                      reply_markup=idea_kb(idea.id, idea.ticker, idea.status))
        except TelegramBadRequest:
            pass


# ------------------------------------------------------------- callbacks

@router.callback_query(F.data == "chart:close")
async def cb_chart_close(c: CallbackQuery) -> None:
    await c.answer()
    _chart_msg.pop(c.message.chat.id, None)
    try:
        await c.message.delete()
    except Exception:
        pass


@router.callback_query(F.data == "msg:close")
async def cb_msg_close(c: CallbackQuery) -> None:
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass


def _reply_ctx(c: CallbackQuery) -> tuple[int, int | None]:
    """Куда отвечать на нажатие: (chat_id, reply_to_message_id).

    Если кнопка под новостью (есть #ТИКЕР в тексте) или пост в канале —
    отвечаем цитатой на это сообщение, чтобы ответ был привязан к новости.
    Под графиком/стаканом в личке — обычное поведение (без цитаты).
    """
    m = c.message
    if m is None:
        return c.from_user.id, None
    is_news = bool(m.text and "#" in m.text and "Источник" in m.text)
    if m.chat.type != "private" or is_news:
        return m.chat.id, m.message_id
    return m.chat.id, None


@router.callback_query(F.data.startswith("chart:"))
async def cb_chart(c: CallbackQuery, store: Store, sess: aiohttp.ClientSession,
                   tk: tinkoff.TinkoffClient, flow: FlowMonitor) -> None:
    parts = c.data.split(":")
    t, period = parts[1], (parts[2] if len(parts) > 2 else charts.DEFAULT_PERIOD)
    if c.message and c.message.photo:
        # кнопка под самим графиком: заменяем картинку на месте
        chat_id, reply_to, current = c.message.chat.id, None, c.message
    else:
        chat_id, reply_to = _reply_ctx(c)
        current = None
    await c.answer("Строю график…")
    try:
        err = await show_chart(c.bot, chat_id, sess, store, t, period,
                               current=current, tk=tk, reply_to=reply_to, flow=flow)
    except Exception as e:
        log.exception("график %s", t)
        err = f"⚠️ Не удалось построить график {esc(t)}:\n<code>{esc(e)}</code>"
    if err:
        try:
            await c.bot.send_message(chat_id, err, reply_to_message_id=reply_to)
        except Exception:
            log.exception("chart error notify")


@router.callback_query(F.data == "refresh")
async def cb_refresh(c: CallbackQuery, store: Store, sess: aiohttp.ClientSession,
                     tk: tinkoff.TinkoffClient) -> None:
    await c.answer("Обновляю…")
    prof = store.get(c.from_user.id)
    text = await _list(c, store, sess, tk)
    try:
        await c.message.edit_text(text, reply_markup=report_kb(prof.watchlist))
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            await c.message.answer(text, reply_markup=report_kb(prof.watchlist))


@router.callback_query(F.data == "settings")
async def cb_settings(c: CallbackQuery, store: Store) -> None:
    await c.answer()
    await c.message.answer(_help_text(store, c.from_user.id))


@router.callback_query(F.data.startswith("unwatch:"))
async def cb_unwatch(c: CallbackQuery, store: Store) -> None:
    t = c.data.split(":", 1)[1]
    prof = store.get(c.from_user.id)
    if t in prof.watchlist:
        prof.watchlist.remove(t)
        store.save()
        await c.answer(f"{t} убран из списка")
        await c.message.answer(f"🗑 <b>{t}</b> — больше не слежу.\n"
                               f"Следим за: {', '.join(prof.watchlist) or '—'}")
    else:
        await c.answer(f"{t} уже не в списке")
    try:
        await c.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


# ----------------------------------------------------------------- канал

@router.channel_post()
async def on_channel_post(m: Message) -> None:
    """Помогает узнать id канала: бот пишет его в лог при любом посте."""
    log.info("канал: id=%s title=%r — используй NEWS_CHANNEL=%s",
             m.chat.id, m.chat.title, m.chat.id)


@router.my_chat_member()
async def on_added(ev) -> None:
    log.info("бота добавили/изменили в чате id=%s (%s) статус=%s",
             ev.chat.id, ev.chat.title or ev.chat.type, ev.new_chat_member.status)


# ----------------------------------------------------------------- fallback

# -------------------------------------------------------------------- меню

BOT_COMMANDS = [
    ("version", "Номер запущенной сборки"),
    ("diag", "Диагностика версии и источников"),
    ("list", "Мои бумаги и цены"),
    ("screener", "Скринер: что двигается — /screener 15m|vol"),
    ("market", "Рынок сейчас: IMOEX, RTS, валюта, Brent"),
    ("calendar", "Календарь: дивиденды, ЦБ, отчётности — /calendar 14"),
    ("event", "Добавить/удалить событие — /event add SBER 2026-11-05 МСФО"),
    ("setup", "Сетап: за/против входа — /setup SBER"),
    ("ideas", "Торговые идеи и их статистика — /ideas 30"),
    ("risk", "Риск на идею и расчёт лотов — /risk 1000"),
    ("clusters", "Кластеры объёма — /clusters SBER 1h"),
    ("chart", "Свечной график — /chart SBER 5m"),
    ("book", "Стакан — /book SBER"),
    ("tape", "Лента сделок — /tape SBER"),
    ("watch", "Добавить бумаги — /watch SBER GAZP"),
    ("unwatch", "Убрать бумагу"),
    ("stats", "Точность сигналов — /stats 7"),
    ("mutes", "Авто-фильтр: какие сигналы отключены"),
    ("news", "Новости: статус и последние"),
    ("flow", "Сигналы роботов вкл/выкл — /flow on|off"),
    ("alert", "Порог алерта, % — /alert 3"),
    ("report", "Сводка каждые N мин — /report 60"),
    ("quiethours", "Тихие часы — /quiethours 23 9"),
    ("backtest", "Прогнать детекторы за час — /backtest SBER"),
    ("clear", "Удалить сообщения бота в чате"),
    ("autoclear", "Ежедневная очистка — /autoclear 3|off"),
    ("cscalp", "Открыть в CScalp (мостик)"),
    ("settings", "Мои настройки"),
    ("help", "Справка по всем командам"),
]


async def setup_menu(bot: Bot) -> bool:
    """Синяя кнопка «Меню» слева от поля ввода: список команд с подсказками.
    Задаётся через API, BotFather не нужен."""
    try:
        commands = [BotCommand(command=c, description=d[:256]) for c, d in BOT_COMMANDS]
        # Telegram может хранить отдельные списки для языка и приватных чатов.
        # Обновляем все варианты, которые видит личный бот, чтобы старое меню
        # BotFather не перекрывало актуальные команды приложения.
        scopes = (BotCommandScopeDefault(), BotCommandScopeAllPrivateChats())
        for scope in scopes:
            for language_code in (None, "ru"):
                await bot.set_my_commands(commands, scope=scope,
                                          language_code=language_code)
        log.info("меню команд обновлено (%d команд, default/private, default/ru)",
                 len(BOT_COMMANDS))
        return True
    except Exception:
        log.exception("не удалось задать меню команд")
        return False


async def keep_menu_current(bot: Bot, initial_ok: bool) -> None:
    """Повторить установку меню после временного сбоя Telegram API."""
    delay = 6 * 60 * 60 if initial_ok else 30
    while True:
        await asyncio.sleep(delay)
        delay = 6 * 60 * 60 if await setup_menu(bot) else 60


def main_kb(watchlist: list[str]) -> ReplyKeyboardMarkup:
    """Постоянная клавиатура под полем ввода: частые действия одним нажатием."""
    rows = [[KeyboardButton(text="📋 Список"), KeyboardButton(text="🎯 Сетап"),
             KeyboardButton(text="🧮 Кластеры")],
            [KeyboardButton(text="📈 График"), KeyboardButton(text="📚 Стакан"),
             KeyboardButton(text="🧾 Лента")],
            [KeyboardButton(text="🔎 Скринер"), KeyboardButton(text="💡 Идеи"),
             KeyboardButton(text="📊 Статистика"),
             KeyboardButton(text="📰 Новости"), KeyboardButton(text="⚙️ Настройки")]]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True,
                               input_field_placeholder="Тикер или команда…")


# кнопка → команда; для команд с тикером бот спросит бумагу, если в списке их больше одной
MENU_MAP = {
    "📋 Список": "list", "🎯 Сетап": "setup", "🧮 Кластеры": "clusters",
    "📈 График": "chart", "📚 Стакан": "book", "🧾 Лента": "tape",
    "📊 Статистика": "stats", "📰 Новости": "news", "⚙️ Настройки": "settings",
    "🔎 Скринер": "screener", "💡 Идеи": "ideas",
}
NEEDS_TICKER = {"setup", "clusters", "chart", "book", "tape"}


def ticker_pick_kb(cmd: str, tickers: list[str]):
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    rows, row = [], []
    for t in tickers:
        row.append(InlineKeyboardButton(text=t, callback_data=f"pick:{cmd}:{t}"))
        if len(row) == 4:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(F.text.in_(MENU_MAP.keys()))
async def menu_button(m: Message, store: Store, **kw) -> None:
    cmd = MENU_MAP[m.text]
    prof = store.get(m.from_user.id)
    if cmd in NEEDS_TICKER:
        wl = prof.watchlist
        if not wl:
            await m.answer("Список пуст — добавь бумаги: /watch SBER GAZP"); return
        if len(wl) > 1:
            await m.answer(f"Какую бумагу? (/{cmd})", reply_markup=ticker_pick_kb(cmd, wl)); return
        m = m.model_copy(update={"text": f"/{cmd} {wl[0]}"})
    else:
        m = m.model_copy(update={"text": f"/{cmd}"})
    await _dispatch(m, store=store, **kw)


@router.callback_query(F.data.startswith("pick:"))
async def cb_pick(c: CallbackQuery, store: Store, **kw) -> None:
    _, cmd, t = c.data.split(":")
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass
    m = c.message.model_copy(update={"text": f"/{cmd} {t}", "from_user": c.from_user})
    await _dispatch(m, store=store, **kw)


async def _dispatch(m: Message, **kw) -> None:
    """Вызвать обработчик команды напрямую (кнопка меню / выбор тикера)."""
    handlers = {
        "list": cmd_list, "setup": cmd_setup, "clusters": cmd_clusters, "chart": cmd_chart,
        "book": cmd_book, "tape": cmd_tape, "stats": cmd_stats, "mutes": cmd_mutes, "news": cmd_news,
        "settings": cmd_settings, "screener": cmd_screener, "ideas": cmd_ideas,
    }
    cmd = m.text.split()[0].lstrip("/")
    fn = handlers.get(cmd)
    if fn is None:
        log.error("_dispatch: нет обработчика для %r", cmd)
        await m.answer(f"⚠️ Кнопка «{esc(cmd)}» пока не подключена.")
        return
    import inspect
    params = inspect.signature(fn).parameters
    args = {k: v for k, v in kw.items() if k in params}
    await fn(m, **args)


@router.message()
async def fallback(m: Message, store: Store, sess: aiohttp.ClientSession) -> None:
    """Обычный текст: если похоже на тикер — добавляем в список."""
    if m.text and TICKER_RE.match(m.text.strip().upper()):
        await m.answer(await _watch(m, store, sess, [m.text.strip()]))
        return
    await m.answer("Не понял 🙂 По командам — /help")


# -------------------------------------------------------------------- main

async def health_check(bot: Bot) -> None:
    """Контроль связи с Telegram API.

    В нестабильных сетях (песочницы, VPN) long polling может «зависнуть»:
    соединение молча умирает, и бот перестаёт отвечать на несколько минут.
    Раз в 45 c проверяем API; два последовательных сбоя — роняем процесс,
    watchdog (run.sh) поднимет его заново за пару секунд.
    """
    await asyncio.sleep(30)
    fails = 0
    while True:
        try:
            await asyncio.wait_for(bot.get_me(), timeout=20)
            fails = 0
        except Exception:
            fails += 1
            log.warning("health check: Telegram API недоступен (сбой %s/2)", fails)
            if fails >= 2:
                log.error("health check: связь с Telegram потеряна — рестарт процесса")
                os._exit(3)
        await asyncio.sleep(45)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    if not BOT_TOKEN:
        sys.exit(
            "BOT_TOKEN не задан. Получи токен у @BotFather и передай так:\n"
            "  export BOT_TOKEN='123456:ABC...'\n"
            "или положи в файл .env:  BOT_TOKEN=123456:ABC..."
        )
    log.info("StockBot build %s starting", BUILD_VERSION)

    bot = Bot(token=BOT_TOKEN,
              default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    sent = cleaner.SentLog(os.path.join(os.path.dirname(DATA_FILE) or ".", "sent.json"))
    bot.session.middleware(cleaner.TrackOutgoing(sent))
    dp = Dispatcher()
    dp.include_router(router)
    store = Store(DATA_FILE)
    tk = tinkoff.TinkoffClient()
    monitor = Monitor(bot, store, tk if tinkoff.enabled() else None)
    journal = Journal(os.path.join(os.path.dirname(DATA_FILE) or ".", "signals.json"))
    idea_journal = IdeaJournal(os.path.join(os.path.dirname(DATA_FILE) or ".", "ideas.json"))
    cstore = clu.ClusterStore(os.path.join(os.path.dirname(DATA_FILE) or ".", "clusters.json"))
    flow = FlowMonitor(bot, store, tk, journal, cstore, idea_journal)
    newsmon = NewsMonitor(bot, store,
                          os.path.join(os.path.dirname(DATA_FILE) or ".", "news_seen.json"),
                          mode=os.getenv("NEWS_MODE", "stocks"))
    scr = scrmod.Screener(tk, os.path.dirname(DATA_FILE) or ".")
    cal = evmod.Calendar(os.path.join(os.path.dirname(DATA_FILE) or ".", "events.json"),
                         tk if tinkoff.enabled() else None)
    flow.screener, flow.newsmon, flow.calendar = scr, newsmon, cal

    async def amain() -> None:
        session = aiohttp.ClientSession(
            headers={"User-Agent": "moex-stockbot/1.0"},
            timeout=ClientTimeout(total=20),
        )
        # Сначала публикуем команды. Тяжёлый прогрев скринера и рыночного
        # потока не должен отнимать соединения в момент запуска бота.
        menu_ok = await setup_menu(bot)
        menu_task = asyncio.create_task(keep_menu_current(bot, menu_ok))
        monitor_task = asyncio.create_task(monitor.run(session))
        flow_task = asyncio.create_task(flow.run(session))
        news_task = asyncio.create_task(newsmon.run(session))
        health_task = asyncio.create_task(health_check(bot))
        clean_task = asyncio.create_task(cleaner.run_daily(bot, sent, store, moex.now_msk))
        scr_task = asyncio.create_task(scr.run(session)) if tinkoff.enabled() else None
        market.set_client(tk if tinkoff.enabled() else None)
        cal_task = asyncio.create_task(cal.run_reminders(bot, store, esc))
        csq = cscalp.CScalpQueue()
        runner = await csq.start() if (cscalp.enabled() and cscalp.HTTP_MODE) else None
        try:
            await dp.start_polling(bot, store=store, sess=session, tk=tk,
                                   journal=journal, newsmon=newsmon, cstore=cstore, flow=flow,
                                   idea_journal=idea_journal, csq=csq, sent=sent, scr=scr, cal=cal)
        finally:
            menu_task.cancel()
            cal_task.cancel()
            cstore.save(force=True)
            clean_task.cancel()
            if scr_task:
                scr_task.cancel()
            sent.save()
            if runner is not None:
                await runner.cleanup()
            monitor_task.cancel()
            flow_task.cancel()
            flow.close()
            news_task.cancel()
            health_task.cancel()
            await tk.close()
            await session.close()
            await bot.session.close()

    try:
        asyncio.run(amain())
    except (KeyboardInterrupt, SystemExit):
        print("Остановлено.")


if __name__ == "__main__":
    main()
