"""Телеграм-бот: слежение за ценами акций Мосбиржи.

Запуск:
    export BOT_TOKEN="123456:ABC..."   (или положить в .env)
    python bot.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sys

import aiohttp
from aiohttp import ClientTimeout
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (BufferedInputFile, CallbackQuery, InputMediaPhoto,
                           Message)

import charts
import moex
import tape
import tinkoff
from flow import FlowMonitor
from journal import Journal
from keyboards import book_kb, chart_kb, report_kb, tape_kb
from config import (BOT_TOKEN, CHECK_INTERVAL_SEC, DATA_FILE,
                    DEFAULT_REPORT_MIN, DEFAULT_THRESHOLD_PCT)
from formatting import cur_symbol, esc, fmt_pct, fmt_price
from monitor import Monitor
from store import Store

log = logging.getLogger(__name__)

router = Router(name="stockbot")

TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,9}$")

HELP = (
    "📈 Я слежу за акциями Мосбиржи и присылаю изменения цен.\n\n"
    "<b>Команды</b>\n"
    "/watch TICKER [TICKER …] — добавить акции, напр. /watch SBER GAZP\n"
    "/unwatch TICKER — убрать из списка\n"
    "/list — список с текущими ценами\n"
    "/alert N — алерт, если цена уйдёт более чем на N% за день (0.1–50)\n"
    "/quiet N — мин. пауза между повторными алертами, мин\n"
    "/report N — сводка каждые N минут, 0 — выключить\n"
    "/chart TICKER [1m|5m|15m|30m|1h|4h|1d] — свечной график\n"
    "/book TICKER — стакан, /tape TICKER — лента сделок\n"
    "/flow on|off — сигналы о роботах (айсберги, спуфинг, перекосы)\n"
    "/stats [7] [TICKER] — точность сигналов за N дней\n"
    "/backtest TICKER — прогнать детекторы по ленте за час\n"
    "/quiethours 23 9 — тихие часы (МСК), off — выключить\n\n"
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
        + "\nТихие часы: "
        + (f"{p.quiet_from:02d}:00–{p.quiet_to:02d}:00 МСК" if p.quiet_from >= 0 else "нет")
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


async def _list(m, store: Store, sess: aiohttp.ClientSession) -> str:
    prof = store.get(m.from_user.id)
    if not prof.watchlist:
        return "Список пуст. Добавь: /watch SBER GAZP"
    lines = [f"📋 <b>Твои акции</b> · {moex.now_msk().strftime('%d.%m %H:%M')} МСК"]
    for t in prof.watchlist:
        try:
            info = await moex.get_security_info(sess, t)
            q = await moex.get_quote(sess, info) if info else None
        except Exception:
            log.exception("не удалось получить данные %s", t)
            q = None
        if q is None:
            lines.append(f"⚪ <b>{esc(t)}</b> — нет данных")
        else:
            cur = cur_symbol(q.info.currency)
            arrow = ("⬆️" if q.change_pct > 0.005
                     else "⬇️" if q.change_pct < -0.005 else "⚪")
            lines.append(
                f"{arrow} <b>{t}</b> · {fmt_price(q.price, q.info.decimals)} {cur} · "
                f"{fmt_pct(q.change_pct)}"
            )
    return "\n".join(lines)


# id единственного сообщения-графика в каждом чате: chat_id -> message_id
_chart_msg: dict[int, int] = {}


async def show_chart(bot: Bot, chat_id: int, sess: aiohttp.ClientSession,
                     store: Store, t: str, period: str,
                     current: Message | None = None, tk=None) -> str | None:
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
    png = await charts.build_chart(sess, info, period,
                                   tk if tinkoff.enabled() else None)
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
        except Exception:
            log.exception("edit_media %s", t)

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
        "Привет! Я слежу за ценами акций на Мосбирже 📈\n\n"
        "Добавь тикеры: /watch SBER GAZP\n"
        "Или просто напиши тикер обычным сообщением.\n\n"
        "Вся информация — /help"
    )


@router.message(Command("help"))
async def cmd_help(m: Message, store: Store) -> None:
    await m.answer(_help_text(store, m.from_user.id))


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
async def cmd_list(m: Message, store: Store, sess: aiohttp.ClientSession) -> None:
    prof = store.get(m.from_user.id)
    await m.answer(await _list(m, store, sess),
                   reply_markup=report_kb(prof.watchlist) if prof.watchlist else None)


@router.message(Command("chart"))
async def cmd_chart(m: Message, store: Store, sess: aiohttp.ClientSession,
                    tk: tinkoff.TinkoffClient) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        await m.answer("Использование: /chart SBER [1m|5m|15m|30m|1h|4h|1d]")
        return
    t = args[0].upper()
    period = args[1].lower() if len(args) > 1 else charts.DEFAULT_PERIOD
    if not TICKER_RE.match(t):
        await m.answer("Не похоже на тикер.")
        return
    err = await show_chart(m.bot, m.chat.id, sess, store, t, period, tk=tk)
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
    await m.answer(f"🔔 Порог алерта: {v:g}% за день (от предыдущего закрытия).")


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


@router.message(Command("flow"))
async def cmd_flow(m: Message, store: Store) -> None:
    args = (m.text or "").split()[1:]
    p = store.get(m.from_user.id)
    if not tinkoff.enabled():
        await m.answer(NO_TK); return
    if args and args[0].lower() in ("on", "off"):
        p.flow_alerts = args[0].lower() == "on"
        store.save()
    await m.answer(
        f"🤖 Сигналы ленты/стакана: <b>{'включены' if p.flow_alerts else 'выключены'}</b>\n"
        "Айсберги 🧊, ритмичные роботы 🤖, перекосы ⚖️, всплески 🔥, "
        "плотности 🧱, спуфинг 👻.\nПереключить: /flow on | /flow off")


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


@router.message(Command("stats"))
async def cmd_stats(m: Message, journal: Journal) -> None:
    args = (m.text or "").split()[1:]
    days, ticker = 7, None
    for a in args:
        if a.isdigit():
            days = max(1, min(int(a), 14))
        elif TICKER_RE.match(a.upper()):
            ticker = a.upper()
    await m.answer(journal.stats(m.from_user.id, days, ticker))


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
    if len(parts) > 2 and parts[2] == "r":
        try:
            await c.message.edit_text(text, reply_markup=book_kb(t)); return
        except TelegramBadRequest as e:
            if "not modified" in str(e): return
    await c.message.answer(text, reply_markup=book_kb(t))


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
    if len(parts) > 2 and parts[2] == "r":
        try:
            await c.message.edit_text(text, reply_markup=tape_kb(t)); return
        except TelegramBadRequest as e:
            if "not modified" in str(e): return
    await c.message.answer(text, reply_markup=tape_kb(t))


@router.callback_query(F.data == "flow:off")
async def cb_flow_off(c: CallbackQuery, store: Store) -> None:
    p = store.get(c.from_user.id)
    p.flow_alerts = False
    store.save()
    await c.answer("Сигналы ленты выключены")
    await c.message.answer("🔕 Сигналы ленты/стакана выключены. Включить: /flow on")


# ------------------------------------------------------------- callbacks

@router.callback_query(F.data == "chart:close")
async def cb_chart_close(c: CallbackQuery) -> None:
    await c.answer()
    _chart_msg.pop(c.message.chat.id, None)
    try:
        await c.message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("chart:"))
async def cb_chart(c: CallbackQuery, store: Store, sess: aiohttp.ClientSession,
                   tk: tinkoff.TinkoffClient) -> None:
    parts = c.data.split(":")
    t, period = parts[1], (parts[2] if len(parts) > 2 else charts.DEFAULT_PERIOD)
    await c.answer("Строю график…")
    try:
        err = await show_chart(c.bot, c.message.chat.id, sess, store, t, period,
                               current=c.message, tk=tk)
    except Exception as e:
        log.exception("график %s", t)
        err = f"Не удалось построить график {esc(t)}: {esc(e)}"
    if err:
        await c.answer(err.replace("<b>", "").replace("</b>", ""), show_alert=True)


@router.callback_query(F.data == "refresh")
async def cb_refresh(c: CallbackQuery, store: Store, sess: aiohttp.ClientSession) -> None:
    await c.answer("Обновляю…")
    prof = store.get(c.from_user.id)
    text = await _list(c, store, sess)
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


# ----------------------------------------------------------------- fallback

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

    bot = Bot(token=BOT_TOKEN,
              default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    store = Store(DATA_FILE)
    monitor = Monitor(bot, store)
    tk = tinkoff.TinkoffClient()
    journal = Journal(os.path.join(os.path.dirname(DATA_FILE) or ".", "signals.json"))
    flow = FlowMonitor(bot, store, tk, journal)

    async def amain() -> None:
        session = aiohttp.ClientSession(
            headers={"User-Agent": "moex-stockbot/1.0"},
            timeout=ClientTimeout(total=20),
        )
        monitor_task = asyncio.create_task(monitor.run(session))
        flow_task = asyncio.create_task(flow.run(session))
        health_task = asyncio.create_task(health_check(bot))
        try:
            await dp.start_polling(bot, store=store, sess=session, tk=tk,
                                   journal=journal)
        finally:
            monitor_task.cancel()
            flow_task.cancel()
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
