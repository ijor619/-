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
from keyboards import chart_kb, report_kb
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
    "/chart TICKER [1m|5m|15m|30m|1h|4h|1d] — свечной график\n\n"
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
async def cmd_chart(m: Message, store: Store, sess: aiohttp.ClientSession) -> None:
    args = (m.text or "").split()[1:]
    if not args:
        await m.answer("Использование: /chart SBER [1m|5m|15m|30m|1h|4h|1d]")
        return
    t = args[0].upper()
    period = args[1].lower() if len(args) > 1 else charts.DEFAULT_PERIOD
    await _send_chart(m, sess, t, period)


async def _send_chart(m: Message, sess: aiohttp.ClientSession,
                      t: str, period: str) -> None:
    if not TICKER_RE.match(t):
        await m.answer("Не похоже на тикер.")
        return
    info = await moex.get_security_info(sess, t)
    if info is None:
        await m.answer(f"❌ <b>{esc(t)}</b> — не найден на Мосбирже")
        return
    png = await charts.build_chart(sess, info, period)
    if png is None:
        await m.answer(f"Нет данных для графика {t}.")
        return
    await m.answer_photo(BufferedInputFile(png, f"{t}_{period}.png"),
                         reply_markup=chart_kb(t, period))


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


# ------------------------------------------------------------- callbacks

@router.callback_query(F.data.startswith("chart:"))
async def cb_chart(c: CallbackQuery, sess: aiohttp.ClientSession) -> None:
    parts = c.data.split(":")
    t, period = parts[1], (parts[2] if len(parts) > 2 else charts.DEFAULT_PERIOD)
    if period not in charts.PERIODS:
        period = charts.DEFAULT_PERIOD
    await c.answer("Строю график…")
    try:
        info = await moex.get_security_info(sess, t)
        png = await charts.build_chart(sess, info, period) if info else None
    except Exception:
        log.exception("график %s", t)
        png = None
    if png is None:
        await c.message.answer(f"Нет данных для графика {esc(t)}.")
        return
    file = BufferedInputFile(png, f"{t}_{period}.png")
    kb = chart_kb(t, period)
    if c.message.photo:
        # уже график — заменяем картинку на месте (смена периода / обновление)
        try:
            await c.message.edit_media(InputMediaPhoto(media=file), reply_markup=kb)
            return
        except TelegramBadRequest as e:
            if "not modified" in str(e):
                return
        except Exception:
            log.exception("edit_media %s", t)
    await c.message.answer_photo(file, reply_markup=kb)


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

    async def amain() -> None:
        session = aiohttp.ClientSession(
            headers={"User-Agent": "moex-stockbot/1.0"},
            timeout=ClientTimeout(total=20),
        )
        monitor_task = asyncio.create_task(monitor.run(session))
        health_task = asyncio.create_task(health_check(bot))
        try:
            await dp.start_polling(bot, store=store, sess=session)
        finally:
            monitor_task.cancel()
            health_task.cancel()
            await session.close()
            await bot.session.close()

    try:
        asyncio.run(amain())
    except (KeyboardInterrupt, SystemExit):
        print("Остановлено.")


if __name__ == "__main__":
    main()
