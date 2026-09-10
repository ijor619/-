"""Фоновый мониторинг цен: пороги-алерты и периодические сводки."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Optional

import aiohttp
from aiogram import Bot

import moex
from config import (CHECK_INTERVAL_SEC, SEC_INFO_TTL_SEC,
                    DEFAULT_THRESHOLD_PCT, DEFAULT_REPORT_MIN)
from formatting import cur_symbol, esc, fmt_pct, fmt_price
from moex import Quote, SecurityInfo, now_msk
from store import Store, UserProfile

log = logging.getLogger(__name__)


class Monitor:
    def __init__(self, bot: Bot, store: Store) -> None:
        self.bot = bot
        self.store = store
        self._hints: Dict[str, datetime] = {}                    # тикер -> конец последней свечи
        self._sec_cache: Dict[str, tuple[float, Optional[SecurityInfo]]] = {}
        self._last_alert: Dict[tuple[int, str], float] = {}      # (user, тикер) -> unix-время
        self._first_tick = True

    # ------------------------------------------------------------------ run
    async def run(self, session: aiohttp.ClientSession) -> None:
        log.info("мониторинг запущен, опрос каждые %s c", CHECK_INTERVAL_SEC)
        while True:
            t0 = time.monotonic()
            try:
                await self.tick(session)
            except Exception:
                log.exception("мониторинг: ошибка тика")
            elapsed = time.monotonic() - t0
            await asyncio.sleep(max(5.0, CHECK_INTERVAL_SEC - elapsed))

    # ------------------------------------------------------------------ tick
    async def tick(self, session: aiohttp.ClientSession) -> None:
        users = self.store.all()
        tickers = sorted({t for _, p in users for t in p.watchlist})
        if not tickers:
            self._first_tick = False
            return

        infos = await self._load_infos(session, tickers)
        known = {t: i for t, i in infos.items() if i is not None}
        quotes = await moex.fetch_quotes(session, known, self._hints)
        now_ts = time.time()

        for uid, prof in users:
            for t in prof.watchlist:
                q = quotes.get(t)
                # Алерты — только когда рынок торгует (данные свежие),
                # чтобы не зациклиться на неизменной цене после закрытия.
                if (q is not None and q.trading
                        and abs(q.change_pct) >= prof.threshold_pct
                        and not self._first_tick):
                    last = self._last_alert.get((uid, t), 0.0)
                    if now_ts - last >= prof.cooldown_min * 60:
                        self._last_alert[(uid, t)] = now_ts
                        await self._send(uid, self._alert_text(q, prof))

            if (prof.report_min > 0
                    and now_ts - prof.last_report_ts >= prof.report_min * 60):
                rows = [(t, quotes.get(t)) for t in prof.watchlist]
                if any(q is not None for _, q in rows):
                    prof.last_report_ts = now_ts
                    self.store.save()
                    await self._send(uid, self._report_text(rows))

        self._first_tick = False

    # ------------------------------------------------------------ справочник
    async def _load_infos(self, session: aiohttp.ClientSession,
                          tickers: list[str]) -> Dict[str, Optional[SecurityInfo]]:
        now_ts = time.time()
        out: Dict[str, Optional[SecurityInfo]] = {}
        missing = []
        for t in tickers:
            hit = self._sec_cache.get(t)
            if hit is not None and now_ts - hit[0] < SEC_INFO_TTL_SEC:
                out[t] = hit[1]
            else:
                missing.append(t)

        async def one(t: str) -> None:
            try:
                info = await moex.get_security_info(session, t)
            except Exception:
                log.exception("не удалось получить справочник %s", t)
                info = None
            self._sec_cache[t] = (now_ts, info)
            out[t] = info

        await asyncio.gather(*(one(t) for t in missing))
        return out

    # ------------------------------------------------------------- сообщения
    @staticmethod
    def _alert_text(q: Quote, prof: UserProfile) -> str:
        emoji = "📈" if q.change_pct > 0 else "📉"
        cur = cur_symbol(q.info.currency)
        d = q.info.prev_date
        date_str = f"{d[8:10]}.{d[5:7]}" if len(d) == 10 else d
        return (
            f"{emoji} <b>{esc(q.info.ticker)}</b> — "
            f"<b>{fmt_price(q.price, q.info.decimals)} {cur}</b>\n"
            f"{esc(q.info.name)}: <b>{fmt_pct(q.change_pct)}</b> за день "
            f"(порог {prof.threshold_pct:g}%)\n"
            f"Пред. закрытие {date_str}: "
            f"{fmt_price(q.info.prev_close, q.info.decimals)} {cur}"
        )

    @staticmethod
    def _report_text(rows: list[tuple[str, Optional[Quote]]]) -> str:
        lines = [f"📊 <b>Сводка</b> · {now_msk().strftime('%d.%m %H:%M')} МСК"]
        for t, q in rows:
            if q is None:
                lines.append(f"⚪ <b>{esc(t)}</b> — нет данных")
            else:
                cur = cur_symbol(q.info.currency)
                arrow = ("⬆️" if q.change_pct > 0.005
                         else "⬇️" if q.change_pct < -0.005 else "⚪")
                lines.append(
                    f"{arrow} <b>{esc(t)}</b> · "
                    f"{fmt_price(q.price, q.info.decimals)} {cur} · "
                    f"{fmt_pct(q.change_pct)}"
                )
        trading = any(q is not None and q.trading for _, q in rows)
        lines.append(f"Рынок: {'открыт' if trading else 'закрыт'}")
        return "\n".join(lines)

    async def _send(self, user_id: int, text: str) -> None:
        try:
            await self.bot.send_message(user_id, text)
        except Exception:
            log.exception("не удалось отправить сообщение пользователю %s", user_id)
