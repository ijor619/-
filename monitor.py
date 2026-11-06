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
from keyboards import alert_kb, report_kb
from tinkoff import enabled as flow_enabled
from store import Store, UserProfile

log = logging.getLogger(__name__)


class Monitor:
    def __init__(self, bot: Bot, store: Store, tk=None) -> None:
        self.bot = bot
        self.store = store
        self.tk = tk
        self._hints: Dict[str, datetime] = {}                    # тикер -> конец последней свечи
        self._sec_cache: Dict[str, tuple[float, Optional[SecurityInfo]]] = {}
        self._last_alert: Dict[tuple[int, str], float] = {}      # (user, тикер) -> unix-время
        # ступень, о которой уже сообщили: (user, тикер) -> (дата, int(change/threshold))
        # алерт уходит при переходе на новую ступень (±0.5%, ±1%, ±1.5% …), а не пока
        # бумага «висит» выше порога
        self._step: Dict[tuple[int, str], tuple[str, int]] = {}
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
        quotes = await moex.fetch_quotes(session, known, self._hints, self.tk)
        now_ts = time.time()

        hour = now_msk().hour
        for uid, prof in users:
            if prof.is_quiet(hour):
                continue
            for t in prof.watchlist:
                q = quotes.get(t)
                # Алерты — только когда рынок торгует (данные свежие),
                # чтобы не зациклиться на неизменной цене после закрытия.
                if q is None or not q.trading or prof.threshold_pct <= 0:
                    continue
                day = q.reference_key or now_msk().strftime("%Y-%m-%d")
                step = int(q.change_pct / prof.threshold_pct)   # 0 — внутри порога
                key = (uid, t)
                prev_day, prev_step = self._step.get(key, ("", 0))
                if prev_day != day:
                    self._step[key] = (day, step)
                    continue  # смена базы/источника не является движением цены
                if step == prev_step:
                    continue
                if self._first_tick:
                    self._step[key] = (day, step)
                    continue    # после рестарта запоминаем текущую ступень молча
                last = self._last_alert.get(key, 0.0)
                if now_ts - last < prof.cooldown_min * 60:
                    continue    # защита от дребезга на границе ступени
                if await self._send(uid, self._alert_text(q, prof, step, prev_step),
                                    alert_kb(t, flow_enabled())):
                    self._last_alert[key] = now_ts
                    self._step[key] = (day, step)

            if (prof.report_min > 0
                    and now_ts - prof.last_report_ts >= prof.report_min * 60):
                rows = [(t, quotes.get(t)) for t in prof.watchlist]
                if any(q is not None for _, q in rows):
                    if await self._send(uid, self._report_text(rows), report_kb(prof.watchlist)):
                        prof.last_report_ts = now_ts
                        self.store.save()

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
    def _alert_text(q: Quote, prof: UserProfile, step: int = 0, prev_step: int = 0) -> str:
        emoji = "📈" if q.change_pct > 0 else "📉"
        cur = cur_symbol(q.info.currency)
        thr = prof.threshold_pct
        if step == 0:
            what = f"вернулась внутрь ±{thr:g}%"
        elif step > prev_step:
            what = (f"прошла +{step * thr:g}%" if step > 0 else
                    f"восстановилась выше -{(abs(step) + 1) * thr:g}%")
        else:
            what = (f"прошла -{abs(step) * thr:g}%" if step < 0 else
                    f"откатилась ниже +{(step + 1) * thr:g}%")
        return (
            f"{emoji} <b>{esc(q.info.ticker)}</b> — "
            f"<b>{fmt_price(q.price, q.info.decimals)} {cur}</b>\n"
            f"{esc(q.info.name)}: <b>{fmt_pct(q.change_pct)}</b> — {what} "
            f"(шаг {thr:g}%)"
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
        return "\n".join(lines)

    async def _send(self, user_id: int, text: str, kb=None) -> bool:
        try:
            await self.bot.send_message(user_id, text, reply_markup=kb)
            return True
        except Exception:
            log.exception("не удалось отправить сообщение пользователю %s", user_id)
            return False
