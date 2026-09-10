"""Фоновый монитор ленты сделок и стакана через T-Invest API."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional

from aiogram import Bot

import moex
import tape
from keyboards import flow_kb
from store import Store
from tinkoff import TinkoffClient, enabled

log = logging.getLogger(__name__)

FLOW_INTERVAL_SEC = 20          # опрос ленты/стакана
SIGNAL_COOLDOWN_SEC = 15 * 60   # один и тот же сигнал по бумаге — не чаще
MAX_SIGNALS_PER_TICK = 3        # на пользователя за тик


class FlowMonitor:
    def __init__(self, bot: Bot, store: Store, tk: TinkoffClient) -> None:
        self.bot, self.store, self.tk = bot, store, tk
        self._books: Dict[str, tape.BookState] = {}
        self._sent: Dict[tuple[int, str], float] = {}   # (uid, key) -> ts
        self._dec: Dict[str, int] = {}

    async def run(self, sess) -> None:
        if not enabled():
            log.info("flow: TINKOFF_TOKEN не задан — анализ ленты/стакана выключен")
            return
        log.info("flow: анализ ленты и стакана запущен, опрос каждые %s c", FLOW_INTERVAL_SEC)
        while True:
            t0 = time.monotonic()
            try:
                await self.tick(sess)
            except Exception:
                log.exception("flow: ошибка тика")
            await asyncio.sleep(max(5.0, FLOW_INTERVAL_SEC - (time.monotonic() - t0)))

    async def _decimals(self, sess, t: str) -> int:
        if t not in self._dec:
            info = await moex.get_security_info(sess, t)
            self._dec[t] = info.decimals if info else 2
        return self._dec[t]

    async def tick(self, sess) -> None:
        users = [(u, p) for u, p in self.store.all() if p.flow_alerts and p.watchlist]
        tickers = sorted({t for _, p in users for t in p.watchlist})
        if not tickers:
            return
        # торгует ли рынок — по свежести MOEX-данных не проверяем каждый раз,
        # достаточно того, что лента пустая вне торгов
        signals: Dict[str, list[tape.Signal]] = {}
        for t in tickers:
            try:
                signals[t] = await self._analyze(sess, t)
            except Exception as e:
                log.warning("flow: %s: %s", t, e)
        now = time.time()
        for uid, prof in users:
            n = 0
            for t in prof.watchlist:
                for sig in signals.get(t, []):
                    k = (uid, f"{t}:{sig.key}")
                    kk = (uid, f"{t}:{sig.kind}")
                    if now - self._sent.get(k, 0) < SIGNAL_COOLDOWN_SEC:
                        continue
                    if now - self._sent.get(kk, 0) < SIGNAL_COOLDOWN_SEC / 3:
                        continue
                    self._sent[k] = self._sent[kk] = now
                    try:
                        await self.bot.send_message(uid, sig.text, reply_markup=flow_kb(t))
                    except Exception:
                        log.exception("flow: не отправлено %s", uid)
                    n += 1
                    if n >= MAX_SIGNALS_PER_TICK:
                        break
                if n >= MAX_SIGNALS_PER_TICK:
                    break
        # чистим старые ключи
        if len(self._sent) > 5000:
            self._sent = {k: v for k, v in self._sent.items() if now - v < 3600}

    async def _analyze(self, sess, t: str) -> list[tape.Signal]:
        try:
            inst = await self.tk.instrument(t)
        except Exception as e:
            log.warning("flow: T-Invest недоступен (%s): %s", t, e)
            return []
        if inst is None:
            return []
        dec = await self._decimals(sess, t)
        trades, ob = await asyncio.gather(
            self.tk.last_trades(inst, minutes=10),
            self.tk.order_book(inst, depth=20),
            return_exceptions=True)
        out: list[tape.Signal] = []
        if isinstance(trades, list):
            out += tape.analyze_trades(t, trades, inst.lot, dec)
        elif trades:
            log.warning("flow: лента %s: %s", t, trades)
        if ob is not None and not isinstance(ob, BaseException):
            st = self._books.setdefault(t, tape.BookState())
            out += tape.analyze_book(t, ob, st, inst.lot, dec)
        elif isinstance(ob, BaseException):
            log.warning("flow: стакан %s: %s", t, ob)
        return out
