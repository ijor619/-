"""Клиент T-Invest API (REST/JSON) — стакан и лента обезличенных сделок.

Только чтение: используются MarketDataService и InstrumentsService.
Токен берётся из переменной окружения TINKOFF_TOKEN и никуда не логируется.
Сертификаты Минцифры (certs/) нужны, т.к. invest-public-api.tinkoff.ru
подписан российским CA, которого нет в системных хранилищах вне РФ.
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp

import config  # noqa: F401  (подгружает .env до чтения токена)

log = logging.getLogger(__name__)

BASE = "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1."
TOKEN = os.getenv("TINKOFF_TOKEN", "").strip()
CERTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")


def enabled() -> bool:
    return bool(TOKEN)


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if os.path.isdir(CERTS_DIR):
        for f in os.listdir(CERTS_DIR):
            if f.endswith(".pem"):
                try:
                    ctx.load_verify_locations(os.path.join(CERTS_DIR, f))
                except ssl.SSLError as e:
                    log.warning("сертификат %s не загружен: %s", f, e)
    return ctx


def q2f(q: Optional[dict]) -> float:
    """Quotation {units, nano} -> float."""
    if not q:
        return 0.0
    return int(q.get("units") or 0) + int(q.get("nano") or 0) / 1e9


@dataclass
class Instrument:
    ticker: str
    uid: str
    figi: str
    lot: int
    name: str


@dataclass
class Trade:
    ts: datetime      # UTC
    price: float
    qty: int          # в лотах
    side: str         # "B" / "S"


@dataclass
class Level:
    price: float
    qty: int


@dataclass
class OrderBook:
    ts: datetime
    bids: list[Level]  # от лучшей (высшей) цены вниз
    asks: list[Level]  # от лучшей (низшей) цены вверх
    last: float


class TinkoffClient:
    def __init__(self) -> None:
        self._sess: Optional[aiohttp.ClientSession] = None
        self._instr: dict[str, Optional[Instrument]] = {}

    async def _session(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {TOKEN}",
                         "Content-Type": "application/json",
                         "x-app-name": "moex-stockbot"},
                connector=aiohttp.TCPConnector(ssl=ssl_context()),
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._sess

    async def close(self) -> None:
        if self._sess and not self._sess.closed:
            await self._sess.close()

    async def _call(self, method: str, body: dict[str, Any]) -> dict[str, Any]:
        sess = await self._session()
        last: Optional[Exception] = None
        for attempt in range(3):
            try:
                async with sess.post(BASE + method, json=body) as r:
                    if r.status == 429:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    data = await r.json(content_type=None)
                    if r.status >= 400:
                        raise RuntimeError(
                            f"T-Invest {method.split('/')[-1]} {r.status}: "
                            f"{data.get('message') if isinstance(data, dict) else data}")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = e
                await asyncio.sleep(1.0 * (attempt + 1))
        raise last or RuntimeError("T-Invest: rate limit")

    # ------------------------------------------------------------ методы
    async def instrument(self, ticker: str) -> Optional[Instrument]:
        t = ticker.upper()
        if t in self._instr:
            return self._instr[t]
        inst: Optional[Instrument] = None
        try:
            d = await self._call("InstrumentsService/ShareBy", {
                "idType": "INSTRUMENT_ID_TYPE_TICKER", "classCode": "TQBR", "id": t})
            i = d.get("instrument") or {}
            if i:
                inst = Instrument(t, i["uid"], i.get("figi", ""),
                                  int(i.get("lot") or 1), i.get("name", t))
        except Exception as e:
            log.warning("T-Invest: инструмент %s: %s", t, e)
            return None  # не кэшируем ошибку
        self._instr[t] = inst
        return inst

    async def order_book(self, inst: Instrument, depth: int = 20) -> Optional[OrderBook]:
        d = await self._call("MarketDataService/GetOrderBook",
                             {"instrumentId": inst.uid, "depth": depth})
        bids = [Level(q2f(x["price"]), int(x["quantity"])) for x in d.get("bids", [])]
        asks = [Level(q2f(x["price"]), int(x["quantity"])) for x in d.get("asks", [])]
        if not bids and not asks:
            return None
        return OrderBook(datetime.now(timezone.utc), bids, asks, q2f(d.get("lastPrice")))

    async def last_trades(self, inst: Instrument, minutes: int = 10) -> list[Trade]:
        now = datetime.now(timezone.utc)
        frm = now - timedelta(minutes=min(minutes, 60))
        d = await self._call("MarketDataService/GetLastTrades", {
            "instrumentId": inst.uid,
            "from": frm.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ")})
        out = []
        for x in d.get("trades", []):
            ts = datetime.fromisoformat(x["time"].replace("Z", "+00:00"))
            side = "B" if x.get("direction") == "TRADE_DIRECTION_BUY" else "S"
            out.append(Trade(ts, q2f(x["price"]), int(x.get("quantity") or 0), side))
        out.sort(key=lambda t: t.ts)
        return out
