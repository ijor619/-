"""Клиент бесплатного ISS API Мосбиржи (https://iss.moex.com).

Ключевые особенности API, на которые опирается реализация:
- ответ candles.json отдаёт НЕ БОЛЕЕ 500 рядов, начиная с даты `from`
  (параметр `count` не работает, `to` игнорируется) => последняя цена
  ищется короткими окнами от 8 часов назад, а при повторном опросе
  начиная с временной метки последней увиденной свечи;
- «время» MOEX — московское, независимо от часового пояса сервера;
- таблица ценных бумаг (…/boards/{board}/securities/{ticker}.json)
  содержит название, валюту, число знаков и предыдущее «правильное»
  закрытие (PREVLEGALCLOSEPRICE + PREVDATE) — за один запрос.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp

from config import MOEX_BASE, MOEX_BOARDS, FRESHNESS_MIN

log = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Максимум 96 окон по 8 часов = 8 дней назад (хватает на праздники).
_PROBE_WINDOWS = 96
_PROBE_STEP = timedelta(minutes=480)  # < 500 рядов => без обрезки лимитом


def now_msk() -> datetime:
    """Текущее московское время (наивное, как в ответах MOEX)."""
    return datetime.now(MSK).replace(tzinfo=None)


@dataclass
class SecurityInfo:
    ticker: str
    name: str
    board: str
    currency: str
    decimals: int
    prev_close: float
    prev_date: str  # YYYY-MM-DD


@dataclass
class Quote:
    info: SecurityInfo
    price: float
    as_of: datetime          # время последней свечи (MSK)
    change_pct: float        # изменение к закрытию предыдущего дня, %
    trading: bool            # данные свежие => рынок торгует


def _rows(data: dict[str, Any]) -> tuple[list[str], list[list[Any]]]:
    """Вернуть (columns, data) из первого блока ответа ISS."""
    block = data[list(data.keys())[0]]
    return block["columns"], (block.get("data") or [])


async def _get(session: aiohttp.ClientSession, path: str,
               params: Optional[dict] = None,
               retries: int = 3) -> dict:
    """GET с ретраями: ISS API из-за TLS-сбоев иногда отвечает ошибкой."""
    last: Optional[Exception] = None
    for attempt in range(retries):
        try:
            async with session.get(MOEX_BASE + path, params=params) as r:
                r.raise_for_status()
                return await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last = e
            if attempt < retries - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
    raise last  # type: ignore[misc]


async def get_security_info(session: aiohttp.ClientSession,
                            ticker: str) -> Optional[SecurityInfo]:
    """Справочная информация по тикеру; None, если не найден/не торгуется."""
    t = ticker.upper().strip()
    for board in MOEX_BOARDS:
        try:
            data = await _get(
                session,
                f"/engines/stock/markets/shares/boards/{board}/securities/{t}.json",
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("MOEX: справочник %s/%s: %s", board, t, e)
            continue
        cols, rows = _rows(data)
        if not rows:
            continue
        r = dict(zip(cols, rows[0]))
        if r.get("STATUS") != "A":
            continue
        try:
            return SecurityInfo(
                ticker=t,
                name=str(r["SHORTNAME"]),
                board=board,
                currency=str(r.get("CURRENCYID") or "SUR"),
                decimals=int(r.get("DECIMALS") or 2),
                prev_close=float(r["PREVLEGALCLOSEPRICE"]),
                prev_date=str(r.get("PREVDATE") or ""),
            )
        except (KeyError, TypeError, ValueError) as e:
            log.warning("MOEX: некорректный справочник %s: %s", t, e)
            continue
    return None


async def _candles(session: aiohttp.ClientSession, ticker: str, board: str,
                   frm: datetime) -> list[list[Any]]:
    params = {"interval": 1, "from": frm.strftime("%Y-%m-%d %H:%M:%S")}
    data = await _get(
        session,
        f"/engines/stock/markets/shares/boards/{board}/securities/{ticker}/candles.json",
        params,
    )
    _, rows = _rows(data)
    return rows


async def get_last_candle(session: aiohttp.ClientSession, ticker: str,
                          board: str,
                          hint: Optional[datetime] = None) -> Optional[list[Any]]:
    """Последняя минутная свеча: [open, close, high, low, value, volume, begin, end].

    `hint` — время конца последней увиденной свечи; если с тех пор были новые
    свечи, возвращается только их «хвост» (один лёгкий запрос).
    """
    now = now_msk()
    if hint is not None:
        try:
            rows = await _candles(session, ticker, board, hint - timedelta(seconds=30))
            if rows:
                return rows[-1]
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("MOEX: свечи %s: %s", ticker, e)
            return None

    t = now
    for _ in range(_PROBE_WINDOWS):
        try:
            rows = await _candles(session, ticker, board, t - _PROBE_STEP)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("MOEX: свечи %s: %s", ticker, e)
            return None
        if rows:
            return rows[-1]
        t -= _PROBE_STEP
    return None


async def get_quote(session: aiohttp.ClientSession, info: SecurityInfo,
                    hint: Optional[datetime] = None) -> Optional[Quote]:
    """Актуальная котировка по уже известному SecurityInfo."""
    row = await get_last_candle(session, info.ticker, info.board, hint)
    if not row:
        return None
    cols_idx = {c: i for i, c in
                enumerate(["open", "close", "high", "low", "value", "volume",
                           "begin", "end"])}
    price = float(row[cols_idx["close"]])
    as_of = datetime.strptime(row[cols_idx["end"]], "%Y-%m-%d %H:%M:%S")
    change_pct = 0.0
    if info.prev_close > 0:
        change_pct = (price - info.prev_close) / info.prev_close * 100.0
    trading = (now_msk() - as_of) < timedelta(minutes=FRESHNESS_MIN)
    return Quote(info=info, price=price, as_of=as_of,
                 change_pct=change_pct, trading=trading)


async def fetch_quotes(session: aiohttp.ClientSession,
                       infos: dict[str, SecurityInfo],
                       hints: dict[str, datetime]) -> dict[str, Quote]:
    """Параллельно скачать котировки для всех тикеров."""
    out: dict[str, Quote] = {}

    async def one(t: str) -> None:
        info = infos[t]
        try:
            q = await get_quote(session, info, hints.get(t))
        except Exception:
            log.exception("MOEX: не удалось получить цену %s", t)
            return
        if q is not None:
            out[t] = q
            hints[t] = q.as_of

    await asyncio.gather(*(one(t) for t in infos))
    return out
