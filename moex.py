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
from formatting import esc, fmt_price

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
    source: str = "MOEX ISS · резерв, возможна задержка"
    reference_price: float = 0.0
    reference_label: str = "к официальному закрытию MOEX"
    reference_key: str = ""
    reference_kind: str = "moex_close"

    def details(self) -> str:
        state = "" if self.trading else " · ⚠️ нет свежей сделки"
        return (f"<i>{esc(self.source)} · цена на {self.as_of:%d.%m %H:%M:%S} МСК{state}\n"
                f"База: {esc(self.reference_label)} — "
                f"{fmt_price(self.reference_price or self.info.prev_close, self.info.decimals)} ₽</i>")


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
            rows = await _candles(session, ticker, board, max(hint - timedelta(seconds=30), now - _PROBE_STEP))
            if rows:
                return rows[-1]
            # После ночи/выходных быстрый диапазон может быть пуст. Это не
            # означает, что цены нет: продолжаем обычный поиск назад.
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
    trading = timedelta(0) <= now_msk() - as_of < timedelta(minutes=FRESHNESS_MIN)
    return Quote(info=info, price=price, as_of=as_of,
                 change_pct=change_pct, trading=trading,
                 reference_label=f"закрытие MOEX {info.prev_date}",
                 reference_key=f"moex:{info.prev_date}:{info.prev_close}")


async def fetch_quotes(session: aiohttp.ClientSession,
                       infos: dict[str, SecurityInfo],
                       hints: dict[str, datetime],
                       tk=None) -> dict[str, Quote]:
    """Параллельно скачать котировки для всех тикеров.

    Если передан TinkoffClient — цены берутся из T-Invest в реальном времени
    (одним запросом на все бумаги), MOEX остаётся резервом для тех, по кому
    T-Invest ничего не вернул. Без токена — MOEX ISS (задержка 15 мин).
    """
    out: dict[str, Quote] = {}
    if tk is not None:
        found = await asyncio.gather(*(tk.instrument(t) for t in infos), return_exceptions=True)
        insts = [i for i in found if i is not None and not isinstance(i, BaseException)]
        if insts:
            prices_r, portfolio_r, closes_r = await asyncio.gather(
                tk.last_prices(insts), tk.portfolio_day_quotes(), tk.close_prices(insts),
                return_exceptions=True)
            prices = prices_r if isinstance(prices_r, dict) else {}
            portfolio = portfolio_r if isinstance(portfolio_r, dict) else {}
            closes = closes_r if isinstance(closes_r, dict) else {}
            if isinstance(portfolio_r, BaseException):
                log.warning("T-Invest GetPortfolio: %s; для бумаг без точной позиции беру закрытие", portfolio_r)
            if isinstance(closes_r, BaseException):
                log.warning("T-Invest GetClosePrices: %s", closes_r)
            for t, (market_price, ts) in prices.items():
                info = infos[t]
                as_of = ts.astimezone(MSK).replace(tzinfo=None)
                trading = timedelta(0) <= now_msk() - as_of < timedelta(seconds=90)
                pos = portfolio.get(t)
                close = closes.get(t)
                if pos is not None:
                    # Цена, дневной P/L и база относятся к одной позиции.
                    price, base, change = pos.price, pos.base_price, pos.change_pct
                    accounts = f" · счетов {pos.account_count}" if pos.account_count > 1 else ""
                    label = f"позиция T‑Invest «за сегодня»{accounts}"
                    source = "T‑Invest GetPortfolio · цена и дневная доходность позиции"
                    calc_day = (datetime.now(MSK) - timedelta(hours=4)).date()
                    # Числовая база округлена, чтобы микроскопический пересчёт
                    # MoneyValue не сбрасывал ступени на каждом опросе.
                    key = (f"portfolio:{calc_day}:{pos.quantity:g}:"
                           f"{base:.{info.decimals + 2}f}")
                    kind = "portfolio"
                elif close is not None:
                    base, close_ts, close_label = close
                    price = market_price
                    change = (price / base - 1) * 100 if base > 0 else 0.0
                    label = f"{close_label} {close_ts.astimezone(MSK):%d.%m %H:%M} МСК"
                    source = "T‑Invest · биржевая цена (бумаги нет в портфеле)"
                    key = f"close:{close_ts.isoformat()}:{base:.9f}"
                    kind = "tinvest_close"
                else:
                    continue  # нет надёжной базы — процент не выдумываем
                out[t] = Quote(info=info, price=price, as_of=as_of,
                               change_pct=change, trading=trading, source=source,
                               reference_price=base, reference_label=label,
                               reference_key=key, reference_kind=kind)
                hints[t] = as_of
    infos = {t: i for t, i in infos.items() if t not in out}
    if not infos:
        return out

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


# ------------------------------------------------------------ индекс IMOEX

async def index_candles(session: aiohttp.ClientSession, minutes: int = 60,
                        index: str = "IMOEX") -> list[list[Any]]:
    """Минутные свечи индекса: [open, close, high, low, value, volume, begin, end]."""
    frm = now_msk() - timedelta(minutes=minutes + 20)
    data = await _get(
        session,
        f"/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json",
        {"interval": 1, "from": frm.strftime("%Y-%m-%d %H:%M:%S")})
    _, rows = _rows(data)
    return rows


async def index_changes(session: aiohttp.ClientSession) -> dict[str, Optional[float]]:
    """Изменение IMOEX: {'15m': %, 'day': % к вчерашнему закрытию}. Значения None при ошибке."""
    out: dict[str, Optional[float]] = {"15m": None, "day": None}
    try:
        rows = await index_candles(session, 60)
        if rows:
            last = float(rows[-1][1])
            end = rows[-1][7]
            ago = [r for r in rows if r[7] <= (datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
                                               - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")]
            if ago:
                base = float(ago[-1][1])
                out["15m"] = (last - base) / base * 100 if base else None
        data = await _get(session, "/engines/stock/markets/index/boards/SNDX/securities/IMOEX.json",
                          {"iss.only": "marketdata", "marketdata.columns": "LASTCHANGEPRC"})
        _, md = _rows(data)
        if md and md[0][0] is not None:
            out["day"] = float(md[0][0])
    except Exception as e:
        log.debug("IMOEX: %s", e)
    return out


async def prev_day_hl(session: aiohttp.ClientSession, ticker: str,
                      board: str = "TQBR") -> tuple[float, float]:
    """Максимум и минимум последнего завершённого торгового дня (ISS history)."""
    frm = (now_msk() - timedelta(days=10)).strftime("%Y-%m-%d")
    today = now_msk().strftime("%Y-%m-%d")
    data = await _get(session,
                      f"/history/engines/stock/markets/shares/boards/{board}/securities/{ticker}.json",
                      {"from": frm, "iss.meta": "off", "history.columns": "TRADEDATE,HIGH,LOW"})
    rows = (data.get("history") or {}).get("data") or []
    rows = [r for r in rows if r[0] < today and r[1] and r[2]]
    if not rows:
        return 0.0, 0.0
    return float(rows[-1][1]), float(rows[-1][2])
