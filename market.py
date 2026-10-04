"""Контекст рынка: IMOEX, RTS, юань, доллар (фьючерс), Brent — одной строкой.

Основной источник — T-Invest (реальное время): индексы — «индикативы»,
CNYRUB_TOM — валюты, USDRUBF и ближайший BR — фьючерсы; изменение считается
к закрытию прошлого дня (GetClosePrices). Если T-Invest недоступен/нет токена —
запасной вариант MOEX ISS (задержка 15 мин). Кэш 60 с.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import asyncio
from datetime import datetime, timezone

import aiohttp

from formatting import fmt_pct
from tinkoff import q2f

log = logging.getLogger(__name__)

ISS = "https://iss.moex.com/iss"
TTL = 60


class Snapshot:
    __slots__ = ("imoex", "imoex_pct", "rts", "rts_pct", "cny", "cny_pct",
                 "usd", "usd_pct", "brent", "brent_pct", "brent_code", "ts", "source")

    def __init__(self) -> None:
        for k in self.__slots__:
            setattr(self, k, None)
        self.ts = 0.0

    def ok(self) -> bool:
        return self.imoex is not None


_cache = Snapshot()
_tk = None                       # TinkoffClient, задаётся из bot.py
_ids: dict[str, str] = {}        # ключ -> uid инструмента в T-Invest
_ids_ts = 0.0
_brent_code = ""
_hist: list[tuple[float, float]] = []    # (ts, IMOEX) — для изменения за 15 мин в реальном времени


def _remember(s: "Snapshot") -> None:
    if s.imoex:
        _hist.append((s.ts, s.imoex))
        cut = time.time() - 3 * 3600
        while _hist and _hist[0][0] < cut:
            _hist.pop(0)


def imoex_change(minutes: int) -> Optional[float]:
    """Изменение IMOEX за N минут по собственной истории (realtime). None — мало истории."""
    if not _hist:
        return None
    target = time.time() - minutes * 60
    base = None
    for ts, v in _hist:
        if ts <= target:
            base = v
        else:
            break
    if base is None or len(_hist) < 2:
        return None
    last = _hist[-1][1]
    return (last - base) / base * 100 if base else None


def set_client(tk) -> None:
    global _tk
    _tk = tk


async def _resolve_ids() -> dict[str, str]:
    """Найти uid: IMOEX, RTSI (индикативы), CNYRUB_TOM (валюта), USDRUBF и
    ближайший BR (фьючерсы). Раз в сутки."""
    global _ids, _ids_ts, _brent_code
    if _ids and time.time() - _ids_ts < 86400:
        return _ids
    ids: dict[str, str] = {}
    d = await _tk._call("InstrumentsService/Indicatives", {})
    for i in d.get("instruments", []):
        if i.get("ticker") in ("IMOEX", "RTSI") and i.get("uid"):
            ids[i["ticker"]] = i["uid"]
    d = await _tk._call("InstrumentsService/Currencies", {"instrumentStatus": "INSTRUMENT_STATUS_ALL"})
    for i in d.get("instruments", []):
        if i.get("ticker") == "CNYRUB_TOM" and i.get("uid"):
            ids["CNY"] = i["uid"]
    d = await _tk._call("InstrumentsService/Futures", {"instrumentStatus": "INSTRUMENT_STATUS_ALL"})
    now = datetime.now(timezone.utc).isoformat()
    brent = []
    for i in d.get("instruments", []):
        t = i.get("ticker", "")
        if t == "USDRUBF" and i.get("uid"):
            ids["USD"] = i["uid"]
        if i.get("basicAsset") == "BR" and i.get("uid") and (i.get("expirationDate") or "") > now:
            brent.append((i["expirationDate"], t, i["uid"]))
    if brent:
        brent.sort()
        ids["BRENT"] = brent[0][2]
        _brent_code = brent[0][1]
    _ids, _ids_ts = ids, time.time()
    log.info("market: инструменты T-Invest: %s", ", ".join(sorted(ids)))
    return ids


async def _snapshot_tinvest() -> Snapshot:
    ids = await _resolve_ids()
    if not ids.get("IMOEX"):
        raise RuntimeError("IMOEX не найден среди индикативов")
    uids = list(ids.values())
    last, close = await asyncio.gather(
        _tk._call("MarketDataService/GetLastPrices", {"instrumentId": uids}),
        _tk._call("MarketDataService/GetClosePrices", {"instruments": [{"instrumentId": u} for u in uids]}))
    lp = {p.get("instrumentUid"): q2f(p.get("price")) for p in last.get("lastPrices", [])}
    cp = {p.get("instrumentUid"): q2f(p.get("price")) for p in close.get("closePrices", [])}
    s = Snapshot()
    s.ts = time.time()

    def pick(key: str) -> tuple[Optional[float], Optional[float]]:
        u = ids.get(key)
        if not u or not lp.get(u):
            return None, None
        v, c = lp[u], cp.get(u)
        return v, ((v - c) / c * 100 if c else None)

    s.imoex, s.imoex_pct = pick("IMOEX")
    s.rts, s.rts_pct = pick("RTSI")
    s.cny, s.cny_pct = pick("CNY")
    s.usd, s.usd_pct = pick("USD")
    s.brent, s.brent_pct = pick("BRENT")
    s.brent_code = _brent_code
    return s


async def _get(sess: aiohttp.ClientSession, path: str, params: dict[str, Any]) -> dict:
    params = {"iss.meta": "off", **params}
    async with sess.get(ISS + path, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
        r.raise_for_status()
        return await r.json(content_type=None)


def _rows(d: dict, block: str) -> list[dict]:
    b = d.get(block) or {}
    cols = b.get("columns") or []
    return [dict(zip(cols, row)) for row in b.get("data") or []]


async def snapshot(sess: aiohttp.ClientSession, force: bool = False) -> Snapshot:
    global _cache
    if not force and time.time() - _cache.ts < TTL and _cache.ok():
        return _cache
    if _tk is not None:
        try:
            s = await _snapshot_tinvest()
            if s.ok():
                s.source = "T-Invest realtime"
                _cache = s
                _remember(s)
                return s
        except Exception as e:
            log.warning("market: T-Invest недоступен, беру MOEX: %s", e)
    return await _snapshot_moex(sess)


async def _snapshot_moex(sess: aiohttp.ClientSession) -> Snapshot:
    global _cache
    s = Snapshot()
    s.source = "MOEX, задержка до 15 мин"
    s.ts = time.time()
    # индексы
    try:
        d = await _get(sess, "/engines/stock/markets/index/securities.json",
                       {"securities": "IMOEX,RTSI", "iss.only": "marketdata",
                        "marketdata.columns": "SECID,CURRENTVALUE,LASTCHANGEPRC"})
        for r in _rows(d, "marketdata"):
            if r["SECID"] == "IMOEX":
                s.imoex, s.imoex_pct = r["CURRENTVALUE"], r["LASTCHANGEPRC"]
            elif r["SECID"] == "RTSI":
                s.rts, s.rts_pct = r["CURRENTVALUE"], r["LASTCHANGEPRC"]
    except Exception as e:
        log.debug("market: индексы: %s", e)
    # юань (валютный рынок)
    try:
        d = await _get(sess, "/engines/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM.json",
                       {"iss.only": "marketdata", "marketdata.columns": "SECID,LAST,LASTCHANGEPRCNT"})
        for r in _rows(d, "marketdata"):
            if r.get("LAST"):
                s.cny, s.cny_pct = r["LAST"], r.get("LASTCHANGEPRCNT")
    except Exception as e:
        log.debug("market: CNY: %s", e)
    # доллар — вечный фьючерс USDRUBF (биржевого спота нет)
    try:
        d = await _get(sess, "/engines/futures/markets/forts/securities/USDRUBF.json",
                       {"iss.only": "marketdata", "marketdata.columns": "SECID,LAST,LASTTOPREVPRICE"})
        for r in _rows(d, "marketdata"):
            if r.get("LAST"):
                s.usd, s.usd_pct = r["LAST"], r.get("LASTTOPREVPRICE")
    except Exception as e:
        log.debug("market: USD: %s", e)
    # Brent — ближайший фьючерс BR
    try:
        d = await _get(sess, "/engines/futures/markets/forts/securities.json",
                       {"iss.only": "securities,marketdata",
                        "securities.columns": "SECID,LASTDELDATE,ASSETCODE",
                        "marketdata.columns": "SECID,LAST,LASTTOPREVPRICE"})
        br = {r["SECID"]: r for r in _rows(d, "securities") if r.get("ASSETCODE") == "BR"}
        md = {r["SECID"]: r for r in _rows(d, "marketdata") if r["SECID"] in br}
        for code in sorted(br, key=lambda k: br[k]["LASTDELDATE"] or "9"):
            m = md.get(code)
            if m and m.get("LAST"):
                s.brent, s.brent_pct, s.brent_code = m["LAST"], m.get("LASTTOPREVPRICE"), code
                break
    except Exception as e:
        log.debug("market: Brent: %s", e)
    if s.ok():
        _cache = s
        _remember(s)
        return s
    return _cache if _cache.ok() else s


def _p(v: Optional[float]) -> str:
    return fmt_pct(v) if v is not None else "—"


def _n(v: Optional[float], dec: int = 2) -> str:
    if v is None:
        return "—"
    return f"{v:,.{dec}f}".replace(",", " ").replace(".", ",")


def line(s: Snapshot) -> str:
    """Короткая строка для сетапа: 🌐 IMOEX +0,5% · RTS +0,5% · ¥ 12,50 · $ 84,1 · Brent 102,6 (−0,3%)."""
    if not s.ok():
        return ""
    parts = [f"IMOEX {_p(s.imoex_pct)}", f"RTS {_p(s.rts_pct)}"]
    if s.cny:
        parts.append(f"¥ {_n(s.cny, 2)} ({_p(s.cny_pct)})")
    if s.usd:
        parts.append(f"$ {_n(s.usd, 1)} ({_p(s.usd_pct)})")
    if s.brent:
        parts.append(f"Brent {_n(s.brent, 1)} ({_p(s.brent_pct)})")
    return "🌐 " + " · ".join(parts)


def text(s: Snapshot) -> str:
    """Развёрнуто для /market."""
    if not s.ok():
        return "🌐 Рынок: данные MOEX недоступны, попробуй позже."
    rows = [
        ("IMOEX", _n(s.imoex, 0), s.imoex_pct),
        ("RTS", _n(s.rts, 0), s.rts_pct),
        ("CNY/RUB", _n(s.cny, 3), s.cny_pct),
        ("USD/RUB (фьюч.)", _n(s.usd, 2), s.usd_pct),
        (f"Brent ({s.brent_code or 'BR'})", _n(s.brent, 2), s.brent_pct),
    ]
    out = ["🌐 <b>Рынок сейчас</b>", "<pre>"]
    for name, val, pct in rows:
        if val == "—":
            continue
        out.append(f"{name:<16}{val:>10}  {_p(pct):>7}")
    out.append("</pre>")
    out.append(f"<i>Изменения — к закрытию предыдущего дня. Источник: {s.source or 'MOEX'}.</i>")
    return "\n".join(out)


def rs_label(d: Optional[float]) -> str:
    if d is None:
        return "—"
    if d >= 0.3:
        return "сильнее рынка"
    if d <= -0.3:
        return "слабее рынка"
    return "с рынком"


def rs_text(s: Snapshot, rows: list[tuple[str, Optional[float], Optional[float]]]) -> str:
    """Таблица силы к рынку по бумагам списка: (тикер, изм. 15м %, изм. день %)."""
    if not rows:
        return ""
    im15 = imoex_change(15)
    imd = s.imoex_pct
    out = ["", "<b>Сила к рынку</b> (бумага − IMOEX)", "<pre>",
           f"{'бумага':<6} {'15м':>7} {'день':>7}  {'итог':<14}"]
    for t, c15, cd in rows:
        d15 = (c15 - im15) if (c15 is not None and im15 is not None) else None
        dd = (cd - imd) if (cd is not None and imd is not None) else None
        f15 = f"{d15:+.2f}" if d15 is not None else "—"
        fd = f"{dd:+.2f}" if dd is not None else "—"
        key = dd if dd is not None else d15
        out.append(f"{t:<6} {f15:>7} {fd:>7}  {rs_label(key):<14}")
    out.append("</pre>")
    if im15 is None:
        out.append("<i>Колонка 15м появится через 15 мин после старта — копится история IMOEX.</i>")
    return "\n".join(out).replace(".", ",")
