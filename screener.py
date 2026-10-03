"""Скринер акций MOEX: изменение цены за 15м / 30м / 1ч / 4ч / 1д по ликвидным
бумагам в реальном времени.

Источники:
  * T-Invest `GetLastPrices` — одним запросом последние цены всех бумаг
    вселенной (реальное время). Опрашивается каждые POLL_SEC секунд, история
    цен копится в памяти (5 часов), изменения считаются локально.
  * T-Invest `GetCandles` (5 мин) — разовый прогрев истории при старте, чтобы
    окна 15м…4ч работали сразу, а не через 4 часа.
  * MOEX ISS `securities.json` — справочник: имя, оборот за день (отсев
    неликвида), официальное закрытие предыдущего дня (для окна 1д).

Вселенная = акции TQBR (список из T-Invest `Shares`, ETF/фонды не попадают)
с дневным оборотом >= MIN_TURNOVER ₽. Обновляется раз в час; ночью и утром,
когда оборот ещё не набран, используется последний «полный» оборот.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from config import MOEX_BASE
from tinkoff import Instrument, TinkoffClient, q2f

log = logging.getLogger(__name__)

POLL_SEC = int(os.getenv("SCREENER_POLL_SEC", "30"))
MIN_TURNOVER = float(os.getenv("SCREENER_MIN_TURNOVER", "50000000"))  # ₽/день
HISTORY_H = 5.0                     # глубина истории цен в памяти, часов
UNIVERSE_TTL = 3600                 # обновление вселенной, сек
TOP_N = 10

WINDOWS: dict[str, int] = {"15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
WINDOW_TITLES = {"15m": "15 мин", "30m": "30 мин", "1h": "1 час", "4h": "4 часа", "1d": "1 день"}
WINDOW_SHORT = {"15m": "15м", "30m": "30м", "1h": "1ч", "4h": "4ч", "1d": "1д"}
WINDOW_ORDER = ["15m", "30m", "1h", "4h", "1d"]
DEFAULT_WINDOW = "15m"

_MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(_MSK)


def trading_time(t: Optional[datetime] = None) -> bool:
    """06:50–23:50 МСК (утренняя + основная + вечерняя сессии)."""
    t = t or now_msk()
    hm = t.hour * 60 + t.minute
    return 6 * 60 + 50 <= hm <= 23 * 60 + 50


@dataclass
class Sec:
    ticker: str
    name: str
    uid: str
    prev_close: float          # официальное закрытие предыдущего дня
    turnover: float            # оборот, ₽ (today или последний полный)
    decimals: int = 2
    hist: deque = field(default_factory=lambda: deque(maxlen=int(HISTORY_H * 3600 / POLL_SEC) + 400))
    last: float = 0.0
    last_ts: float = 0.0

    def price_at(self, ago_sec: float) -> Optional[float]:
        """Цена «ago_sec назад»: последний сэмпл не позже целевого момента.
        Если история не дотягивается (после рестарта / ночной разрыв) — None."""
        target = time.time() - ago_sec
        best = None
        for ts, p in self.hist:
            if ts <= target:
                best = p
            else:
                break
        if best is None:
            return None
        return best

    def change(self, win: str) -> Optional[float]:
        if not self.last:
            return None
        if win == "1d":
            if self.prev_close <= 0:
                return None
            return (self.last / self.prev_close - 1) * 100
        base = self.price_at(WINDOWS[win] * 60)
        if not base:
            return None
        return (self.last / base - 1) * 100


@dataclass
class Row:
    ticker: str
    name: str
    pct: float
    price: float
    decimals: int
    turnover: float


class Screener:
    def __init__(self, tk: TinkoffClient, data_dir: str) -> None:
        self.tk = tk
        self.secs: dict[str, Sec] = {}
        self._universe_ts = 0.0
        self._turn_file = os.path.join(data_dir, "screener_turnover.json")
        self._full_turnover: dict[str, float] = self._load_turnover()
        self.updated: Optional[datetime] = None
        self.error: str = ""
        self._warm = False

    # ------------------------------------------------------------ persist
    def _load_turnover(self) -> dict[str, float]:
        try:
            with open(self._turn_file, encoding="utf-8") as f:
                return {k: float(v) for k, v in json.load(f).items()}
        except Exception:
            return {}

    def _save_turnover(self) -> None:
        try:
            os.makedirs(os.path.dirname(self._turn_file) or ".", exist_ok=True)
            with open(self._turn_file, "w", encoding="utf-8") as f:
                json.dump(self._full_turnover, f)
        except Exception as e:
            log.debug("screener: save turnover: %s", e)

    # ------------------------------------------------------------ universe
    async def _moex_board(self, session: aiohttp.ClientSession) -> dict[str, dict]:
        url = (MOEX_BASE + "/engines/stock/markets/shares/boards/TQBR/securities.json"
               "?iss.meta=off&securities.columns=SECID,SHORTNAME,PREVLEGALCLOSEPRICE,DECIMALS,STATUS"
               "&marketdata.columns=SECID,VALTODAY")
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            r.raise_for_status()
            d = await r.json()
        out: dict[str, dict] = {}
        for sec, name, prev, dec, status in d["securities"]["data"]:
            if status != "A":
                continue
            out[sec] = {"name": name, "prev": float(prev or 0), "dec": int(dec or 2), "val": 0.0}
        for sec, val in d["marketdata"]["data"]:
            if sec in out:
                out[sec]["val"] = float(val or 0)
        return out

    async def _tinvest_shares(self) -> dict[str, Instrument]:
        d = await self.tk._call("InstrumentsService/Shares",
                                {"instrumentStatus": "INSTRUMENT_STATUS_BASE"})
        out: dict[str, Instrument] = {}
        for i in d.get("instruments", []):
            if i.get("classCode") != "TQBR" or not i.get("uid"):
                continue
            if i.get("apiTradeAvailableFlag") is False:
                continue
            t = i["ticker"]
            out[t] = Instrument(t, i["uid"], i.get("figi", ""), int(i.get("lot") or 1), i.get("name", t))
        return out

    async def refresh_universe(self, session: aiohttp.ClientSession) -> None:
        board, shares = await asyncio.gather(self._moex_board(session), self._tinvest_shares())
        # «полный» оборот: после 18:40 МСК запоминаем дневной, им пользуемся утром
        t = now_msk()
        end_of_day = t.hour * 60 + t.minute >= 18 * 60 + 40
        new: dict[str, Sec] = {}
        for tick, inst in shares.items():
            b = board.get(tick)
            if not b or b["prev"] <= 0:
                continue
            val_today = b["val"]
            full = self._full_turnover.get(tick, 0.0)
            if end_of_day and val_today > 0:
                self._full_turnover[tick] = val_today
                full = val_today
            turnover = max(val_today, full)
            if turnover < MIN_TURNOVER:
                continue
            old = self.secs.get(tick)
            if old:
                old.name, old.prev_close, old.turnover, old.decimals = b["name"], b["prev"], turnover, b["dec"]
                new[tick] = old
            else:
                new[tick] = Sec(tick, b["name"], inst.uid, b["prev"], turnover, b["dec"])
        if len(new) < 20:
            log.warning("screener: вселенная подозрительно мала (%d) — оставляю прежнюю", len(new))
            if self.secs:
                return
        added = set(new) - set(self.secs)
        self.secs = new
        self._universe_ts = time.time()
        if end_of_day:
            self._save_turnover()
        log.info("screener: вселенная %d бумаг (оборот ≥ %.0f млн ₽), новых %d",
                 len(new), MIN_TURNOVER / 1e6, len(added))
        if added:
            await self._warm_up([self.secs[t] for t in added])

    # ------------------------------------------------------------ история
    async def _warm_up(self, secs: list[Sec]) -> None:
        """Прогрев истории 5-минутными свечами за HISTORY_H часов."""
        sem = asyncio.Semaphore(8)
        depth = timedelta(hours=HISTORY_H)

        async def one(s: Sec) -> None:
            async with sem:
                try:
                    inst = Instrument(s.ticker, s.uid, "", 1, s.name)
                    candles = await self.tk.candles(inst, 5, depth)
                except Exception as e:
                    log.debug("screener: прогрев %s: %s", s.ticker, e)
                    return
            pts = []
            for c in candles:
                # закрытие свечи относим к её концу
                ts = (c.begin.replace(tzinfo=_MSK) + timedelta(minutes=5)).timestamp()
                pts.append((min(ts, time.time()), c.close))
            existing = list(s.hist)
            s.hist.clear()
            for p in sorted(pts + existing):
                s.hist.append(p)
        await asyncio.gather(*(one(s) for s in secs))
        self._warm = True

    async def poll_prices(self) -> None:
        secs = list(self.secs.values())
        if not secs:
            return
        now = time.time()
        for i in range(0, len(secs), 100):
            chunk = secs[i:i + 100]
            d = await self.tk._call("MarketDataService/GetLastPrices",
                                    {"instrumentId": [s.uid for s in chunk]})
            by_uid = {s.uid: s for s in chunk}
            for p in d.get("lastPrices", []):
                s = by_uid.get(p.get("instrumentUid"))
                if not s or not p.get("price"):
                    continue
                price = q2f(p["price"])
                if price <= 0:
                    continue
                s.last, s.last_ts = price, now
                s.hist.append((now, price))
        self.updated = now_msk()

    # ------------------------------------------------------------ цикл
    async def run(self, session: aiohttp.ClientSession) -> None:
        await asyncio.sleep(3)
        while True:
            try:
                if time.time() - self._universe_ts > UNIVERSE_TTL:
                    await self.refresh_universe(session)
                if trading_time():
                    await self.poll_prices()
                self.error = ""
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.error = str(e)[:200]
                log.warning("screener: %s", e)
            await asyncio.sleep(POLL_SEC)

    # ------------------------------------------------------------ выдача
    def rows(self, win: str) -> tuple[list[Row], list[Row], int]:
        """(рост топ-N, падение топ-N, сколько бумаг имело данные по окну)."""
        rows: list[Row] = []
        for s in self.secs.values():
            pct = s.change(win)
            if pct is None:
                continue
            rows.append(Row(s.ticker, s.name, pct, s.last, s.decimals, s.turnover))
        ups = sorted((r for r in rows if r.pct > 0), key=lambda r: -r.pct)[:TOP_N]
        downs = sorted((r for r in rows if r.pct < 0), key=lambda r: r.pct)[:TOP_N]
        return ups, downs, len(rows)

    def status_text(self) -> str:
        if not self.secs:
            return "вселенная ещё не загружена"
        upd = self.updated.strftime("%H:%M:%S") if self.updated else "—"
        s = f"{len(self.secs)} бумаг, оборот ≥ {MIN_TURNOVER / 1e6:.0f} млн ₽, обновлено {upd} МСК"
        if self.error:
            s += f"\n⚠️ {self.error}"
        return s


# ---------------------------------------------------------------- формат

def _fmt_price(p: float, dec: int) -> str:
    dec = max(0, min(dec, 4))
    s = f"{p:,.{dec}f}".replace(",", " ")
    return s


def _fmt_turn(v: float) -> str:
    if v >= 1e9:
        return f"{v / 1e9:.1f} млрд"
    return f"{v / 1e6:.0f} млн"


def format_screener(scr: Screener, win: str, esc) -> str:
    ups, downs, n = scr.rows(win)
    title = WINDOW_TITLES.get(win, win)
    head = f"🔎 <b>Скринер MOEX — изменение за {esc(title)}</b>\n"
    if not scr.secs:
        return head + "\n⏳ Загружаю список бумаг… попробуй через минуту."
    if n == 0:
        why = ("рынок закрыт или история ещё не набрана" if win != "1d"
               else "нет данных о ценах")
        return head + f"\n😴 Нет данных по окну {esc(title)}: {esc(why)}.\n\n<i>{esc(scr.status_text())}</i>"

    def block(icon: str, name: str, rows: list[Row]) -> str:
        if not rows:
            return f"{icon} <b>{name}</b>: нет\n"
        lines = [f"{icon} <b>{name}</b>"]
        for i, r in enumerate(rows, 1):
            lines.append(f"{i:>2}. <code>{esc(r.ticker):<6}</code> "
                         f"<b>{r.pct:+.2f}%</b>  {esc(_fmt_price(r.price, r.decimals))} ₽"
                         f"  <i>{esc(_fmt_turn(r.turnover))}</i>")
        return "\n".join(lines) + "\n"

    body = block("🚀", "Рост", ups) + "\n" + block("🔻", "Падение", downs)
    if not trading_time():
        body += "\n<i>Сейчас не торговое время — цены на момент закрытия.</i>"
    foot = f"\n<i>{esc(scr.status_text())}; в расчёте {n} бумаг</i>"
    return head + "\n" + body + foot


# ---------------------------------------------------------------- картинка

def render_png(scr: "Screener", win: str) -> bytes:
    """Таблица в тёмной теме (стиль брокерского приложения): два блока —
    Рост и Падение. Колонки: Инструмент | Цена | Объём, день | Изм."""
    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch

    BG, PANEL, ROW_ALT = "#0f1621", "#161f2c", "#1a2432"
    TXT, MUTED, HEAD = "#e6edf3", "#8b98a9", "#c9d3df"
    UP, DOWN, LINE = "#22c55e", "#ef4444", "#243040"

    ups, downs, n = scr.rows(win)
    title = WINDOW_TITLES.get(win, win)
    blocks = [("Рост", ups, UP), ("Падение", downs, DOWN)]

    row_h = 0.42
    n_rows = sum(max(len(r), 1) for _, r, _ in blocks)
    height = 1.05 + n_rows * row_h + len(blocks) * 1.1 + 0.6
    fig = plt.figure(figsize=(6.4, height), dpi=180, facecolor=BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 6.4); ax.set_ylim(0, height); ax.axis("off")

    # колонки (x-координаты)
    x_tick, x_price, x_vol, x_chg = 0.35, 3.05, 4.55, 6.15
    y = height - 0.45
    ax.text(0.3, y, f"Скринер MOEX · изменение за {title}", color=TXT, fontsize=12.5,
            fontweight="bold", va="center")
    upd = scr.updated.strftime("%H:%M") if scr.updated else "—"
    ax.text(6.1, y, f"{upd} МСК", color=MUTED, fontsize=8.5, va="center", ha="right")
    y -= 0.6

    def header(y: float) -> None:
        ax.text(x_tick, y, "Инструмент", color=MUTED, fontsize=8.5, va="center")
        ax.text(x_price, y, "Цена", color=MUTED, fontsize=8.5, va="center", ha="right")
        ax.text(x_vol, y, "Объём, день", color=MUTED, fontsize=8.5, va="center", ha="right")
        ax.text(x_chg, y, f"Изм. {WINDOW_SHORT[win]}", color=MUTED, fontsize=8.5, va="center", ha="right")
        ax.plot([0.25, 6.15], [y - 0.2, y - 0.2], color=LINE, lw=0.8)

    for name, rows, col in blocks:
        # заголовок блока — «чип»
        ax.add_patch(FancyBboxPatch((0.25, y - 0.17), 1.25, 0.34, boxstyle="round,pad=0.02,rounding_size=0.08",
                                    fc=PANEL, ec=col, lw=1.0))
        ax.text(0.875, y, name, color=col, fontsize=9.5, fontweight="bold", ha="center", va="center")
        y -= 0.42
        header(y)
        y -= 0.38
        if not rows:
            ax.text(x_tick, y, "нет", color=MUTED, fontsize=9, va="center")
            y -= row_h
        for i, r in enumerate(rows):
            if i % 2 == 0:
                ax.add_patch(FancyBboxPatch((0.25, y - row_h / 2 + 0.02), 5.9, row_h - 0.04,
                                            boxstyle="round,pad=0,rounding_size=0.05", fc=ROW_ALT, ec="none"))
            # «иконка» — кружок с первой буквой
            ax.add_patch(plt.Circle((x_tick + 0.12, y), 0.13, fc=col, ec="none", alpha=0.85))
            ax.text(x_tick + 0.12, y, r.ticker[0], color=BG, fontsize=7.5, fontweight="bold", ha="center", va="center")
            ax.text(x_tick + 0.36, y + 0.08, r.ticker, color=TXT, fontsize=9.5, fontweight="bold", va="center")
            ax.text(x_tick + 0.36, y - 0.11, r.name[:22], color=MUTED, fontsize=6.5, va="center")
            ax.text(x_price, y, _fmt_price(r.price, r.decimals) + " ₽", color=TXT, fontsize=9, va="center", ha="right")
            ax.text(x_vol, y, _fmt_turn(r.turnover) + " ₽", color=HEAD, fontsize=8.5, va="center", ha="right")
            ax.text(x_chg, y, f"{r.pct:+.2f}%", color=col, fontsize=9.5, fontweight="bold", va="center", ha="right")
            y -= row_h
        y -= 0.3

    foot = f"{len(scr.secs)} бумаг, оборот ≥ {MIN_TURNOVER / 1e6:.0f} млн ₽ · T-Invest realtime"
    if not trading_time():
        foot += " · не торговое время"
    ax.text(0.3, 0.22, foot, color=MUTED, fontsize=7, va="center")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=BG)
    plt.close(fig)
    return buf.getvalue()
