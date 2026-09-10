"""Свечные графики (PNG) по данным ISS Мосбиржи.

ISS отдаёт свечи только с интервалами 1, 10, 60 мин и 1 день (24), причём
не более 500 рядов за запрос. Поэтому 5м/15м собираются из минутных свечей,
30м — из 10-минутных, 4ч — из часовых; глубина истории качается
параллельными окнами, каждое из которых заведомо < 500 рядов.
"""
from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import aiohttp
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import gridspec  # noqa: E402

import moex  # noqa: E402
from formatting import cur_symbol, fmt_pct, fmt_price  # noqa: E402
from moex import SecurityInfo, now_msk  # noqa: E402

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Period:
    key: str
    title: str          # подпись кнопки
    base: int           # интервал ISS: 1, 10, 60 (мин) или 24 (день)
    agg: int            # сколько базовых свечей в одной итоговой
    bars: int           # сколько свечей показывать
    depth: timedelta    # насколько назад качать историю (календарно, с запасом)
    fmt: str            # формат подписи оси X


PERIODS: dict[str, Period] = {p.key: p for p in (
    Period("1m", "1м", 1, 1, 120, timedelta(days=4), "%H:%M"),
    Period("5m", "5м", 1, 5, 100, timedelta(days=5), "%H:%M"),
    Period("15m", "15м", 1, 15, 96, timedelta(days=7), "%d.%m %H:%M"),
    Period("30m", "30м", 10, 3, 96, timedelta(days=10), "%d.%m %H:%M"),
    Period("1h", "1ч", 60, 1, 96, timedelta(days=14), "%d.%m %H:%M"),
    Period("4h", "4ч", 60, 4, 96, timedelta(days=45), "%d.%m"),
    Period("1d", "1д", 24, 1, 90, timedelta(days=150), "%d.%m"),
)}
PERIOD_ORDER = tuple(PERIODS)
PERIOD_TITLES = {k: p.title for k, p in PERIODS.items()}
DEFAULT_PERIOD = "5m"

# размер окна одного запроса: base_interval * ~480 рядов (лимит 500)
_WINDOW = {1: timedelta(minutes=480), 10: timedelta(minutes=4800),
           60: timedelta(hours=480), 24: timedelta(days=480)}

UP, DOWN, GRID, TXT, MUTED = "#1f9d55", "#d64545", "#e5e7eb", "#111827", "#6b7280"


@dataclass
class Candle:
    begin: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


# --------------------------------------------------------------- загрузка

async def _window(sess: aiohttp.ClientSession, info: SecurityInfo,
                  base: int, frm: datetime) -> list[list[Any]]:
    data = await moex._get(
        sess,
        f"/engines/stock/markets/shares/boards/{info.board}/securities/"
        f"{info.ticker}/candles.json",
        {"interval": base, "from": frm.strftime("%Y-%m-%d %H:%M:%S"),
         "iss.meta": "off"},
    )
    _, rows = moex._rows(data)
    return rows


async def fetch_candles(sess: aiohttp.ClientSession, info: SecurityInfo,
                        base: int, depth: timedelta) -> list[Candle]:
    """Базовые свечи за `depth` назад, параллельными окнами, без дублей."""
    now = now_msk()
    step = _WINDOW[base]
    starts = []
    t = now - depth
    while t < now:
        starts.append(t)
        t += step
    results = await asyncio.gather(
        *(_window(sess, info, base, s) for s in starts), return_exceptions=True)

    seen: dict[str, Candle] = {}
    for res in results:
        if isinstance(res, BaseException):
            log.warning("MOEX: окно свечей %s: %s", info.ticker, res)
            continue
        for r in res:
            # [open, close, high, low, value, volume, begin, end]
            if r[0] is None or r[1] is None:
                continue
            seen[r[6]] = Candle(
                begin=datetime.strptime(r[6], "%Y-%m-%d %H:%M:%S"),
                open=float(r[0]), high=float(r[2]), low=float(r[3]),
                close=float(r[1]), volume=float(r[5] or 0),
            )
    return [seen[k] for k in sorted(seen)]


def aggregate(cs: list[Candle], base: int, agg: int) -> list[Candle]:
    """Склеить базовые свечи в более крупные, выравнивая по границам суток."""
    if agg == 1:
        return cs
    minutes = base * agg  # 5, 15, 30, 240
    out: list[Candle] = []
    cur_key = None
    for c in cs:
        mins = c.begin.hour * 60 + c.begin.minute
        key = (c.begin.date(), mins // minutes)
        if key != cur_key:
            cur_key = key
            start = c.begin.replace(hour=(mins // minutes * minutes) // 60,
                                    minute=(mins // minutes * minutes) % 60,
                                    second=0)
            out.append(Candle(start, c.open, c.high, c.low, c.close, c.volume))
        else:
            o = out[-1]
            o.high = max(o.high, c.high)
            o.low = min(o.low, c.low)
            o.close = c.close
            o.volume += c.volume
    return out


# --------------------------------------------------------------- рисование

def render(info: SecurityInfo, p: Period, cs: list[Candle]) -> bytes:
    cur = cur_symbol(info.currency)
    n = len(cs)
    xs = range(n)
    last = cs[-1].close
    first = cs[0].open
    chg = (last - first) / first * 100 if first else 0.0
    colors = [UP if c.close >= c.open else DOWN for c in cs]

    fig = plt.figure(figsize=(9, 5.2), dpi=150, facecolor="white")
    gs = gridspec.GridSpec(2, 1, height_ratios=[4, 1], hspace=0.05)
    ax = fig.add_subplot(gs[0])
    axv = fig.add_subplot(gs[1], sharex=ax)

    # тени и тела свечей
    ax.vlines(xs, [c.low for c in cs], [c.high for c in cs],
              colors=colors, linewidth=0.9)
    bodies_h = [abs(c.close - c.open) or (c.high - c.low) * 0.02 or 1e-9 for c in cs]
    bodies_b = [min(c.open, c.close) for c in cs]
    ax.bar(xs, bodies_h, bottom=bodies_b, width=0.7, color=colors,
           edgecolor=colors, linewidth=0.6)

    # линия предыдущего закрытия (актуальна для внутридневных периодов)
    if p.base != 24 and info.prev_close > 0:
        ax.axhline(info.prev_close, color=MUTED, linewidth=0.9, linestyle="--")
        ax.annotate(f"пред. закр. {fmt_price(info.prev_close, info.decimals)}",
                    xy=(0, info.prev_close), xytext=(3, 3),
                    textcoords="offset points", fontsize=7.5, color=MUTED)
    # текущая цена
    ax.axhline(last, color=colors[-1], linewidth=0.7, alpha=0.6)
    ax.annotate(fmt_price(last, info.decimals), xy=(n - 1, last),
                xytext=(6, -3), textcoords="offset points", fontsize=8,
                color="white", fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.25", fc=colors[-1], ec="none"))

    ax.set_title(
        f"{info.ticker} · {info.name} · {p.title}    "
        f"{fmt_price(last, info.decimals)} {cur}    {fmt_pct(chg)} за период",
        loc="left", fontsize=11, fontweight="bold", color=TXT)

    # объём
    axv.bar(xs, [c.volume for c in cs], width=0.7, color=colors, alpha=0.55)
    axv.set_ylabel("Объём", fontsize=7.5, color=MUTED)
    axv.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6 else
        f"{v/1e3:.0f}K" if v >= 1e3 else f"{v:.0f}"))

    # оси
    ticks = list(range(0, n, max(1, n // 8)))
    labels = [cs[i].begin.strftime(p.fmt) for i in ticks]
    axv.set_xticks(ticks)
    axv.set_xticklabels(labels, fontsize=7.5, color="#374151")
    plt.setp(ax.get_xticklabels(), visible=False)
    ax.set_xlim(-1, n + max(3, n * 0.06))
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: fmt_price(v, info.decimals)))
    for a in (ax, axv):
        a.grid(True, color=GRID, linewidth=0.8)
        a.tick_params(labelsize=8, colors="#374151", length=0)
        for s in ("top", "right"):
            a.spines[s].set_visible(False)
        a.set_facecolor("white")
    fig.text(0.99, 0.01, f"MOEX ISS · {now_msk():%d.%m %H:%M} МСК",
             ha="right", fontsize=7, color="#9ca3af")
    fig.subplots_adjust(left=0.09, right=0.97, top=0.92, bottom=0.09)

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


async def build_chart(sess: aiohttp.ClientSession, info: SecurityInfo,
                      period: str) -> Optional[bytes]:
    """PNG свечного графика или None, если данных нет."""
    p = PERIODS.get(period) or PERIODS[DEFAULT_PERIOD]
    base = await fetch_candles(sess, info, p.base, p.depth)
    cs = aggregate(base, p.base, p.agg)[-p.bars:]
    if len(cs) < 2:
        return None
    return render(info, p, cs)
