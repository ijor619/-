"""Построение графиков цены (PNG) по свечам ISS Мосбиржи."""
from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

import aiohttp
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

import moex  # noqa: E402
from formatting import cur_symbol, fmt_pct, fmt_price  # noqa: E402
from moex import SecurityInfo, now_msk  # noqa: E402

log = logging.getLogger(__name__)

# период -> (interval ISS в минутах/днях, глубина, подпись)
PERIODS: dict[str, tuple[int, timedelta, str]] = {
    "1d": (10, timedelta(days=0), "сегодня"),          # 10-мин свечи с начала дня
    "1w": (60, timedelta(days=7), "неделя"),           # часовые
    "1m": (24, timedelta(days=31), "месяц"),           # дневные
    "3m": (24, timedelta(days=93), "3 месяца"),
}
PERIOD_ORDER = ("1d", "1w", "1m", "3m")
PERIOD_TITLES = {"1d": "1д", "1w": "1н", "1m": "1м", "3m": "3м"}

UP, DOWN, GRID = "#1f9d55", "#d64545", "#e5e7eb"


async def _candles(sess: aiohttp.ClientSession, info: SecurityInfo,
                   interval: int, frm: datetime) -> list[list[Any]]:
    data = await moex._get(
        sess,
        f"/engines/stock/markets/shares/boards/{info.board}/securities/"
        f"{info.ticker}/candles.json",
        {"interval": interval, "from": frm.strftime("%Y-%m-%d %H:%M:%S"),
         "iss.meta": "off"},
    )
    _, rows = moex._rows(data)
    return rows


async def load_series(sess: aiohttp.ClientSession, info: SecurityInfo,
                      period: str) -> list[tuple[datetime, float]]:
    """[(время, close)] за период. Для «сегодня» — последний торговый день."""
    interval, depth, _ = PERIODS[period]
    now = now_msk()
    if period == "1d":
        # берём 10-мин свечи за 3 суток и оставляем последний торговый день
        rows = await _candles(sess, info, interval, now - timedelta(days=3))
        if not rows:
            return []
        last_day = rows[-1][6][:10]
        rows = [r for r in rows if r[6][:10] == last_day]
    else:
        rows = await _candles(sess, info, interval, now - depth)
        # ISS отдаёт максимум 500 рядов — для наших глубин этого хватает,
        # но на всякий случай оставляем хвост.
        rows = rows[-500:]
    return [(datetime.strptime(r[6], "%Y-%m-%d %H:%M:%S"), float(r[1]))
            for r in rows if r[1] is not None]


def render(info: SecurityInfo, period: str,
           series: list[tuple[datetime, float]]) -> bytes:
    xs = [p[0] for p in series]
    ys = [p[1] for p in series]
    cur = cur_symbol(info.currency)

    base = info.prev_close if period == "1d" else ys[0]
    last = ys[-1]
    chg = (last - base) / base * 100 if base else 0.0
    color = UP if last >= base else DOWN

    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    fig.patch.set_facecolor("white")
    ax.plot(xs, ys, color=color, linewidth=1.8)
    ax.fill_between(xs, ys, base, color=color, alpha=0.10)
    ax.axhline(base, color="#6b7280", linewidth=1, linestyle="--")
    ax.annotate(
        ("пред. закр. " if period == "1d" else "") + fmt_price(base, info.decimals),
        xy=(xs[0], base), xytext=(4, 4), textcoords="offset points",
        fontsize=8, color="#6b7280",
    )

    ax.set_title(
        f"{info.ticker} · {info.name}   {fmt_price(last, info.decimals)} {cur}   "
        f"{fmt_pct(chg)} ({PERIODS[period][2]})",
        loc="left", fontsize=11, fontweight="bold", color="#111827",
    )
    ax.grid(True, color=GRID, linewidth=0.8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors="#374151")
    if period == "1d":
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    elif period == "1w":
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.xaxis.set_major_locator(mdates.DayLocator())
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: fmt_price(v, info.decimals)))
    fig.text(0.99, 0.01, f"MOEX ISS · {now_msk():%d.%m %H:%M} МСК",
             ha="right", fontsize=7, color="#9ca3af")
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


async def build_chart(sess: aiohttp.ClientSession, info: SecurityInfo,
                      period: str) -> Optional[bytes]:
    """PNG графика или None, если данных нет."""
    if period not in PERIODS:
        period = "1d"
    series = await load_series(sess, info, period)
    if len(series) < 2:
        return None
    return render(info, period, series)
