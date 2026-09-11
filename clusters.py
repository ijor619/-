"""Кластерный анализ ленты: сколько объёма прошло по каким ценам и за какое
время, и в какую сторону был перевес (агрессивные покупки vs продажи).

T-Invest отдаёт историю сделок только за последний час, поэтому бот копит
ленту сам: FlowMonitor каждые ~20 с забирает сделки по бумагам из списков
и складывает их сюда. Хранится агрегат «минута × цена → (лоты покупок,
лоты продаж, число сделок)», на диске в data/clusters.json, глубина 5 дней.
"""
from __future__ import annotations

import io
import json
import logging
import math
import os
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from formatting import esc, fmt_price
from tinkoff import Trade

log = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
KEEP_DAYS = 5
SAVE_EVERY_SEC = 60

# окно -> (подпись, минут, размер временной корзины в минутах)
WINDOWS = {
    "5m": ("5м", 5, 1),
    "15m": ("15м", 15, 1),
    "30m": ("30м", 30, 2),
    "1h": ("1ч", 60, 5),
    "4h": ("4ч", 240, 15),
    "1d": ("1д", 24 * 60, 30),
}
WINDOW_ORDER = ("5m", "15m", "30m", "1h", "4h", "1d")
DEFAULT_WINDOW = "1h"


def fmt_n(n: float) -> str:
    return f"{int(round(n)):,}".replace(",", "\u00a0")


def fmt_money(rub: float) -> str:
    a = abs(rub)
    if a >= 1e9:
        s = f"{rub / 1e9:.2f} млрд ₽"
    elif a >= 1e6:
        s = f"{rub / 1e6:.1f} млн ₽"
    elif a >= 1e3:
        s = f"{rub / 1e3:.0f} тыс ₽"
    else:
        s = f"{rub:.0f} ₽"
    return s


@dataclass
class Cell:
    minute: datetime   # MSK naive, начало минуты
    price: float
    buy: int           # лоты
    sell: int
    n: int             # число сделок


class ClusterStore:
    """Агрегат сделок по минутам и ценам с персистентностью."""

    def __init__(self, path: str) -> None:
        self.path = path
        # ticker -> minute_key -> price_str -> [buy, sell, n]
        self.data: dict[str, dict[str, dict[str, list[int]]]] = {}
        # дедупликация: ticker -> (последний ts, отпечатки сделок на этом ts)
        self._last: dict[str, tuple[datetime, Counter]] = {}
        self._dirty = False
        self._saved_at = 0.0
        self.started_at = datetime.now(MSK).replace(tzinfo=None)
        self._load()

    # ---------------------------------------------------------------- io
    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                self.data = json.load(f)
            log.info("clusters: загружено %d бумаг из %s", len(self.data), self.path)
        except FileNotFoundError:
            pass
        except Exception:
            log.exception("clusters: не удалось прочитать %s", self.path)
        self.prune()

    def save(self, force: bool = False) -> None:
        if not self._dirty:
            return
        if not force and time.time() - self._saved_at < SAVE_EVERY_SEC:
            return
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self.path)
            self._dirty = False
            self._saved_at = time.time()
        except Exception:
            log.exception("clusters: не удалось сохранить %s", self.path)

    def prune(self) -> None:
        cutoff = (datetime.now(MSK) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%dT%H:%M")
        for t, mins in self.data.items():
            old = [k for k in mins if k < cutoff]
            for k in old:
                del mins[k]
            if old:
                self._dirty = True

    # ------------------------------------------------------------ ingest
    def ingest(self, ticker: str, trades: list[Trade]) -> int:
        """Добавить сделки (UTC ts), пропуская уже учтённые. Возвращает число новых.

        Запросы к T-Invest перекрываются, поэтому помним последнюю секунду
        и сколько раз каждая сделка на ней уже встречалась (одинаковые сделки
        в одну секунду — нормальное явление).
        """
        if not trades:
            return 0
        last_ts, seen = self._last.get(ticker, (None, Counter()))
        mins = self.data.setdefault(ticker, {})
        added = 0
        batch: Counter = Counter()
        for tr in sorted(trades, key=lambda x: x.ts):
            if last_ts is not None and tr.ts < last_ts:
                continue
            fp = (tr.price, tr.qty, tr.side)
            if last_ts is not None and tr.ts == last_ts:
                batch[fp] += 1
                if batch[fp] <= seen[fp]:
                    continue
                seen[fp] += 1
            else:
                last_ts, seen, batch = tr.ts, Counter(), Counter()
                batch[fp] += 1
                seen[fp] += 1
            mk = tr.ts.astimezone(MSK).strftime("%Y-%m-%dT%H:%M")
            pk = repr(tr.price)
            cell = mins.setdefault(mk, {}).setdefault(pk, [0, 0, 0])
            if tr.side == "B":
                cell[0] += tr.qty
            else:
                cell[1] += tr.qty
            cell[2] += 1
            added += 1
        self._last[ticker] = (last_ts, seen)
        if added:
            self._dirty = True
            self.save()
        return added

    # ------------------------------------------------------------- query
    def window(self, ticker: str, minutes: int,
               now: Optional[datetime] = None) -> list[Cell]:
        now = now or datetime.now(MSK).replace(tzinfo=None)
        frm = (now - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M")
        out: list[Cell] = []
        for mk, prices in self.data.get(ticker, {}).items():
            if mk < frm:
                continue
            m = datetime.strptime(mk, "%Y-%m-%dT%H:%M")
            for pk, (b, s, n) in prices.items():
                out.append(Cell(m, float(pk), b, s, n))
        out.sort(key=lambda c: (c.minute, c.price))
        return out

    def coverage(self, ticker: str) -> Optional[datetime]:
        """С какого момента есть данные по бумаге (MSK naive)."""
        mins = self.data.get(ticker)
        if not mins:
            return None
        return datetime.strptime(min(mins), "%Y-%m-%dT%H:%M")


# ------------------------------------------------------------------ анализ

@dataclass
class Bucket:
    start: datetime
    buy: int
    sell: int
    n: int
    lo: float
    hi: float

    @property
    def vol(self) -> int:
        return self.buy + self.sell

    @property
    def delta(self) -> int:
        return self.buy - self.sell


@dataclass
class LevelRow:
    price: float   # нижняя граница бина
    buy: int
    sell: int
    n: int

    @property
    def vol(self) -> int:
        return self.buy + self.sell

    @property
    def delta(self) -> int:
        return self.buy - self.sell


@dataclass
class Analysis:
    ticker: str
    window: str
    cells: list[Cell]
    lot: int
    decimals: int
    buckets: list[Bucket]
    levels: list[LevelRow]      # снизу вверх
    step: float                 # шаг ценового бина
    heat: dict[tuple[int, int], tuple[int, int]]  # (bucket_idx, level_idx) -> (buy, sell)
    buy: int
    sell: int
    n: int
    turnover: float             # ₽
    poc: float                  # уровень с максимальным объёмом (нижняя граница бина)
    va_lo: float
    va_hi: float
    first: float
    last: float
    since: datetime
    until: datetime

    @property
    def vol(self) -> int:
        return self.buy + self.sell

    @property
    def delta(self) -> int:
        return self.buy - self.sell

    @property
    def buy_pct(self) -> float:
        return self.buy / self.vol * 100 if self.vol else 0.0


def _nice_step(span: float, decimals: int, target_rows: int = 24) -> float:
    tick = 10 ** (-decimals)
    if span <= 0:
        return tick
    raw = span / target_rows
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else tick
    for m in (1, 2, 2.5, 5, 10):
        step = mag * m
        if step >= raw:
            break
    return max(tick, round(step, decimals + 2))


def analyze(ticker: str, window: str, cells: list[Cell], lot: int,
            decimals: int) -> Optional[Analysis]:
    if not cells:
        return None
    _, minutes, bucket_min = WINDOWS[window]
    lo = min(c.price for c in cells)
    hi = max(c.price for c in cells)
    step = _nice_step(hi - lo, decimals)
    base = math.floor(lo / step) * step
    n_lv = int(math.floor((hi - base) / step)) + 1
    levels = [LevelRow(round(base + i * step, decimals + 2), 0, 0, 0) for i in range(n_lv)]

    # временные корзины, выровненные по сетке
    t0 = cells[0].minute
    t0 = t0.replace(minute=t0.minute - t0.minute % bucket_min, second=0)
    t_end = cells[-1].minute
    buckets: list[Bucket] = []
    t = t0
    while t <= t_end:
        buckets.append(Bucket(t, 0, 0, 0, math.inf, -math.inf))
        t += timedelta(minutes=bucket_min)
    heat: dict[tuple[int, int], list[int]] = {}
    buy = sell = n = 0
    turnover = 0.0
    for c in cells:
        li = min(n_lv - 1, int((c.price - base) / step))
        bi = int((c.minute - t0).total_seconds() // (bucket_min * 60))
        bi = min(bi, len(buckets) - 1)
        lv, bk = levels[li], buckets[bi]
        lv.buy += c.buy; lv.sell += c.sell; lv.n += c.n
        bk.buy += c.buy; bk.sell += c.sell; bk.n += c.n
        bk.lo = min(bk.lo, c.price); bk.hi = max(bk.hi, c.price)
        h = heat.setdefault((bi, li), [0, 0])
        h[0] += c.buy; h[1] += c.sell
        buy += c.buy; sell += c.sell; n += c.n
        turnover += (c.buy + c.sell) * lot * c.price

    # POC и зона стоимости (70% объёма вокруг POC)
    total = buy + sell
    pi = max(range(n_lv), key=lambda i: levels[i].vol)
    acc = levels[pi].vol
    a = b = pi
    while acc < total * 0.7 and (a > 0 or b < n_lv - 1):
        va = levels[a - 1].vol if a > 0 else -1
        vb = levels[b + 1].vol if b < n_lv - 1 else -1
        if vb > va:
            b += 1; acc += vb
        else:
            a -= 1; acc += va
    return Analysis(
        ticker=ticker, window=window, cells=cells, lot=lot, decimals=decimals,
        buckets=buckets, levels=levels, step=step,
        heat={k: (v[0], v[1]) for k, v in heat.items()},
        buy=buy, sell=sell, n=n, turnover=turnover,
        poc=levels[pi].price, va_lo=levels[a].price, va_hi=levels[b].price + step,
        first=cells[0].price, last=cells[-1].price,
        since=cells[0].minute, until=cells[-1].minute,
    )


# -------------------------------------------------------------------- текст

def _bar(pct: float, width: int = 10) -> str:
    k = int(round(pct / 100 * width))
    return "█" * k + "░" * (width - k)


def cluster_text(an: Optional[Analysis], ticker: str, window: str, name: str,
                 coverage: Optional[datetime], started: datetime) -> str:
    title, minutes, bucket_min = WINDOWS[window]
    head = f"🧮 <b>{esc(ticker)}</b> · кластеры за {title}"
    if an is None:
        return (head + f"\n\nСделок за этот период в базе нет. "
                f"Лента копится с {started:%d.%m %H:%M} МСК по бумагам из списка "
                f"(/watch {esc(ticker)}), плюс последний час подгружается при запросе.")
    d = an.decimals
    chg = (an.last - an.first) / an.first * 100 if an.first else 0.0
    sign = "+" if chg >= 0 else ""
    verdict = ("перевес покупателей" if an.buy_pct >= 58 else
               "перевес продавцов" if an.buy_pct <= 42 else "баланс")
    lines = [
        head,
        f"{an.since:%H:%M}–{an.until:%H:%M} МСК · {esc(name)}",
        "",
        f"Объём: <b>{fmt_n(an.vol)}</b> лот ≈ {fmt_money(an.turnover)} · {fmt_n(an.n)} сделок",
        f"Покупки {fmt_n(an.buy)} / продажи {fmt_n(an.sell)} · "
        f"дельта <b>{'+' if an.delta >= 0 else ''}{fmt_n(an.delta)}</b>",
        f"{_bar(an.buy_pct)} {an.buy_pct:.0f}% покупок — <b>{verdict}</b>",
        f"Цена {fmt_price(an.first, d)} → {fmt_price(an.last, d)} ({sign}{chg:.2f}%)",
        f"POC {fmt_price(an.poc, d)}–{fmt_price(an.poc + an.step, d)} · "
        f"зона 70%: {fmt_price(an.va_lo, d)}–{fmt_price(an.va_hi, d)}",
    ]
    # расхождение цена/дельта — самое интересное
    if an.delta > 0 and chg < -0.15 and an.buy_pct >= 55:
        lines.append("⚠️ Покупают по рынку, а цена падает — покупки поглощаются лимитными продавцами.")
    elif an.delta < 0 and chg > 0.15 and an.buy_pct <= 45:
        lines.append("⚠️ Продают по рынку, а цена растёт — продажи поглощаются лимитными покупателями.")

    # крупнейшие кластеры по цене
    top = sorted(an.levels, key=lambda l: l.vol, reverse=True)[:5]
    top = [l for l in top if l.vol]
    if top:
        lines += ["", "<b>Крупнейшие кластеры</b>", "<pre>"]
        lines.append(f"{'цена':>9} {'лоты':>8} {'пок%':>5} дельта")
        for l in sorted(top, key=lambda l: l.price, reverse=True):
            bp = l.buy / l.vol * 100
            mark = "▲" if bp >= 58 else "▼" if bp <= 42 else "·"
            lines.append(f"{fmt_price(l.price, d):>9} {fmt_n(l.vol):>8} {bp:>4.0f}% "
                         f"{'+' if l.delta >= 0 else ''}{fmt_n(l.delta)} {mark}")
        lines.append("</pre>")

    # по времени
    bks = [b for b in an.buckets if b.vol]
    if len(bks) > 1:
        show = bks[-8:]
        lines += [f"<b>По времени</b> (шаг {bucket_min} мин)", "<pre>"]
        for b in show:
            bp = b.buy / b.vol * 100
            lines.append(f"{b.start:%H:%M} {fmt_n(b.vol):>8} {bp:>4.0f}% "
                         f"{'+' if b.delta >= 0 else ''}{fmt_n(b.delta):>7}")
        lines.append("</pre>")
        if len(bks) > len(show):
            lines.append(f"…показаны последние {len(show)} из {len(bks)} корзин, полная картина — на картинке.")
    if coverage and minutes >= 180 and coverage > an.since - timedelta(minutes=minutes) \
            and coverage > started - timedelta(minutes=1):
        lines.append(f"\nℹ️ История копится с {coverage:%d.%m %H:%M} МСК, "
                     f"окно пока неполное.")
    return "\n".join(lines)


# ----------------------------------------------------------------- картинка

def render(an: Analysis, name: str) -> Optional[bytes]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib import gridspec
        from matplotlib.colors import LinearSegmentedColormap
    except Exception:
        return None
    UP, DOWN, TXT, MUTED, GRID = "#1f9d55", "#d64545", "#111827", "#6b7280", "#e5e7eb"
    d = an.decimals
    nb, nl = len(an.buckets), len(an.levels)
    if nb == 0 or nl == 0:
        return None
    # матрицы
    import numpy as np
    vol = np.zeros((nl, nb))
    dl = np.zeros((nl, nb))
    for (bi, li), (b, s) in an.heat.items():
        vol[li, bi] = b + s
        dl[li, bi] = (b - s) / (b + s) if b + s else 0
    alpha = np.sqrt(vol / vol.max()) if vol.max() else vol

    fig = plt.figure(figsize=(10, 5.6), dpi=150, facecolor="white")
    gs = gridspec.GridSpec(2, 2, width_ratios=[4.2, 1], height_ratios=[4, 1.1],
                           wspace=0.03, hspace=0.06)
    ax = fig.add_subplot(gs[0, 0])
    axp = fig.add_subplot(gs[0, 1], sharey=ax)
    axv = fig.add_subplot(gs[1, 0], sharex=ax)

    cmap = LinearSegmentedColormap.from_list("d", [DOWN, "#f3f4f6", UP])
    # тепловая карта: цвет = перевес, насыщенность = объём
    rgba = cmap((dl + 1) / 2)
    rgba[..., 3] = np.clip(alpha, 0, 1) * 0.95
    rgba[vol == 0, 3] = 0
    ax.imshow(rgba, aspect="auto", origin="lower", interpolation="nearest",
              extent=(-0.5, nb - 0.5, -0.5, nl - 0.5))
    # подписи объёмов в крупных ячейках
    thr = np.percentile(vol[vol > 0], 80) if (vol > 0).any() else 0
    for li in range(nl):
        for bi in range(nb):
            v = vol[li, bi]
            if v and v >= thr and nb * nl <= 1200:
                ax.text(bi, li, fmt_n(v), ha="center", va="center", fontsize=5.5,
                        color=TXT if alpha[li, bi] < 0.7 else "white")
    # линия цены по корзинам (средняя между lo/hi)
    mids = [((b.lo + b.hi) / 2 - an.levels[0].price) / an.step - 0.5
            for b in an.buckets if b.vol]
    xs_ = [i for i, b in enumerate(an.buckets) if b.vol]
    ax.plot(xs_, mids, color=TXT, linewidth=0.9, alpha=0.6)
    # POC и зона стоимости
    poc_i = [l.price for l in an.levels].index(an.poc)
    ax.axhline(poc_i, color="#f59e0b", linewidth=1.2, linestyle="--")
    va_a = [l.price for l in an.levels].index(an.va_lo)
    va_b = round((an.va_hi - an.levels[0].price) / an.step) - 1
    ax.axhspan(va_a - 0.5, va_b + 0.5, color="#f59e0b", alpha=0.07)

    yt = list(range(0, nl, max(1, nl // 12)))
    ax.set_yticks(yt)
    ax.set_yticklabels([fmt_price(an.levels[i].price, d) for i in yt], fontsize=7.5)
    ax.set_xlim(-0.5, nb - 0.5)
    ax.set_ylim(-0.5, nl - 0.5)
    plt.setp(ax.get_xticklabels(), visible=False)

    # профиль объёма справа: продажи влево от нуля? — делаем стек: buy зелёный, sell красный
    ys = range(nl)
    axp.barh(ys, [l.buy for l in an.levels], color=UP, alpha=0.85, height=0.9)
    axp.barh(ys, [l.sell for l in an.levels], left=[l.buy for l in an.levels],
             color=DOWN, alpha=0.85, height=0.9)
    axp.axhline(poc_i, color="#f59e0b", linewidth=1.2, linestyle="--")
    plt.setp(axp.get_yticklabels(), visible=False)
    axp.tick_params(axis="y", length=0)
    axp.set_xticks([])
    axp.set_title("профиль", fontsize=7.5, color=MUTED)

    # объём по времени: покупки вверх, продажи вниз + дельта
    xs = range(nb)
    axv.bar(xs, [b.buy for b in an.buckets], color=UP, alpha=0.8, width=0.85)
    axv.bar(xs, [-b.sell for b in an.buckets], color=DOWN, alpha=0.8, width=0.85)
    cum = np.cumsum([b.delta for b in an.buckets])
    ax2 = axv.twinx()
    ax2.plot(list(xs), cum, color="#2563eb", linewidth=1.2)
    ax2.axhline(0, color="#2563eb", linewidth=0.5, alpha=0.4)
    ax2.set_yticks([])
    axv.axhline(0, color=MUTED, linewidth=0.6)
    step_lbl = max(1, nb // 8)
    xt = list(range(0, nb, step_lbl))
    axv.set_xticks(xt)
    fmt = "%H:%M" if an.buckets[-1].start - an.buckets[0].start < timedelta(hours=20) else "%d.%m %H:%M"
    axv.set_xticklabels([an.buckets[i].start.strftime(fmt) for i in xt], fontsize=7.5)
    axv.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: fmt_n(abs(v))))

    for a in (ax, axp, axv):
        a.grid(True, color=GRID, linewidth=0.6, alpha=0.7)
        a.tick_params(labelsize=7.5, colors="#374151", length=0)
        for s in ("top", "right"):
            a.spines[s].set_visible(False)
        a.set_facecolor("white")
    ax2.spines["top"].set_visible(False)

    title = WINDOWS[an.window][0]
    chg = (an.last - an.first) / an.first * 100 if an.first else 0
    fig.suptitle(f"{an.ticker}  ·  кластеры за {title}  ·  {an.since:%d.%m %H:%M}–{an.until:%H:%M} МСК",
                 x=0.01, ha="left", fontsize=11, fontweight="bold", color=TXT)
    fig.text(0.01, 0.925,
             f"{name}  ·  {fmt_n(an.vol)} лот ≈ {fmt_money(an.turnover)}  ·  "
             f"покупки {an.buy_pct:.0f}%  ·  дельта {'+' if an.delta >= 0 else ''}{fmt_n(an.delta)}  ·  "
             f"цена {chg:+.2f}%  ·  POC {fmt_price(an.poc, d)}",
             fontsize=8, color=MUTED)
    fig.text(0.99, 0.01, "зелёный — перевес покупок, красный — продаж; "
             "яркость — объём; синяя линия — накопленная дельта; пунктир — POC",
             ha="right", fontsize=6.5, color="#9ca3af")
    fig.subplots_adjust(left=0.075, right=0.985, top=0.9, bottom=0.08)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()
