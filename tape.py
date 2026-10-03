"""Анализ ленты сделок и стакана: эвристики распознавания алгоритмов.

Сигналы (все — эвристики, не доказательство):
  🧊 iceberg   — серия сделок одного объёма по одной цене с короткими паузами
  🤖 rhythm    — сделки с почти постоянным интервалом (TWAP/VWAP-исполнение)
  ⚖️ imbalance — сильный перекос покупок/продаж при почти неизменной цене
  🔥 burst     — всплеск числа сделок / объёма за минуту
  🧱 wall      — плотность в стакане: уровень в разы больше соседних
  👻 spoof     — крупная заявка появилась и исчезла, не исполнившись
"""
from __future__ import annotations

import logging
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from formatting import esc, fmt_pct, fmt_price
from tinkoff import Level, OrderBook, Trade

log = logging.getLogger(__name__)


def fmt_n(n: float) -> str:
    return f"{int(n):,}".replace(",", "\u00a0")


@dataclass
class Signal:
    kind: str       # iceberg / rhythm / imbalance / burst / wall / spoof
    ticker: str
    text: str       # HTML
    key: str        # для антидубля (kind + детали)


# ------------------------------------------------------------- параметры
ICEBERG_MIN_TRADES = 8       # сделок одного размера по одной цене
ICEBERG_WINDOW_SEC = 180
RHYTHM_MIN_TRADES = 10
RHYTHM_CV_MAX = 0.20         # коэффициент вариации интервалов
IMBALANCE_SHARE = 0.80       # доля объёма одной стороны
IMBALANCE_MIN_TRADES = 40
IMBALANCE_MAX_MOVE = 0.15    # % движения цены при перекосе
BURST_MULT = 4.0             # во сколько раз минута выше медианной
BURST_MIN_TRADES = 30
WALL_MULT = 8.0              # уровень к медиане остальных уровней
WALL_MIN_LEVELS = 8
SPOOF_MULT = 6.0
SPOOF_MAX_LIFE_SEC = 90
# --- кит: одиночная сделка, крупная ОТНОСИТЕЛЬНО ленты этой бумаги -------
# порог = max(p99.9 размеров сделок бумаги за последние дни, рублёвый пол).
WHALE_QUANTILE = 0.999
WHALE_FLOOR_RUB = {           # рублёвый пол по бумагам (откалибровано 11.09.2026)
    "SBER": 50_000_000,
    "GAZP": 5_000_000, "YDEX": 5_000_000, "PLZL": 5_000_000,
    "RUAL": 3_000_000,
}
WHALE_FLOOR_DEFAULT = 5_000_000
WHALE_SERIES_MIN = 3          # серия: N+ крупных сделок одной стороны
WHALE_SERIES_SEC = 120        # за это время
WHALE_SERIES_QUANTILE = 0.99  # сделки серии — от p99 ленты
WALL_EATEN_MIN_SHARE = 0.7    # стена считается съеденной, если исполнено ≥70%


@dataclass
class Baseline:
    """Адаптивные «нормы» бумаги, накапливаются в течение дня (EMA).

    Позволяют сравнивать активность не с фиксированными числами, а с тем,
    что типично для ЭТОЙ бумаги: SBER и RUAL живут по разным правилам.
    """
    trades_per_min: float = 0.0   # средняя частота сделок
    avg_qty: float = 0.0          # средний размер сделки, лот.
    avg_rub: float = 0.0          # средний размер сделки, ₽
    n: int = 0                    # сколько минут учтено
    # выборка размеров сделок (₽) для квантилей кита; хвост ленты ~2 дня
    sizes: list = field(default_factory=list)
    _sizes_last: Optional[datetime] = None

    def add_sizes(self, trades: list["Trade"], lot: int) -> None:
        new = [t for t in trades if self._sizes_last is None or t.ts > self._sizes_last]
        if not new:
            return
        self._sizes_last = new[-1].ts
        self.sizes.extend(t.qty * t.price * lot for t in new)
        if len(self.sizes) > 150_000:
            del self.sizes[: len(self.sizes) - 150_000]

    def quantile(self, q: float) -> Optional[float]:
        """Квантиль размера сделки; None, пока выборка мала (< 2000 сделок)."""
        if len(self.sizes) < 2000:
            return None
        srt = sorted(self.sizes)
        return srt[min(len(srt) - 1, int(len(srt) * q))]

    def median_rub(self) -> float:
        return statistics.median(self.sizes) if self.sizes else self.avg_rub

    def update(self, trades: list["Trade"], lot: int) -> None:
        if not trades:
            return
        self.add_sizes(trades, lot)
        span = max(1.0, (trades[-1].ts - trades[0].ts).total_seconds() / 60)
        tpm = len(trades) / span
        aq = statistics.fmean(t.qty for t in trades)
        ar = statistics.fmean(t.qty * t.price * lot for t in trades)
        if self.n == 0:
            self.trades_per_min, self.avg_qty, self.avg_rub = tpm, aq, ar
        else:
            a = 0.1
            self.trades_per_min += a * (tpm - self.trades_per_min)
            self.avg_qty += a * (aq - self.avg_qty)
            self.avg_rub += a * (ar - self.avg_rub)
        self.n += 1

    @property
    def ready(self) -> bool:
        return self.n >= 3


# =============================================================== лента

def analyze_trades(ticker: str, trades: list[Trade], lot: int,
                   decimals: int, base: Optional[Baseline] = None) -> list[Signal]:
    if len(trades) < 10:
        return []
    out: list[Signal] = []
    now = trades[-1].ts

    # адаптивные пороги: если по бумаге уже есть статистика — масштабируем
    iceberg_min = ICEBERG_MIN_TRADES
    burst_min = BURST_MIN_TRADES
    if base is not None and base.ready:
        # для вялых бумаг 8 одинаковых сделок — событие; для SBER — фон
        iceberg_min = max(ICEBERG_MIN_TRADES, int(base.trades_per_min * 0.5))
        burst_min = max(BURST_MIN_TRADES, int(base.trades_per_min * BURST_MULT))

    # --- кит: одиночная сделка, крупная относительно ленты бумаги -----------
    out += whale_signals(ticker, trades, lot, decimals, base)
    recent = [t for t in trades if now - t.ts <= timedelta(seconds=ICEBERG_WINDOW_SEC)]

    # --- айсберг: одинаковые (цена, объём, сторона) ------------------------
    groups: dict[tuple, list[Trade]] = defaultdict(list)
    for t in recent:
        if t.qty > 0:
            groups[(t.price, t.qty, t.side)].append(t)
    for (price, qty, side), g in groups.items():
        if len(g) >= iceberg_min and qty * len(g) >= 20:
            span = (g[-1].ts - g[0].ts).total_seconds()
            out.append(Signal(
                "iceberg", ticker,
                f"🧊 <b>{ticker}</b> — похоже на айсберг\n"
                f"{len(g)} сделок × {qty} лот. по {fmt_price(price, decimals)} "
                f"за {int(span)} c, {'покупка' if side == 'B' else 'продажа'}\n"
                f"Итого {fmt_n(qty * len(g) * lot)} шт.",
                f"iceberg:{price}:{qty}:{side}"))

    # --- ритм: постоянный интервал между сделками одной стороны -----------
    for side in ("B", "S"):
        ss = [t for t in recent if t.side == side]
        if len(ss) >= RHYTHM_MIN_TRADES:
            gaps = [(b.ts - a.ts).total_seconds() for a, b in zip(ss, ss[1:])]
            gaps = [g for g in gaps if g > 0.0]
            if len(gaps) >= RHYTHM_MIN_TRADES - 1:
                mean = statistics.fmean(gaps)
                cv = statistics.pstdev(gaps) / mean if mean else 9
                if cv <= RHYTHM_CV_MAX and 1.0 <= mean <= 60:
                    qtys = Counter(t.qty for t in ss).most_common(1)[0]
                    out.append(Signal(
                        "rhythm", ticker,
                        f"🤖 <b>{ticker}</b> — ритмичное исполнение "
                        f"({'покупки' if side == 'B' else 'продажи'})\n"
                        f"{len(ss)} сделок с шагом ≈{mean:.1f} c "
                        f"(разброс {cv * 100:.0f}%), типичный объём {qtys[0]} лот.\n"
                        f"Характерно для TWAP/VWAP-робота",
                        f"rhythm:{side}:{round(mean)}"))

    # --- дисбаланс -----------------------------------------------------
    win = [t for t in trades if now - t.ts <= timedelta(minutes=5)]
    if len(win) >= IMBALANCE_MIN_TRADES:
        vb = sum(t.qty for t in win if t.side == "B")
        vs = sum(t.qty for t in win if t.side == "S")
        tot = vb + vs
        move = (win[-1].price - win[0].price) / win[0].price * 100 if win[0].price else 0
        if tot and abs(move) <= IMBALANCE_MAX_MOVE:
            share = max(vb, vs) / tot
            if share >= IMBALANCE_SHARE:
                side = "покупки" if vb > vs else "продажи"
                out.append(Signal(
                    "imbalance", ticker,
                    f"⚖️ <b>{ticker}</b> — перекос ленты: {share * 100:.0f}% объёма "
                    f"за 5 мин — {side}\n"
                    f"Цена при этом {fmt_pct(move)} → кто-то "
                    f"{'набирает' if vb > vs else 'раздаёт'} позицию, не двигая рынок\n"
                    f"Объём: {fmt_n(tot * lot)} шт. в {len(win)} сделках",
                    f"imbalance:{side}"))

    # --- всплеск -----------------------------------------------------
    per_min: dict[datetime, int] = defaultdict(int)
    for t in trades:
        per_min[t.ts.replace(second=0, microsecond=0)] += 1
    if len(per_min) >= 4:
        mins = sorted(per_min)
        last_m, prev = mins[-1], [per_min[m] for m in mins[:-1]]
        med = statistics.median(prev) or 1
        if per_min[last_m] >= burst_min and per_min[last_m] >= BURST_MULT * med:
            tm = [t for t in trades if t.ts.replace(second=0, microsecond=0) == last_m]
            vb = sum(t.qty for t in tm if t.side == "B")
            vs = sum(t.qty for t in tm if t.side == "S")
            out.append(Signal(
                "burst", ticker,
                f"🔥 <b>{ticker}</b> — всплеск активности\n"
                f"{per_min[last_m]} сделок за минуту (обычно ≈{med:.0f}), "
                f"покупки/продажи {vb}/{vs} лот.\n"
                f"Цена {fmt_price(tm[0].price, decimals)} → {fmt_price(tm[-1].price, decimals)}",
                f"burst:{last_m.isoformat()}"))
    return out


# =============================================================== стакан

@dataclass
class BookState:
    """Память по стакану одного инструмента (спуфинг, съедание стен)."""
    big: dict[tuple[str, float], tuple[float, int]] = field(default_factory=dict)
    # (side, price) -> (first_seen_ts, qty)
    last: Optional[OrderBook] = None


def _wall(levels: list[Level], side: str, ticker: str, decimals: int,
          lot: int) -> Optional[Signal]:
    if len(levels) < WALL_MIN_LEVELS:
        return None
    qs = [l.qty for l in levels]
    top = max(levels, key=lambda l: l.qty)
    others = [q for q in qs if q != top.qty] or [1]
    med = statistics.median(others) or 1
    if top.qty >= WALL_MULT * med and top.qty * lot >= 1000:
        idx = levels.index(top)
        return Signal(
            "wall", ticker,
            f"🧱 <b>{ticker}</b> — плотность в стакане "
            f"({'бид, поддержка' if side == 'B' else 'аск, сопротивление'})\n"
            f"{fmt_price(top.price, decimals)}: {fmt_n(top.qty * lot)} шт. "
            f"(≈{top.qty / med:.0f}× медианы уровня), {idx + 1}-й уровень",
            f"wall:{side}:{top.price}")
    return None


def analyze_book(ticker: str, ob: OrderBook, st: BookState, lot: int,
                 decimals: int) -> list[Signal]:
    out: list[Signal] = []
    for side, levels in (("B", ob.bids), ("S", ob.asks)):
        s = _wall(levels, side, ticker, decimals, lot)
        if s:
            out.append(s)

    # --- спуфинг: крупная заявка исчезла быстро, цена до неё не дошла ----
    now = time.time()
    seen: set[tuple[str, float]] = set()
    for side, levels in (("B", ob.bids), ("S", ob.asks)):
        if len(levels) < WALL_MIN_LEVELS:
            continue
        med = statistics.median(l.qty for l in levels) or 1
        for l in levels:
            if l.qty >= SPOOF_MULT * med and l.qty * lot >= 1000:
                key = (side, l.price)
                seen.add(key)
                st.big.setdefault(key, (now, l.qty))
    best_bid = ob.bids[0].price if ob.bids else 0
    best_ask = ob.asks[0].price if ob.asks else 0
    for key in list(st.big):
        if key in seen:
            continue
        first, qty = st.big.pop(key)
        side, price = key
        life = now - first
        # если цена дошла до уровня — заявка могла исполниться, это не спуфинг
        touched = (side == "B" and best_bid <= price) or (side == "S" and best_ask >= price)
        if touched and life > 10:
            # стена стояла, до неё дошли и её больше нет => съели (пробой уровня)
            out.append(Signal(
                "eaten", ticker,
                f"🍽 <b>{ticker}</b> — стену съели\n"
                f"Плотность {fmt_n(qty * lot)} шт. по {fmt_price(price, decimals)} "
                f"({'бид' if side == 'B' else 'аск'}) простояла {int(life)} c и "
                f"исполнена — уровень {'пробит вниз' if side == 'B' else 'пробит вверх'}",
                f"eaten:{side}:{price}"))
        elif life <= SPOOF_MAX_LIFE_SEC and not touched:
            out.append(Signal(
                "spoof", ticker,
                f"👻 <b>{ticker}</b> — возможный спуфинг\n"
                f"Заявка {'на покупку' if side == 'B' else 'на продажу'} "
                f"{fmt_n(qty * lot)} шт. по {fmt_price(price, decimals)} "
                f"снята через {int(life)} c, цена до неё не дошла",
                f"spoof:{side}:{price}:{int(first)}"))
    st.last = ob
    return out


# =============================================================== контекст

def context_text(price: float, prev_close: float, day_high: float,
                 day_low: float, vwap: float, decimals: int,
                 ob: Optional[OrderBook] = None, lot: int = 1) -> str:
    """Блок «где мы находимся» под сигналом."""
    parts = []
    if prev_close:
        parts.append(f"к закр. {fmt_pct((price - prev_close) / prev_close * 100)}")
    if day_high and day_low and day_high > day_low:
        pos = (price - day_low) / (day_high - day_low)
        where = ("у максимума дня" if pos >= 0.95 else "у минимума дня" if pos <= 0.05
                 else f"{pos * 100:.0f}% диапазона дня")
        parts.append(where)
        parts.append(f"H {fmt_price(day_high, decimals)} / L {fmt_price(day_low, decimals)}")
    if vwap:
        parts.append(f"VWAP {fmt_price(vwap, decimals)} "
                     f"({'выше' if price >= vwap else 'ниже'})")
    line = "📍 " + " · ".join(parts) if parts else ""
    if ob and ob.bids and ob.asks:
        tb = sum(l.qty for l in ob.bids)
        ta = sum(l.qty for l in ob.asks)
        if tb and ta:
            r = tb / ta
            line += (f"\n📚 стакан: {'бид' if r >= 1 else 'аск'} сильнее в "
                     f"{max(r, 1 / r):.1f}×, спред "
                     f"{fmt_price(ob.asks[0].price - ob.bids[0].price, decimals)}")
    return line


# =============================================================== тексты

def book_text(ticker: str, name: str, ob: OrderBook, lot: int,
              decimals: int, depth: int = 10) -> str:
    bids, asks = ob.bids[:depth], ob.asks[:depth]
    tb = sum(l.qty for l in ob.bids)
    ta = sum(l.qty for l in ob.asks)
    ratio = tb / ta if ta else 0
    width = max(len(fmt_price(l.price, decimals)) for l in bids + asks) if bids + asks else 8
    lines = [f"📚 <b>{ticker}</b> · {esc(name)} · стакан "
             f"{ob.ts.astimezone(timezone(timedelta(hours=3))):%H:%M:%S} МСК",
             "<pre>"]
    maxq = max([l.qty for l in bids + asks] or [1])
    for l in reversed(asks):
        bar = "█" * max(1, round(l.qty / maxq * 12))
        lines.append(f"{fmt_price(l.price, decimals):>{width}}  {l.qty:>8}  🔴{bar}")
    spread = (asks[0].price - bids[0].price) if bids and asks else 0
    lines.append(f"{'—' * width}  спред {fmt_price(spread, decimals)}")
    for l in bids:
        bar = "█" * max(1, round(l.qty / maxq * 12))
        lines.append(f"{fmt_price(l.price, decimals):>{width}}  {l.qty:>8}  🟢{bar}")
    lines.append("</pre>")
    lines.append(f"Σ бид {fmt_n(tb * lot)} шт. · Σ аск {fmt_n(ta * lot)} шт. · "
                 f"перевес {'покупателей' if ratio > 1 else 'продавцов'} "
                 f"{max(ratio, 1 / ratio if ratio else 0):.1f}×")
    lines.append("<i>объёмы в лотах, 1 лот = %d шт.</i>" % lot)
    return "\n".join(lines)


def tape_text(ticker: str, trades: list[Trade], lot: int, decimals: int,
              n: int = 15) -> str:
    if not trades:
        return f"🧾 <b>{ticker}</b> — сделок за последние минуты нет"
    msk = timezone(timedelta(hours=3))
    win = trades[-200:]
    vb = sum(t.qty for t in win if t.side == "B")
    vs = sum(t.qty for t in win if t.side == "S")
    lines = [f"🧾 <b>{ticker}</b> · последние сделки", "<pre>"]
    for t in trades[-n:]:
        mark = "▲" if t.side == "B" else "▼"
        lines.append(f"{t.ts.astimezone(msk):%H:%M:%S} {mark} "
                     f"{fmt_price(t.price, decimals):>9} {t.qty:>7}")
    lines.append("</pre>")
    span = (win[-1].ts - win[0].ts).total_seconds() / 60
    lines.append(f"За {span:.0f} мин ({len(win)} сделок): покупки {vb} / продажи {vs} лот."
                 + (f" · {vb / (vb + vs) * 100:.0f}% покупок" if vb + vs else ""))
    return "\n".join(lines)


# =============================================================== киты

def fmt_rub(rub: float) -> str:
    return (f"{rub / 1e6:.1f} млн ₽" if rub >= 1e6 else f"{rub / 1e3:.0f} тыс ₽").replace(".", ",")


def whale_thresholds(ticker: str, base: Optional[Baseline]) -> tuple[float, float, bool]:
    """(порог одиночного кита ₽, порог сделки для серии ₽, адаптивен ли)."""
    floor = WHALE_FLOOR_RUB.get(ticker, WHALE_FLOOR_DEFAULT)
    q999 = base.quantile(WHALE_QUANTILE) if base else None
    q99 = base.quantile(WHALE_SERIES_QUANTILE) if base else None
    single = max(floor, q999) if q999 else floor
    # сделки серии: от p99 ленты, но не мельче 1/10 пола (SBER 5 млн, GAZP 0,5 млн, RUAL 0,3 млн)
    series = max(floor * 0.1, q99) if q99 else floor * 0.1
    return single, series, q999 is not None


def whale_signals(ticker: str, trades: list[Trade], lot: int, decimals: int,
                  base: Optional[Baseline]) -> list[Signal]:
    out: list[Signal] = []
    if not trades:
        return out
    msk = timezone(timedelta(hours=3))
    single_th, series_th, adaptive = whale_thresholds(ticker, base)
    typical = base.median_rub() if base else 0.0
    now = trades[-1].ts
    recent = [t for t in trades if now - t.ts <= timedelta(seconds=90)]

    # одиночные
    for t in recent:
        rub = t.qty * t.price * lot
        if rub < single_th:
            continue
        before = [x for x in trades if t.ts - timedelta(minutes=2) <= x.ts < t.ts]
        ctx = ""
        if len(before) >= 5:
            vb = sum(x.qty for x in before if x.side == "B")
            vs = sum(x.qty for x in before if x.side == "S")
            lo = min(x.price for x in before); hi = max(x.price for x in before)
            ctx = (f"\nЗа 2 мин до сделки: покупки {vb / (vb + vs) * 100:.0f}%, "
                   f"цена {fmt_price(lo, decimals)}–{fmt_price(hi, decimals)}.")
        rel = (f"Крупнее {WHALE_QUANTILE * 100:g}% сделок бумаги" if adaptive else "Выше порога по бумаге")
        if typical:
            rel += f", в {fmt_n(rub / typical)}× больше типичной ({fmt_rub(typical)})"
        out.append(Signal(
            "whale", ticker,
            f"🐋 <b>{ticker}</b> · кит · <b>{'покупка' if t.side == 'B' else 'продажа'}</b>\n"
            f"{t.ts.astimezone(msk):%H:%M:%S} МСК · <b>{fmt_n(t.qty * lot)} шт.</b> ({fmt_n(t.qty)} лот) "
            f"по <b>{fmt_price(t.price, decimals)}</b> ≈ <b>{fmt_rub(rub)}</b>\n\n{rel}.{ctx}",
            f"whale:{t.ts.isoformat()}:{t.price}:{t.qty}"))

    # серия: N+ крупных сделок одной стороны за WHALE_SERIES_SEC
    big = [t for t in trades if t.qty * t.price * lot >= series_th
           and now - t.ts <= timedelta(seconds=WHALE_SERIES_SEC + 60)]
    for side in ("B", "S"):
        ss = [t for t in big if t.side == side]
        if len(ss) < WHALE_SERIES_MIN:
            continue
        # берём самое свежее окно WHALE_SERIES_SEC, в котором есть >= N сделок
        best: list[Trade] = []
        for i in range(len(ss)):
            win = [x for x in ss[i:] if x.ts - ss[i].ts <= timedelta(seconds=WHALE_SERIES_SEC)]
            if len(win) >= WHALE_SERIES_MIN and len(win) >= len(best):
                best = win
        if not best or now - best[-1].ts > timedelta(seconds=90):
            continue
        total_rub = sum(t.qty * t.price * lot for t in best)
        total_qty = sum(t.qty for t in best)
        p0, p1 = best[0].price, best[-1].price
        chg = (p1 - p0) / p0 * 100 if p0 else 0.0
        win_all = [t for t in trades if now - t.ts <= timedelta(minutes=15)]
        share = total_qty / sum(t.qty for t in win_all) * 100 if win_all else 0.0
        if side == "S":
            verdict = ("продают по рынку, бид <b>поглощает</b> (цена почти не сдвинулась)"
                       if chg > -0.1 else "продают по рынку и <b>продавливают</b> цену")
        else:
            verdict = ("покупают по рынку, аск <b>поглощает</b> (цена почти не сдвинулась)"
                       if chg < 0.1 else "покупают по рынку и <b>двигают</b> цену вверх")
        rows = "\n".join(
            f"{t.ts.astimezone(msk):%H:%M:%S}  {'▲' if side == 'B' else '▼'}  "
            f"{fmt_price(t.price, decimals):>8}  {fmt_n(t.qty * lot):>10} шт.  {fmt_rub(t.qty * t.price * lot):>11}"
            for t in best[-8:])
        out.append(Signal(
            "whale_series", ticker,
            f"🐋🐋 <b>{ticker}</b> · серия китов · <b>{'покупка' if side == 'B' else 'продажа'}</b>\n"
            f"{best[0].ts.astimezone(msk):%H:%M:%S}–{best[-1].ts.astimezone(msk):%H:%M:%S} МСК · "
            f"<b>{len(best)} сделок</b> на <b>{fmt_rub(total_rub)}</b> ({fmt_n(total_qty * lot)} шт.)\n"
            f"<pre>{rows}</pre>"
            f"Цена за серию {fmt_price(p0, decimals)} → {fmt_price(p1, decimals)} ({fmt_pct(chg)}) — {verdict}. "
            f"Это {share:.0f}% всего объёма за 15 мин.",
            f"whale_series:{side}:{best[0].ts.strftime('%H:%M')}"))
    return out
