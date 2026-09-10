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


# =============================================================== лента

def analyze_trades(ticker: str, trades: list[Trade], lot: int,
                   decimals: int) -> list[Signal]:
    if len(trades) < 10:
        return []
    out: list[Signal] = []
    now = trades[-1].ts
    recent = [t for t in trades if now - t.ts <= timedelta(seconds=ICEBERG_WINDOW_SEC)]

    # --- айсберг: одинаковые (цена, объём, сторона) ------------------------
    groups: dict[tuple, list[Trade]] = defaultdict(list)
    for t in recent:
        if t.qty > 0:
            groups[(t.price, t.qty, t.side)].append(t)
    for (price, qty, side), g in groups.items():
        if len(g) >= ICEBERG_MIN_TRADES and qty * len(g) >= 20:
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
        if per_min[last_m] >= BURST_MIN_TRADES and per_min[last_m] >= BURST_MULT * med:
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
    """Память по стакану одного инструмента для детекции спуфинга."""
    big: dict[tuple[str, float], tuple[float, int]] = field(default_factory=dict)
    # (side, price) -> (first_seen_ts, qty)


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
        if life <= SPOOF_MAX_LIFE_SEC and not touched:
            out.append(Signal(
                "spoof", ticker,
                f"👻 <b>{ticker}</b> — возможный спуфинг\n"
                f"Заявка {'на покупку' if side == 'B' else 'на продажу'} "
                f"{fmt_n(qty * lot)} шт. по {fmt_price(price, decimals)} "
                f"снята через {int(life)} c, цена до неё не дошла",
                f"spoof:{side}:{price}:{int(first)}"))
    return out


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
