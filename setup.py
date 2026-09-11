"""Глубокий разбор бумаги перед входом: уровни по объёму, тесты уровней,
относительная сила к рынку, аномальный объём и итоговый скоринг.

Всё считается из уже накопленных данных: кластеры (ClusterStore),
дневные high/low/VWAP, стакан, свечи T-Invest/MOEX. Никаких предсказаний —
только структурированный список «за / против» с фактами.
"""
from __future__ import annotations

import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import clusters as clu
from formatting import esc, fmt_pct, fmt_price
from tinkoff import OrderBook

log = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# --------------------------------------------------------------- уровни

LEVEL_LOOKBACK_MIN = 5 * 24 * 60   # уровни ищем по кластерам за 5 дней (вся база)
LEVEL_MAX = 6                      # сколько уровней держим на бумагу
LEVEL_NEAR_PCT = 0.25              # «у уровня» — ближе N % цены
LEVEL_TOUCH_PCT = 0.12             # касание для теста уровня
LEVEL_REACT_MIN = 3                # минут после касания смотрим реакцию
LEVEL_ALERT_COOLDOWN = 45 * 60     # алерт «подошёл к уровню» не чаще, на бумагу+уровень


@dataclass
class Level:
    price: float          # центр уровня
    lo: float             # границы бина(ов)
    hi: float
    vol: int              # лоты
    buy: int
    sell: int
    share: float          # доля объёма периода
    kind: str             # "poc" | "hvn" (high volume node)
    day: str              # дата, когда объём прошёл (YYYY-MM-DD), или "" если несколько
    tests: list = field(default_factory=list)  # [Test]

    @property
    def delta(self) -> int:
        return self.buy - self.sell

    @property
    def buy_pct(self) -> float:
        v = self.buy + self.sell
        return self.buy / v * 100 if v else 0.0


@dataclass
class Test:
    ts: datetime          # MSK naive, минута касания
    from_above: bool      # подошли сверху (уровень как поддержка)
    vol: int              # объём за LEVEL_REACT_MIN после касания
    delta: int
    move_pct: float       # куда ушла цена через LEVEL_REACT_MIN (%)
    held: Optional[bool]  # уровень удержался (отскок) / пробит / None непонятно


def _step_for(price: float, decimals: int) -> float:
    """Шаг ценового бина для поиска уровней: ~0.05 % цены, не мельче тика."""
    tick = 10 ** (-decimals)
    raw = price * 0.0005
    if raw <= tick:
        return tick
    mag = 10 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5, 10):
        if mag * m >= raw:
            return round(mag * m, decimals + 2)
    return round(mag * 10, decimals + 2)


def find_levels(cells: list[clu.Cell], price: float, decimals: int,
                max_levels: int = LEVEL_MAX) -> list[Level]:
    """Уровни с максимальным объёмом (POC каждого дня + HVN всего периода)."""
    if not cells or not price:
        return []
    step = _step_for(price, decimals)
    total = sum(c.buy + c.sell for c in cells)
    if not total:
        return []
    # агрегируем по бину и дню
    bins: dict[float, list[int]] = {}
    per_day: dict[str, dict[float, list[int]]] = {}
    for c in cells:
        b = math.floor(c.price / step + 1e-9) * step
        b = round(b, decimals + 2)
        cell = bins.setdefault(b, [0, 0])
        cell[0] += c.buy; cell[1] += c.sell
        dk = c.minute.strftime("%Y-%m-%d")
        dc = per_day.setdefault(dk, {}).setdefault(b, [0, 0])
        dc[0] += c.buy; dc[1] += c.sell
    out: list[Level] = []
    used: set[float] = set()

    def _add(b: float, kind: str, day: str) -> None:
        # склеиваем соседние бины в один уровень (±1 шаг)
        for u in used:
            if abs(u - b) <= step * 1.01:
                return
        bb = [x for x in (round(b - step, decimals + 2), b, round(b + step, decimals + 2)) if x in bins]
        buy = sum(bins[x][0] for x in bb)
        sell = sum(bins[x][1] for x in bb)
        used.add(b)
        out.append(Level(price=round(b + step / 2, decimals + 2), lo=min(bb), hi=max(bb) + step,
                         vol=buy + sell, buy=buy, sell=sell, share=(buy + sell) / total,
                         kind=kind, day=day))

    # POC каждого дня (самые «честные» уровни)
    for dk in sorted(per_day, reverse=True):
        d = per_day[dk]
        b = max(d, key=lambda x: d[x][0] + d[x][1])
        _add(b, "poc", dk)
    # HVN всего периода
    for b in sorted(bins, key=lambda x: -(bins[x][0] + bins[x][1])):
        if len(out) >= max_levels:
            break
        _add(b, "hvn", "")
    out = sorted(out, key=lambda l: -l.vol)[:max_levels]
    out.sort(key=lambda l: l.price)
    return out


def level_tests(level: Level, cells: list[clu.Cell], decimals: int) -> list[Test]:
    """Разбор касаний уровня: подошли, что было с объёмом/дельтой, куда ушла цена."""
    if not cells:
        return []
    # минутный ряд: цена (VWAP минуты), объём, дельта
    by_min: dict[datetime, list[float]] = {}
    for c in cells:
        m = by_min.setdefault(c.minute, [0.0, 0, 0])  # pv, vol, delta
        v = c.buy + c.sell
        m[0] += c.price * v; m[1] += v; m[2] += c.buy - c.sell
    mins = sorted(by_min)
    series = [(m, by_min[m][0] / by_min[m][1] if by_min[m][1] else None, by_min[m][1], by_min[m][2])
              for m in mins]
    series = [s for s in series if s[1]]
    tol = level.price * LEVEL_TOUCH_PCT / 100
    tests: list[Test] = []
    i = 0
    last_side: Optional[bool] = None   # True — цена выше уровня
    while i < len(series):
        ts, p, v, d = series[i]
        above = p > level.hi if p > level.price else (p > level.lo and p > level.price)
        if abs(p - level.price) > max(tol, (level.hi - level.lo)):
            last_side = p > level.price
            i += 1
            continue
        # касание. откуда пришли?
        from_above = bool(last_side) if last_side is not None else p >= level.price
        # реакция за следующие LEVEL_REACT_MIN минут
        react = [s for s in series[i + 1:i + 1 + LEVEL_REACT_MIN + 5]
                 if s[0] <= ts + timedelta(minutes=LEVEL_REACT_MIN)]
        rv = sum(s[2] for s in react) + v
        rd = sum(s[3] for s in react) + d
        if react:
            p_after = react[-1][1]
            move = (p_after - level.price) / level.price * 100
            if from_above:
                held = True if move > LEVEL_TOUCH_PCT else (False if move < -LEVEL_TOUCH_PCT else None)
            else:
                held = True if move < -LEVEL_TOUCH_PCT else (False if move > LEVEL_TOUCH_PCT else None)
        else:
            move, held = 0.0, None
        tests.append(Test(ts, from_above, rv, rd, move, held))
        # пропускаем, пока цена не отойдёт от уровня
        j = i + 1
        while (j < len(series)
               and abs(series[j][1] - level.price) <= max(tol, level.hi - level.lo) * 1.5
               and series[j][0] - ts < timedelta(minutes=30)):
            j += 1
        last_side = series[j][1] > level.price if j < len(series) else None
        i = j
    return tests[-6:]


# ------------------------------------------------------ аномальный объём

def volume_anomaly(cells_now: list[clu.Cell], history: list[clu.Cell],
                   minutes: int = 5) -> Optional[tuple[float, int, float]]:
    """Объём за последние `minutes` против нормы того же времени суток.

    Возвращает (кратность к медиане, объём сейчас, медиана) или None,
    если истории для сравнения мало (< 3 дней)."""
    if not cells_now:
        return None
    now_v = sum(c.buy + c.sell for c in cells_now)
    last_min = max(c.minute for c in cells_now)
    hh, mm = last_min.hour, last_min.minute
    tod_from = hh * 60 + mm - minutes + 1
    tod_to = hh * 60 + mm
    by_day: dict[str, int] = {}
    for c in history:
        tod = c.minute.hour * 60 + c.minute.minute
        if tod_from <= tod <= tod_to and c.minute.date() != last_min.date():
            by_day[c.minute.strftime("%Y-%m-%d")] = by_day.get(c.minute.strftime("%Y-%m-%d"), 0) + c.buy + c.sell
    if len(by_day) < 3:
        return None
    med = statistics.median(by_day.values())
    if not med:
        return None
    return now_v / med, now_v, med


# ---------------------------------------------------- относительная сила

@dataclass
class RelStrength:
    ticker_chg: float
    index_chg: float
    label: str           # "сильнее рынка" / "слабее рынка" / "с рынком"
    window: str


def rel_strength(t_chg: Optional[float], i_chg: Optional[float], window: str) -> Optional[RelStrength]:
    if t_chg is None or i_chg is None:
        return None
    diff = t_chg - i_chg
    if diff >= 0.3:
        label = "сильнее рынка"
    elif diff <= -0.3:
        label = "слабее рынка"
    else:
        label = "с рынком"
    return RelStrength(t_chg, i_chg, label, window)


# -------------------------------------------------------- время дня

def session_phase(now: datetime) -> tuple[str, str]:
    """(название фазы, комментарий о ликвидности)."""
    m = now.hour * 60 + now.minute
    if m < 9 * 60 + 50:
        return "утренняя сессия", "тонкий рынок, широкие спреды — уровни пробиваются легко"
    if m < 10 * 60 + 15:
        return "открытие", "первые 15 мин: импульс и ложные выносы, объём не показателен"
    if m < 13 * 60:
        return "утро основной сессии", "самое ликвидное время, сигналы надёжнее"
    if m < 15 * 60:
        return "обед", "объём ниже, движения вязкие"
    if m < 18 * 60 + 40:
        return "вечер основной сессии", "приходят западные площадки, объём растёт"
    if m < 19 * 60:
        return "клиринг", "перерыв 18:40–19:05"
    return "вечерняя сессия", "объём ниже, крупные игроки менее активны"


# -------------------------------------------------------------- скоринг

@dataclass
class Factor:
    sign: int        # +1 за лонг, -1 за шорт, 0 нейтрально
    weight: int      # 1..2
    text: str


@dataclass
class Setup:
    ticker: str
    price: float
    factors: list[Factor]
    long_score: int
    short_score: int
    max_score: int
    verdict: str
    levels: list[Level]
    nearest: Optional[Level]
    nearest_dist_pct: float
    phase: tuple[str, str]
    rs15: Optional[RelStrength]
    rs_day: Optional[RelStrength]
    vol_anom: Optional[tuple[float, int, float]]
    an15: Optional[clu.Analysis]
    an1h: Optional[clu.Analysis]


def build_setup(ticker: str, price: float, prev_close: float, day_high: float,
                day_low: float, vwap: float, ob: Optional[OrderBook],
                cells_all: list[clu.Cell], decimals: int, lot: int,
                signals: list[tuple[str, int, float]],   # (kind, direction, age_min)
                rs15: Optional[RelStrength], rs_day: Optional[RelStrength],
                now: Optional[datetime] = None) -> Setup:
    now = now or datetime.now(MSK).replace(tzinfo=None)
    F: list[Factor] = []

    # --- уровни
    levels = find_levels(cells_all, price, decimals)
    for lv in levels:
        lv.tests = level_tests(lv, cells_all, decimals)
    nearest = min(levels, key=lambda l: abs(l.price - price)) if levels else None
    dist = abs(nearest.price - price) / price * 100 if nearest else 99.0
    if nearest and dist <= LEVEL_NEAR_PCT:
        side = "над" if price >= nearest.price else "под"
        held = [t for t in nearest.tests if t.held is True]
        broke = [t for t in nearest.tests if t.held is False]
        txt = (f"цена {side} уровнем {fmt_price(nearest.price, decimals)} "
               f"({clu.fmt_n(nearest.vol)} лот, {nearest.buy_pct:.0f}% покупок)")
        if nearest.tests:
            txt += f"; тестов {len(nearest.tests)}: удержан {len(held)}, пробит {len(broke)}"
        # уровень с историей удержания — за отскок от него
        if len(held) >= 2 and len(held) > len(broke):
            F.append(Factor(+1 if price >= nearest.price else -1, 2, txt + " — уровень держит"))
        elif nearest.tests and len(broke) > len(held):
            F.append(Factor(0, 1, txt + " — уровень слабый, часто пробивается"))
        else:
            F.append(Factor(+1 if nearest.buy_pct >= 55 else -1 if nearest.buy_pct <= 45 else 0, 1, txt))
    elif nearest:
        F.append(Factor(0, 1, f"до ближайшего уровня {fmt_price(nearest.price, decimals)} — "
                             f"{dist:.2f}% (вход в середине диапазона)"))

    # --- дельта 15м / 1ч и расхождение с ценой
    c15 = [c for c in cells_all if c.minute >= now - timedelta(minutes=15)]
    c1h = [c for c in cells_all if c.minute >= now - timedelta(minutes=60)]
    an15 = clu.analyze(ticker, "15m", c15, lot, decimals) if c15 else None
    an1h = clu.analyze(ticker, "1h", c1h, lot, decimals) if c1h else None
    for an, name in ((an15, "15 мин"), (an1h, "1 час")):
        if not an or an.n < 5:
            continue
        chg = (an.last - an.first) / an.first * 100 if an.first else 0
        if an.buy_pct >= 60:
            if chg < -0.15:
                F.append(Factor(-1, 2, f"{name}: покупают ({an.buy_pct:.0f}%), а цена −{abs(chg):.2f}% — "
                                       f"покупки поглощаются, продавец сильнее"))
            else:
                F.append(Factor(+1, 1 if name == "15 мин" else 2,
                                f"{name}: перевес покупок {an.buy_pct:.0f}%, дельта +{clu.fmt_n(an.delta)}, цена {fmt_pct(chg)}"))
        elif an.buy_pct <= 40:
            if chg > 0.15:
                F.append(Factor(+1, 2, f"{name}: продают ({100 - an.buy_pct:.0f}%), а цена +{chg:.2f}% — "
                                       f"продажи поглощаются, покупатель сильнее"))
            else:
                F.append(Factor(-1, 1 if name == "15 мин" else 2,
                                f"{name}: перевес продаж {100 - an.buy_pct:.0f}%, дельта {clu.fmt_n(an.delta)}, цена {fmt_pct(chg)}"))
        else:
            F.append(Factor(0, 1, f"{name}: баланс ({an.buy_pct:.0f}% покупок), цена {fmt_pct(chg)}"))

    # --- VWAP и диапазон дня
    if vwap:
        F.append(Factor(+1 if price >= vwap else -1, 1,
                        f"цена {'выше' if price >= vwap else 'ниже'} VWAP {fmt_price(vwap, decimals)} "
                        f"({fmt_pct((price - vwap) / vwap * 100)})"))
    if day_high and day_low and day_high > day_low:
        pos = (price - day_low) / (day_high - day_low)
        if pos >= 0.9:
            F.append(Factor(0, 1, f"у максимума дня {fmt_price(day_high, decimals)} — лонг догоняющий, шорт против импульса"))
        elif pos <= 0.1:
            F.append(Factor(0, 1, f"у минимума дня {fmt_price(day_low, decimals)} — шорт догоняющий, лонг против импульса"))
        else:
            F.append(Factor(0, 0, f"{pos * 100:.0f}% диапазона дня (H {fmt_price(day_high, decimals)} / L {fmt_price(day_low, decimals)})"))
    if prev_close:
        F.append(Factor(0, 0, f"к закрытию {fmt_pct((price - prev_close) / prev_close * 100)}"))

    # --- стакан
    if ob and ob.bids and ob.asks:
        tb = sum(l.qty for l in ob.bids); ta = sum(l.qty for l in ob.asks)
        if tb and ta:
            r = tb / ta
            if r >= 1.5:
                F.append(Factor(+1, 1, f"стакан: бид сильнее в {r:.1f}×"))
            elif r <= 1 / 1.5:
                F.append(Factor(-1, 1, f"стакан: аск сильнее в {1 / r:.1f}×"))
            else:
                F.append(Factor(0, 0, f"стакан сбалансирован ({r:.1f}×)"))
        med_b = statistics.median(l.qty for l in ob.bids) or 1
        med_a = statistics.median(l.qty for l in ob.asks) or 1
        wb = [l for l in ob.bids if l.qty >= 8 * med_b][:1]
        wa = [l for l in ob.asks if l.qty >= 8 * med_a][:1]
        if wb:
            F.append(Factor(+1, 1, f"плотность в биде {fmt_price(wb[0].price, decimals)} ({clu.fmt_n(wb[0].qty)} лот) — опора под ценой"))
        if wa:
            F.append(Factor(-1, 1, f"плотность в аске {fmt_price(wa[0].price, decimals)} ({clu.fmt_n(wa[0].qty)} лот) — потолок над ценой"))

    # --- свежие сигналы
    for kind, direction, age in signals:
        if age > 30 or not direction:
            continue
        from journal import KIND_EMOJI, KIND_NAME
        F.append(Factor(direction, 1, f"{KIND_EMOJI.get(kind, '')} {KIND_NAME.get(kind, kind)} "
                                      f"{age:.0f} мин назад, в сторону {'роста' if direction > 0 else 'падения'}"))

    # --- относительная сила
    for rs in (rs15, rs_day):
        if rs is None:
            continue
        s = +1 if rs.label == "сильнее рынка" else -1 if rs.label == "слабее рынка" else 0
        F.append(Factor(s, 1, f"{rs.window}: бумага {fmt_pct(rs.ticker_chg)}, IMOEX {fmt_pct(rs.index_chg)} — {rs.label}"))

    # --- аномальный объём
    va = volume_anomaly(c15[-5:] and [c for c in c15 if c.minute >= now - timedelta(minutes=5)], cells_all, 5)
    if va:
        k, v, med = va
        if k >= 3:
            F.append(Factor(0, 1, f"объём за 5 мин {clu.fmt_n(v)} лот — в {k:.1f}× выше нормы для этого времени ({clu.fmt_n(med)})"))
        elif k <= 0.4:
            F.append(Factor(0, 1, f"объём за 5 мин {clu.fmt_n(v)} лот — {k:.1f}× от нормы, рынок спит"))

    # --- время дня
    phase = session_phase(now)

    long_s = sum(f.weight for f in F if f.sign > 0)
    short_s = sum(f.weight for f in F if f.sign < 0)
    total = sum(f.weight for f in F if f.sign != 0)
    if total == 0:
        verdict = "нет направленных факторов — ждать"
    elif long_s >= short_s * 2 and long_s >= 4:
        verdict = "перевес в пользу лонга"
    elif short_s >= long_s * 2 and short_s >= 4:
        verdict = "перевес в пользу шорта"
    elif abs(long_s - short_s) <= 1:
        verdict = "факторы противоречат друг другу — лучше подождать"
    else:
        verdict = "слабый перевес " + ("лонга" if long_s > short_s else "шорта") + ", подтверждения мало"
    return Setup(ticker, price, F, long_s, short_s, total, verdict, levels, nearest, dist,
                 phase, rs15, rs_day, va, an15, an1h)


def setup_text(s: Setup, name: str, decimals: int) -> str:
    L = [f"🎯 <b>{esc(s.ticker)}</b> · {esc(name)} · {fmt_price(s.price, decimals)}",
         f"<b>Итог: {s.verdict}</b> — за лонг {s.long_score}, за шорт {s.short_score} (из {s.max_score})",
         f"🕒 {s.phase[0]}: {s.phase[1]}", ""]
    pos = [f for f in s.factors if f.sign > 0]
    neg = [f for f in s.factors if f.sign < 0]
    neu = [f for f in s.factors if f.sign == 0 and f.weight > 0]
    ctx = [f for f in s.factors if f.sign == 0 and f.weight == 0]
    if pos:
        L.append("🟢 <b>За лонг</b>")
        L += [f"• {esc(f.text)}" + (" (×2)" if f.weight == 2 else "") for f in pos]
    if neg:
        L.append("🔴 <b>За шорт</b>")
        L += [f"• {esc(f.text)}" + (" (×2)" if f.weight == 2 else "") for f in neg]
    if neu:
        L.append("⚪️ <b>Осторожно</b>")
        L += [f"• {esc(f.text)}" for f in neu]
    if ctx:
        L.append("📍 " + " · ".join(esc(f.text) for f in ctx))
    if s.levels:
        L += ["", "<b>Уровни по объёму</b> (ближайшие)", "<pre>"]
        near = sorted(s.levels, key=lambda l: abs(l.price - s.price))[:4]
        for lv in sorted(near, key=lambda l: -l.price):
            mark = "◀" if lv is s.nearest else " "
            held = sum(1 for t in lv.tests if t.held is True)
            broke = sum(1 for t in lv.tests if t.held is False)
            tests = f"{held}✓/{broke}✗" if lv.tests else "  —  "
            L.append(f"{fmt_price(lv.price, decimals):>9} {clu.fmt_n(lv.vol):>8} {lv.buy_pct:>3.0f}% "
                     f"{tests:>6} {lv.day[5:] if lv.day else 'HVN':>5} {mark}")
        L.append("</pre>")
        L.append("<i>цена · лоты · % покупок · тесты (удержан/пробит) · день POC</i>")
    L.append("\n<i>Это сводка фактов, а не рекомендация. Решение и риск — за вами.</i>")
    return "\n".join(L)


def level_alert_text(ticker: str, lv: Level, price: float, decimals: int,
                     from_above: bool) -> str:
    held = [t for t in lv.tests if t.held is True]
    broke = [t for t in lv.tests if t.held is False]
    role = "поддержке" if from_above else "сопротивлению"
    L = [f"📏 <b>{esc(ticker)}</b> подошёл к {role} <b>{fmt_price(lv.price, decimals)}</b> "
         f"(цена {fmt_price(price, decimals)})",
         f"Здесь прошло {clu.fmt_n(lv.vol)} лот"
         + (f" {lv.day[8:]}.{lv.day[5:7]}" if lv.day else " за 5 дней")
         + f", {lv.buy_pct:.0f}% покупок, дельта {'+' if lv.delta >= 0 else ''}{clu.fmt_n(lv.delta)}."]
    if lv.tests:
        L.append(f"Тестов: {len(lv.tests)} — удержан {len(held)}, пробит {len(broke)}.")
        last = lv.tests[-1]
        L.append(f"Последний {last.ts:%d.%m %H:%M}: объём {clu.fmt_n(last.vol)}, дельта "
                 f"{'+' if last.delta >= 0 else ''}{clu.fmt_n(last.delta)}, цена за {LEVEL_REACT_MIN} мин "
                 f"{fmt_pct(last.move_pct)} → {'удержан' if last.held else 'пробит' if last.held is False else 'без реакции'}.")
    else:
        L.append("Ранее не тестировался — первая реакция самая показательная.")
    L.append("Смотрите 🎯 Сетап — там сведены все факторы.")
    return "\n".join(L)
