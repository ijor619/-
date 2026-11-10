"""Actionable trade ideas with an explicit, auditable life cycle.

Raw tape/order-book events are deliberately not ideas.  An idea is created
only when several independent groups agree: price structure, order flow and
market context.  It then moves through prepared -> active -> target/stop or
is cancelled/expired before entry.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Optional
from uuid import uuid4


log = logging.getLogger(__name__)

TERMINAL = {"target", "stop", "cancelled", "expired"}
ACTIVE = {"prepared", "active"}
MAX_LEVEL_DISTANCE_PCT = 5.0
MAX_TRIGGER_DISTANCE_PCT = 2.0
MAX_RISK_PCT = 3.0
SETUP_NAMES = {
    "breakout": "принятый пробой",
    "false_breakout": "ложный пробой",
    "pullback": "откат по тренду",
    "absorption": "поглощение у уровня",
}


@dataclass
class IdeaContext:
    ticker: str
    price: float
    prev_price: float
    decimals: int = 2
    lot: int = 1
    vwap: float = 0.0
    levels: list[tuple[str, float]] = field(default_factory=list)
    ch5: Optional[float] = None
    ch15: Optional[float] = None
    ch60: Optional[float] = None
    market15: Optional[float] = None
    buy_pct5: Optional[float] = None
    buy_pct15: Optional[float] = None
    volume_mult: Optional[float] = None
    spread_pct: Optional[float] = None
    book_imbalance: Optional[float] = None  # -1..+1, distance weighted
    microprice_edge_pct: Optional[float] = None
    breadth: Optional[float] = None
    sector_edge: Optional[float] = None
    rsi14: Optional[float] = None
    ema_edge_pct: Optional[float] = None
    turnover_rub_min: Optional[float] = None
    fresh: bool = True
    event_risk: str = ""
    news_risk: str = ""

    def nearest(self) -> Optional[tuple[str, float, float]]:
        xs = [(n, p, abs(self.price - p) / p * 100) for n, p in self.levels if p > 0]
        return min(xs, key=lambda x: x[2]) if xs else None


@dataclass
class Delivery:
    uid: int
    chat_id: int
    msg_id: int
    decision: str = ""       # taken / skipped


@dataclass
class Idea:
    id: str
    ticker: str
    direction: int
    setup: str
    created_ts: float
    updated_ts: float
    expires_ts: float
    status: str
    trigger: float
    entry_lo: float
    entry_hi: float
    stop: float
    target1: float
    target2: float
    confidence: int
    reasons: list[str] = field(default_factory=list)
    cautions: list[str] = field(default_factory=list)
    level_name: str = ""
    level: float = 0.0
    spread_pct: float = 0.0
    lot: int = 1
    decimals: int = 2
    trigger_ts: float = 0.0
    entry_price: float = 0.0
    closed_ts: float = 0.0
    exit_price: float = 0.0
    close_reason: str = ""
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    realized_r: float = 0.0
    market_regime: str = ""
    timeframe_score: int = 0
    liquidity_rub_min: float = 0.0
    deliveries: list[Delivery] = field(default_factory=list)

    @property
    def side(self) -> str:
        return "лонг" if self.direction > 0 else "шорт"

    def delivery(self, uid: int) -> Optional[Delivery]:
        return next((d for d in self.deliveries if d.uid == uid), None)


def idea_geometry_error(idea: Idea, reference_price: float = 0.0) -> str:
    """Return why an idea is unsafe, or an empty string when geometry is sane."""
    prices = (idea.trigger, idea.entry_lo, idea.entry_hi, idea.stop,
              idea.target1, idea.target2)
    if idea.direction not in (-1, 1):
        return "некорректное направление"
    if any(not math.isfinite(x) or x <= 0 for x in prices):
        return "неположительная или бесконечная цена"
    if idea.direction > 0:
        ordered = (idea.stop < idea.entry_lo <= idea.trigger <= idea.entry_hi
                   < idea.target1 < idea.target2)
    else:
        ordered = (idea.target2 < idea.target1 < idea.entry_lo <= idea.trigger
                   <= idea.entry_hi < idea.stop)
    if not ordered:
        return "перепутан порядок входа, стопа и целей"
    risk_pct = abs(idea.trigger - idea.stop) / idea.trigger * 100
    if risk_pct <= 0 or risk_pct > MAX_RISK_PCT:
        return f"риск {risk_pct:.2f}% вне допустимого диапазона"
    if idea.level and abs(idea.level - idea.trigger) / idea.trigger * 100 > MAX_LEVEL_DISTANCE_PCT:
        return "уровень относится к другому ценовому диапазону"
    if reference_price > 0:
        trigger_gap = abs(idea.trigger - reference_price) / reference_price * 100
        if trigger_gap > MAX_TRIGGER_DISTANCE_PCT:
            return f"триггер на {trigger_gap:.2f}% от свежей цены"
    return ""


class IdeaJournal:
    def __init__(self, path: str) -> None:
        self.path = path
        self.ideas: list[Idea] = []
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
            if raw.get("version") != 1:
                raise ValueError("неизвестная версия журнала идей")
            self.ideas = []
            dropped = 0
            for row in raw.get("ideas", []):
                row["deliveries"] = [Delivery(**d) for d in row.get("deliveries", [])]
                idea = Idea(**row)
                reason = idea_geometry_error(idea)
                if reason:
                    dropped += 1
                    log.warning("ideas: отброшена повреждённая идея %s %s: %s",
                                idea.ticker, idea.id, reason)
                    continue
                self.ideas.append(idea)
            if dropped:
                self.save()
        except FileNotFoundError:
            self.ideas = []
        except Exception:
            # A damaged statistics file must not stop market monitoring. Keep it
            # for inspection and begin a clean journal next to it.
            if os.path.exists(self.path):
                try:
                    os.replace(self.path, self.path + f".broken-{int(time.time())}")
                except OSError:
                    pass
            self.ideas = []

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        cutoff = time.time() - 180 * 86400
        self.ideas = [x for x in self.ideas if x.created_ts >= cutoff or x.status in ACTIVE]
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "ideas": [asdict(x) for x in self.ideas]},
                      f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.path)

    def add(self, idea: Idea) -> None:
        reason = idea_geometry_error(idea)
        if reason:
            log.warning("ideas: не сохранена повреждённая идея %s %s: %s",
                        idea.ticker, idea.id, reason)
            return
        if not any(x.id == idea.id for x in self.ideas):
            self.ideas.append(idea)
            self.save()

    def active(self, ticker: Optional[str] = None) -> list[Idea]:
        return [x for x in self.ideas if x.status in ACTIVE and (not ticker or x.ticker == ticker)]

    def find(self, idea_id: str) -> Optional[Idea]:
        return next((x for x in self.ideas if x.id == idea_id), None)

    def has_active(self, ticker: str, direction: int) -> bool:
        return any(x.ticker == ticker and x.direction == direction and x.status in ACTIVE
                   for x in self.ideas)

    def set_decision(self, idea_id: str, uid: int, decision: str) -> bool:
        idea = self.find(idea_id)
        d = idea.delivery(uid) if idea else None
        if not d:
            return False
        d.decision = decision
        self.save()
        return True

    def stats(self, uid: int, days: int = 30, ticker: str = "") -> str:
        cutoff = time.time() - max(1, min(days, 180)) * 86400
        xs = [x for x in self.ideas if x.created_ts >= cutoff
              and (not ticker or x.ticker == ticker)
              and any(d.uid == uid for d in x.deliveries)]
        if not xs:
            return "💡 Идей за этот период ещё нет."
        triggered = [x for x in xs if x.trigger_ts]
        wins = [x for x in triggered if x.status == "target"]
        stops = [x for x in triggered if x.status == "stop"]
        live = [x for x in xs if x.status in ACTIVE]
        canc = [x for x in xs if x.status == "cancelled"]
        exp = [x for x in xs if x.status == "expired"]
        resolved = wins + stops
        wr = len(wins) / len(resolved) * 100 if resolved else 0.0
        er = sum(x.realized_r for x in resolved) / len(resolved) if resolved else 0.0
        act = len(triggered) / len(xs) * 100
        lines = [f"💡 <b>Статистика идей</b> · {days} дн."
                 + (f" · {ticker}" if ticker else ""),
                 f"Всего: <b>{len(xs)}</b> · активировано: <b>{len(triggered)}</b> ({act:.0f}%) · сейчас: {len(live)}",
                 f"✅ цель: <b>{len(wins)}</b> · 🛑 стоп: <b>{len(stops)}</b> · ↩️ отмена: {len(canc)} · ⌛ истекла: {len(exp)}",
                 f"Точность после входа: <b>{wr:.0f}%</b> · ожидание: <b>{er:+.2f}R</b>", ""]
        groups: dict[tuple[str, int], list[Idea]] = {}
        for x in xs:
            groups.setdefault((x.setup, x.direction), []).append(x)
        lines.append("<pre>сетап                 n  вход  ц/с   E(R)")
        for (setup, direction), grp in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            tr = [x for x in grp if x.trigger_ts]
            w = sum(x.status == "target" for x in tr)
            s = sum(x.status == "stop" for x in tr)
            resolved_group = [x for x in tr if x.status in ("target", "stop")]
            group_er = (sum(x.realized_r for x in resolved_group) / len(resolved_group)
                        if resolved_group else 0.0)
            name = SETUP_NAMES.get(setup, setup)[:17]
            side = "L" if direction > 0 else "S"
            lines.append(f"{name:<17} {side} {len(grp):>3} {len(tr):>5} "
                         f"{w:>2}/{s:<2} {group_er:>+5.2f}")
        lines.append("</pre>")
        taken = [x for x in xs if (x.delivery(uid) and x.delivery(uid).decision == "taken")]
        skipped = [x for x in xs if (x.delivery(uid) and x.delivery(uid).decision == "skipped")]
        lines.append(f"👆 Отметки: «взял» {len(taken)}, «пропустил» {len(skipped)}.")
        lines.append("<i>Точность считается только по идеям, где цена сначала дошла до триггера; отмены до входа не портят win rate.</i>")
        return "\n".join(lines)

    def adaptive_adjustment(self, setup: str, direction: int,
                            min_sample: int = 20) -> tuple[int, int, float]:
        """Conservative score correction from resolved market ideas.

        It is deliberately inactive on a small sample.  One market idea is
        one observation regardless of how many users received it.
        """
        rows = [x for x in self.ideas
                if x.setup == setup and x.direction == direction
                and x.status in ("target", "stop") and x.trigger_ts]
        n = len(rows)
        expectancy = sum(x.realized_r for x in rows) / n if n else 0.0
        if n < min_sample:
            return 0, n, expectancy
        wins = sum(x.status == "target" for x in rows)
        win_rate = wins / n
        if expectancy <= -0.15 and win_rate < 0.40:
            return -10, n, expectancy
        if expectancy < 0:
            return -5, n, expectancy
        if expectancy >= 0.25 and win_rate >= 0.45:
            return 5, n, expectancy
        return 0, n, expectancy

    def paper_stats(self, uid: int, days: int = 30, ticker: str = "") -> str:
        """Virtual results only for ideas explicitly marked as taken."""
        days = max(1, min(int(days), 180))
        cutoff = time.time() - days * 86400
        rows = [x for x in self.ideas
                if x.created_ts >= cutoff
                and (not ticker or x.ticker == ticker)
                and x.delivery(uid) is not None
                and x.delivery(uid).decision == "taken"]
        title = f"🧪 <b>Бумажные сделки · {days} дн.</b>" + (f" · {ticker}" if ticker else "")
        if not rows:
            return title + "\nЕщё нет идей, отмеченных кнопкой «Взял»."
        triggered = [x for x in rows if x.trigger_ts]
        targets = [x for x in triggered if x.status == "target"]
        stops = [x for x in triggered if x.status == "stop"]
        timed = [x for x in triggered if x.status == "expired"]
        active = [x for x in triggered if x.status == "active"]
        waiting = [x for x in rows if x.status == "prepared"]
        no_entry = [x for x in rows if not x.trigger_ts and x.status in ("cancelled", "expired")]
        resolved = targets + stops
        win_rate = len(targets) / len(resolved) * 100 if resolved else 0.0
        expectancy = sum(x.realized_r for x in resolved) / len(resolved) if resolved else 0.0
        completed = resolved + timed
        total_r = sum(x.realized_r for x in completed)
        lines = [title,
                 f"Отмечено «Взял»: <b>{len(rows)}</b> · триггер достигнут: {len(triggered)}",
                 f"✅ цель: <b>{len(targets)}</b> · 🛑 стоп: <b>{len(stops)}</b> · ⌛ закрыты по времени: {len(timed)}",
                 f"Точность цель/стоп: <b>{win_rate:.0f}%</b> · ожидание: <b>{expectancy:+.2f}R</b>",
                 f"Сумма завершённых исходов: <b>{total_r:+.2f}R</b>",
                 f"Сейчас: ждут входа {len(waiting)} · активны {len(active)} · отменены до входа {len(no_entry)}",
                 "", "<b>Последние:</b>"]
        labels = {"target": "✅ цель", "stop": "🛑 стоп",
                  "cancelled": "↩️ без входа", "expired": "⌛ время",
                  "active": "🚀 активна", "prepared": "⏳ ждёт"}
        for idea in sorted(rows, key=lambda x: x.created_ts, reverse=True)[:8]:
            result = f" · {idea.realized_r:+.2f}R" if idea.trigger_ts and idea.status in TERMINAL else ""
            lines.append(f"• {idea.ticker} {('L' if idea.direction > 0 else 'S')} · "
                         f"{labels.get(idea.status, idea.status)}{result}")
        lines.append("<i>Это симуляция от триггера идеи. Она не проверяет покупку на счёте и не учитывает твоё реальное проскальзывание.</i>")
        return "\n".join(lines)


@dataclass
class BreakProbe:
    ticker: str
    level_name: str
    level: float
    direction: int
    ts: float


class IdeaEngine:
    """Stateful detector. It never sends messages and is easy to unit-test."""
    def __init__(self) -> None:
        self.probes: dict[tuple[str, float], BreakProbe] = {}
        self.cooldown: dict[tuple[str, str, int], float] = {}

    @staticmethod
    def _agree(value: Optional[float], direction: int, dead: float = 0.0) -> bool:
        return value is not None and value * direction > dead

    @staticmethod
    def _book_ok(ctx: IdeaContext, direction: int) -> bool:
        return ctx.book_imbalance is not None and ctx.book_imbalance * direction >= 0.08

    @staticmethod
    def regime(ctx: IdeaContext) -> tuple[str, int]:
        """Return a compact market regime and its direction (-1/0/+1)."""
        ch5, ch15, ch60 = ctx.ch5 or 0.0, ctx.ch15 or 0.0, ctx.ch60 or 0.0
        if (ctx.volume_mult or 0) >= 2 and abs(ch5) >= 0.25:
            direction = 1 if ch5 > 0 else -1
            return ("импульс вверх" if direction > 0 else "импульс вниз", direction)
        if ch60 >= 0.45 and ch15 >= -0.15:
            return "тренд вверх", 1
        if ch60 <= -0.45 and ch15 <= 0.15:
            return "тренд вниз", -1
        if abs(ch60) < 0.35 and abs(ch15) < 0.20:
            return "диапазон", 0
        return "смешанный", 0

    @staticmethod
    def timeframe_alignment(ctx: IdeaContext, direction: int) -> tuple[int, int]:
        """Signed agreement count across 5/15/60 minute horizons."""
        values = ((ctx.ch5, 0.03), (ctx.ch15, 0.08), (ctx.ch60, 0.20))
        score = available = 0
        for value, dead in values:
            if value is None:
                continue
            available += 1
            if value * direction > dead:
                score += 1
            elif value * direction < -dead:
                score -= 1
        return score, available

    def _quality_gate(self, idea: Idea, ctx: IdeaContext) -> bool:
        """Enrich an idea and reject contexts likely to be noise/slippage."""
        spread = ctx.spread_pct
        if spread is None or spread > 0.20:
            return False
        if ctx.turnover_rub_min is None or ctx.turnover_rub_min < 100_000:
            return False
        regime, regime_direction = self.regime(ctx)
        tf_score, tf_available = self.timeframe_alignment(ctx, idea.direction)
        # Continuation setups need a readable higher-timeframe context.
        if idea.setup in ("breakout", "pullback"):
            if tf_available < 2 or tf_score <= -1:
                return False
            if idea.setup == "pullback" and regime_direction != idea.direction:
                return False
            if idea.setup == "breakout" and regime == "диапазон" and (ctx.volume_mult or 0) < 2:
                return False
        # Do not fade a strong impulse without both absorption and book support.
        if idea.setup in ("false_breakout", "absorption"):
            if regime.startswith("импуль") and regime_direction == -idea.direction:
                if not self._book_ok(ctx, idea.direction):
                    return False

        adjustment = 0
        if tf_score >= 2:
            adjustment += 5
        elif tf_score <= -2:
            adjustment -= 8
        if spread <= 0.08:
            adjustment += 4
        if (ctx.turnover_rub_min or 0) >= 500_000:
            adjustment += 4
        if ctx.microprice_edge_pct is not None and ctx.microprice_edge_pct * idea.direction >= 0.01:
            adjustment += 4
        if ctx.breadth is not None:
            if (idea.direction > 0 and ctx.breadth >= 55) or (idea.direction < 0 and ctx.breadth <= 45):
                adjustment += 3
            elif (idea.direction > 0 and ctx.breadth <= 30) or (idea.direction < 0 and ctx.breadth >= 70):
                adjustment -= 6
        idea.confidence = max(1, min(95, idea.confidence + adjustment))
        idea.market_regime = regime
        idea.timeframe_score = tf_score
        idea.liquidity_rub_min = ctx.turnover_rub_min or 0.0
        return idea.confidence >= 60

    def _make(self, ctx: IdeaContext, setup: str, direction: int, confidence: int,
              reasons: list[str], level_name: str = "", level: float = 0.0,
              minutes: int = 30) -> Idea:
        p = ctx.price
        spread = max(ctx.spread_pct or 0.0, 0.02)
        # A trigger outside current noise, but no farther than a sensible
        # intraday entry. Stop distance is volatility-aware and at least 0.35%.
        trigger_gap = max(spread * 1.5, 0.04) / 100
        trigger = p * (1 + direction * trigger_gap)
        if level:
            trigger = max(trigger, level * (1 + trigger_gap)) if direction > 0 else min(trigger, level * (1 - trigger_gap))
        vol = max(abs(ctx.ch5 or 0.0) * 1.4, abs(ctx.ch15 or 0.0) * 0.45, 0.35)
        risk = p * min(vol, 1.5) / 100
        if level:
            structural = abs(trigger - level) + p * max(spread, 0.05) / 100
            risk = max(risk, structural)
        stop = trigger - direction * risk
        entry_lo, entry_hi = sorted((trigger - p * spread / 200, trigger + p * spread / 200))
        target1 = trigger + direction * risk * 1.5
        target2 = trigger + direction * risk * 2.5
        cautions = []
        if (ctx.spread_pct or 0) >= 0.15:
            cautions.append(f"широкий спред {ctx.spread_pct:.2f}%")
        if ctx.event_risk:
            cautions.append(ctx.event_risk)
        if ctx.news_risk:
            cautions.append(ctx.news_risk)
        if ctx.breadth is not None and ((direction > 0 and ctx.breadth < 35) or
                                        (direction < 0 and ctx.breadth > 65)):
            cautions.append(f"ширина рынка против идеи ({ctx.breadth:.0f}% бумаг растут)")
        if ctx.rsi14 is not None and ((direction > 0 and ctx.rsi14 > 75) or
                                      (direction < 0 and ctx.rsi14 < 25)):
            cautions.append(f"RSI 14 в экстремуме ({ctx.rsi14:.0f})")
        return Idea(uuid4().hex[:12], ctx.ticker, direction, setup, time.time(), time.time(),
                    time.time() + minutes * 60, "prepared", trigger, entry_lo, entry_hi,
                    stop, target1, target2, max(1, min(confidence, 95)), reasons, cautions,
                    level_name, level, ctx.spread_pct or 0.0, ctx.lot, ctx.decimals)

    def candidates(self, ctx: IdeaContext) -> list[Idea]:
        if not ctx.fresh or ctx.price <= 0 or (ctx.spread_pct or 0) > 0.35:
            return []
        now = time.time()
        out: list[Idea] = []
        nearest = ctx.nearest()

        # First remember every structural cross. A cross is not a breakout yet.
        for name, lv in ctx.levels:
            if not lv or not ctx.prev_price:
                continue
            if abs(lv - ctx.price) / ctx.price * 100 > MAX_LEVEL_DISTANCE_PCT:
                continue
            direction = 1 if ctx.prev_price < lv <= ctx.price else -1 if ctx.prev_price > lv >= ctx.price else 0
            if direction:
                self.probes[(ctx.ticker, round(lv, 6))] = BreakProbe(ctx.ticker, name, lv, direction, now)

        for key, pr in list(self.probes.items()):
            # Probes are shared by the engine, but every probe belongs to one
            # instrument. Applying a RUAL level to LKOH produced absurd stops
            # near 20 rubles and targets above 14,000 rubles.
            if pr.ticker != ctx.ticker:
                continue
            age = now - pr.ts
            if abs(pr.level - ctx.price) / ctx.price * 100 > MAX_LEVEL_DISTANCE_PCT:
                self.probes.pop(key, None)
                continue
            outside = (ctx.price - pr.level) * pr.direction > 0
            flow = (ctx.buy_pct5 or 50) - 50
            flow_ok = flow * pr.direction >= 7
            if 45 <= age <= 5 * 60 and outside and flow_ok and self._agree(ctx.ch15, pr.direction, 0.05):
                reasons = [f"цена удержалась за уровнем {pr.level:g}",
                           f"дельта 5м подтверждает ({ctx.buy_pct5:.0f}% покупок)"]
                conf = 62 + (10 if self._book_ok(ctx, pr.direction) else 0) + (8 if (ctx.volume_mult or 0) >= 2 else 0)
                out.append(self._make(ctx, "breakout", pr.direction, conf, reasons,
                                      pr.level_name, pr.level, 35))
                self.probes.pop(key, None)
            elif age <= 5 * 60 and not outside and age >= 20 and flow * pr.direction <= -5:
                direction = -pr.direction
                reasons = [f"цена вернулась за {pr.level:g} после выноса",
                           "агрессивный поток развернулся"]
                conf = 66 + (10 if self._book_ok(ctx, direction) else 0)
                out.append(self._make(ctx, "false_breakout", direction, conf, reasons,
                                      pr.level_name, pr.level, 25))
                self.probes.pop(key, None)
            elif age > 5 * 60:
                self.probes.pop(key, None)

        # Trend pullback: long-horizon direction, return to VWAP/volume level,
        # then short-horizon flow resumes. These are independent factor groups.
        trend = 1 if (ctx.ch60 or 0) >= 0.45 else -1 if (ctx.ch60 or 0) <= -0.45 else 0
        ref_name, ref, dist = (nearest if nearest else ("VWAP", ctx.vwap, 99.0))
        if ctx.vwap:
            vd = abs(ctx.price - ctx.vwap) / ctx.vwap * 100
            if vd < dist:
                ref_name, ref, dist = "VWAP", ctx.vwap, vd
        flow5 = (ctx.buy_pct5 or 50) - 50
        rs = (ctx.ch15 - ctx.market15) if ctx.ch15 is not None and ctx.market15 is not None else 0.0
        if trend and dist <= 0.28 and flow5 * trend >= 7 and rs * trend >= -0.05:
            reasons = [f"тренд 60м {ctx.ch60:+.2f}%", f"откат к {ref_name} {ref:g}",
                       f"поток 5м снова за {('покупки' if trend > 0 else 'продажи')}"]
            conf = 64 + (10 if self._book_ok(ctx, trend) else 0) + (7 if rs * trend >= 0.2 else 0)
            if ctx.sector_edge is not None and ctx.sector_edge * trend >= 0.15:
                conf += 6
                reasons.append(f"сильнее сектора на {ctx.sector_edge * trend:+.2f}%")
            if ctx.ema_edge_pct is not None and ctx.ema_edge_pct * trend < 0:
                conf -= 8  # indicator is context only, never a trigger
            out.append(self._make(ctx, "pullback", trend, conf, reasons, ref_name, ref, 40))

        # Absorption is deliberately opposite to the aggressive side: strong
        # buying without upward response near resistance means a seller absorbs.
        if nearest and nearest[2] <= 0.25 and ctx.buy_pct5 is not None and ctx.ch5 is not None:
            aggressor = 1 if ctx.buy_pct5 >= 70 else -1 if ctx.buy_pct5 <= 30 else 0
            stalled = aggressor and ctx.ch5 * aggressor <= 0.08
            side_of_level = (ctx.price - nearest[1])
            structural = side_of_level * aggressor <= 0
            direction = -aggressor
            if stalled and structural and (self._book_ok(ctx, direction) or (ctx.volume_mult or 0) >= 2):
                reasons = [f"{ctx.buy_pct5:.0f}% агрессивных {('покупок' if aggressor > 0 else 'продаж')}, но цена не идёт за ними",
                           f"поглощение у {nearest[0]} {nearest[1]:g}"]
                conf = 68 + (10 if self._book_ok(ctx, direction) else 0)
                out.append(self._make(ctx, "absorption", direction, conf, reasons,
                                      nearest[0], nearest[1], 25))

        unique: list[Idea] = []
        for idea in sorted(out, key=lambda x: -x.confidence):
            if not self._quality_gate(idea, ctx):
                continue
            reason = idea_geometry_error(idea, ctx.price)
            if reason:
                log.warning("ideas: отклонена %s %s: %s", idea.ticker, idea.setup, reason)
                continue
            ck = (idea.ticker, idea.setup, idea.direction)
            if now - self.cooldown.get(ck, 0) < 30 * 60:
                continue
            if any(x.direction == idea.direction for x in unique):
                continue
            self.cooldown[ck] = now
            unique.append(idea)
        return unique


def update_lifecycle(idea: Idea, price: float, now: Optional[float] = None,
                     context_valid: bool = True) -> Optional[str]:
    """Update one idea from a fresh exchange price; return transition name."""
    now = now or time.time()
    if idea.status not in ACTIVE or price <= 0:
        return None
    idea.updated_ts = now
    if idea.status == "prepared":
        # Invalidation before entry is a cancellation, never a stop.
        invalid = price <= idea.stop if idea.direction > 0 else price >= idea.stop
        if invalid or not context_valid:
            idea.status, idea.closed_ts, idea.exit_price = "cancelled", now, price
            idea.close_reason = "откат сломал уровень до входа" if invalid else "контекст перестал подтверждать идею"
            return "cancelled"
        hit = price >= idea.trigger if idea.direction > 0 else price <= idea.trigger
        if hit:
            idea.status, idea.trigger_ts, idea.entry_price = "active", now, idea.trigger
            return "active"
        if now >= idea.expires_ts:
            idea.status, idea.closed_ts, idea.exit_price = "expired", now, price
            idea.close_reason = "цена не дошла до триггера"
            return "expired"
        return None
    signed = (price / idea.entry_price - 1) * 100 * idea.direction
    idea.mfe_pct = max(idea.mfe_pct, signed)
    idea.mae_pct = min(idea.mae_pct, signed)
    target = price >= idea.target1 if idea.direction > 0 else price <= idea.target1
    stopped = price <= idea.stop if idea.direction > 0 else price >= idea.stop
    risk = abs(idea.entry_price - idea.stop)
    if target:
        idea.status, idea.closed_ts, idea.exit_price = "target", now, idea.target1
        idea.realized_r = abs(idea.target1 - idea.entry_price) / risk if risk else 0.0
        idea.close_reason = "первая цель достигнута"
        return "target"
    if stopped:
        idea.status, idea.closed_ts, idea.exit_price = "stop", now, idea.stop
        idea.realized_r = -1.0
        idea.close_reason = "защитный стоп достигнут"
        return "stop"
    if now >= idea.expires_ts:
        idea.status, idea.closed_ts, idea.exit_price = "expired", now, price
        idea.realized_r = ((price - idea.entry_price) * idea.direction / risk) if risk else 0.0
        idea.close_reason = "время идеи истекло после входа"
        return "expired"
    return None


def position_size(idea: Idea, risk_rub: float, costs_bps: float = 8.0) -> tuple[int, float]:
    """Return lots and estimated ruble risk including round-trip costs."""
    if risk_rub <= 0 or idea.lot <= 0:
        return 0, 0.0
    per_share = abs(idea.trigger - idea.stop) + idea.trigger * costs_bps / 10000
    per_lot = per_share * idea.lot
    lots = math.floor(risk_rub / per_lot) if per_lot > 0 else 0
    return max(0, lots), max(0, lots) * per_lot


def card(idea: Idea, risk_rub: float = 0.0) -> str:
    d = idea.decimals
    f = lambda x: f"{x:.{d}f}".replace(".", ",")
    status = {"prepared": "💡 ПОДГОТОВЛЕН", "active": "🚀 АКТИВИРОВАНА",
              "target": "✅ ОТРАБОТАНА В ПЛЮС", "stop": "🛑 ВЫБИТА ПО СТОПУ",
              "cancelled": "↩️ ОТМЕНЕНА ДО ВХОДА", "expired": "⌛ ВРЕМЯ ИСТЕКЛО"}[idea.status]
    side = "LONG ↑" if idea.direction > 0 else "SHORT ↓"
    lines = [f"{status} · <b>{idea.ticker} {side}</b>",
             f"Сетап: <b>{SETUP_NAMES.get(idea.setup, idea.setup)}</b> · оценка {idea.confidence}/100"]
    if idea.market_regime:
        tf = f"{idea.timeframe_score:+d}" if idea.timeframe_score else "0"
        liq = (f" · оборот ≈{idea.liquidity_rub_min / 1_000_000:.1f} млн ₽/мин"
               if idea.liquidity_rub_min else "")
        lines.append(f"Режим: <b>{idea.market_regime}</b> · TF {tf}/3{liq}")
    lines.append("")
    if idea.status == "prepared":
        lines += [f"🎯 Триггер: <b>{f(idea.trigger)} ₽</b>",
                  f"Вход: {f(idea.entry_lo)}–{f(idea.entry_hi)} ₽", f"🛑 Стоп/отмена: {f(idea.stop)} ₽"]
    else:
        lines += [f"Вход: <b>{f(idea.entry_price or idea.trigger)} ₽</b>", f"🛑 Стоп: {f(idea.stop)} ₽"]
    lines += [f"✅ Цель 1: {f(idea.target1)} ₽ (1,5R) · цель 2: {f(idea.target2)} ₽ (2,5R)"]
    lots, rub = position_size(idea, risk_rub)
    if lots:
        lines.append(f"💰 При риске {risk_rub:,.0f} ₽: до <b>{lots} лот.</b> (оценка риска {rub:,.0f} ₽)")
    if idea.reasons:
        lines += ["", "<b>Почему:</b>"] + [f"• {x}" for x in idea.reasons[:4]]
    if idea.cautions:
        lines += ["<b>Риски:</b> " + "; ".join(idea.cautions)]
    if idea.status in TERMINAL:
        lines += ["", f"Итог: <b>{idea.close_reason}</b>"]
        if idea.trigger_ts:
            lines.append(f"Результат: {idea.realized_r:+.2f}R · MFE {idea.mfe_pct:+.2f}% · MAE {idea.mae_pct:+.2f}%")
    elif idea.status == "prepared":
        mins = max(0, int((idea.expires_ts - time.time()) / 60))
        lines.append(f"<i>Не вход до касания триггера. Идея живёт ещё ≈{mins} мин.</i>")
    return "\n".join(lines)
