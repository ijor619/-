"""Actionable trade ideas with an explicit, auditable life cycle.

Raw tape/order-book events are deliberately not ideas.  An idea is created
only when several independent groups agree: price structure, order flow and
market context.  It then moves through prepared -> active -> target/stop or
is cancelled/expired before entry.
"""
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Optional
from uuid import uuid4


TERMINAL = {"target", "stop", "cancelled", "expired"}
ACTIVE = {"prepared", "active"}
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
    deliveries: list[Delivery] = field(default_factory=list)

    @property
    def side(self) -> str:
        return "лонг" if self.direction > 0 else "шорт"

    def delivery(self, uid: int) -> Optional[Delivery]:
        return next((d for d in self.deliveries if d.uid == uid), None)


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
            for row in raw.get("ideas", []):
                row["deliveries"] = [Delivery(**d) for d in row.get("deliveries", [])]
                self.ideas.append(Idea(**row))
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
        lines.append("<pre>сетап                 n  вход  цель/стоп")
        for (setup, direction), grp in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            tr = [x for x in grp if x.trigger_ts]
            w = sum(x.status == "target" for x in tr)
            s = sum(x.status == "stop" for x in tr)
            name = SETUP_NAMES.get(setup, setup)[:17]
            side = "L" if direction > 0 else "S"
            lines.append(f"{name:<17} {side} {len(grp):>3} {len(tr):>5} {w:>3}/{s:<3}")
        lines.append("</pre>")
        taken = [x for x in xs if (x.delivery(uid) and x.delivery(uid).decision == "taken")]
        skipped = [x for x in xs if (x.delivery(uid) and x.delivery(uid).decision == "skipped")]
        lines.append(f"👆 Отметки: «взял» {len(taken)}, «пропустил» {len(skipped)}.")
        lines.append("<i>Точность считается только по идеям, где цена сначала дошла до триггера; отмены до входа не портят win rate.</i>")
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
            direction = 1 if ctx.prev_price < lv <= ctx.price else -1 if ctx.prev_price > lv >= ctx.price else 0
            if direction:
                self.probes[(ctx.ticker, round(lv, 6))] = BreakProbe(ctx.ticker, name, lv, direction, now)

        for key, pr in list(self.probes.items()):
            age = now - pr.ts
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
             f"Сетап: <b>{SETUP_NAMES.get(idea.setup, idea.setup)}</b> · оценка {idea.confidence}/100", ""]
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
