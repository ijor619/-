"""Paper strategy: confirmed long bounce from a known intraday low.

The strategy never buys a fresh low blindly. It waits for either a liquidity
sweep and reclaim, or a higher-timeframe uptrend pullback that has already
started bouncing from VWAP/a structural level. Results live in a separate
journal and never imply a broker order.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from ideas import (COMMISSION_PCT_PER_SIDE, Idea, IdeaContext,
                   IdeaEngine, IdeaJournal, TERMINAL,
                   idea_geometry_error, position_size)


BOUNCE_SETUP_NAMES = {
    "bounce_reclaim": "ложный вынос минимума",
    "bounce_trend": "отскок по восходящему тренду",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


BOUNCE_DAILY_LIMIT = max(1, _env_int("BOUNCE_DAILY_LIMIT", 5))
BOUNCE_COOLDOWN_SEC = max(15 * 60, _env_int("BOUNCE_COOLDOWN_MIN", 90) * 60)


@dataclass
class BounceContext:
    market: IdeaContext
    history: list[tuple[float, float]]  # unix timestamp, trade price


class BounceJournal(IdeaJournal):
    """Independent persistence and net-percent statistics for bounce trades."""

    def publication_slot(self, ticker: str, now: Optional[float] = None) -> bool:
        return self.published_today(now=now) < BOUNCE_DAILY_LIMIT

    def stats(self, uid: int, days: int = 30, ticker: str = "") -> str:
        days = max(1, min(int(days), 180))
        cutoff = time.time() - days * 86400
        rows = [x for x in self.ideas
                if x.created_ts >= cutoff
                and (not ticker or x.ticker == ticker)
                and x.delivery(uid) is not None]
        title = f"🌱 <b>Отскоки от минимума · {days} дн.</b>" + (f" · {ticker}" if ticker else "")
        if not rows:
            return title + "\nЗавершённых или активных бумажных входов ещё нет."
        triggered = [x for x in rows if x.trigger_ts]
        completed = [x for x in triggered if x.status in TERMINAL]
        wins = [x for x in completed if x.net_pct >= 0.005]
        losses = [x for x in completed if x.net_pct <= -0.005]
        flats = [x for x in completed if abs(x.net_pct) < 0.005]
        avg = sum(x.net_pct for x in completed) / len(completed) if completed else 0.0
        total = sum(x.net_pct for x in completed)
        gp = sum(x.net_pct for x in wins)
        gl = abs(sum(x.net_pct for x in losses))
        pf = f"{gp / gl:.2f}" if gl else "∞" if gp else "—"
        wr = len(wins) / (len(wins) + len(losses)) * 100 if wins or losses else 0.0
        lines = [title,
                 f"Входов: <b>{len(triggered)}</b> · сейчас активно: "
                 f"{sum(x.status == 'active' for x in triggered)}",
                 f"🟢 плюс: <b>{len(wins)}</b> · 🔴 минус: <b>{len(losses)}</b>"
                 + (f" · около нуля: {len(flats)}" if flats else ""),
                 f"Win rate: <b>{wr:.0f}%</b> · средний результат: <b>{avg:+.2f}%</b>",
                 f"Сумма процентов: <b>{total:+.2f}%</b> · Profit factor: <b>{pf}</b>", "",
                 "<pre>паттерн                    n   win   среднее"]
        for setup in BOUNCE_SETUP_NAMES:
            group = [x for x in completed if x.setup == setup]
            if not group:
                continue
            gw = sum(x.net_pct >= 0.005 for x in group)
            ga = sum(x.net_pct for x in group) / len(group)
            lines.append(f"{BOUNCE_SETUP_NAMES[setup][:25]:<25} {len(group):>3} {gw:>5} {ga:>+7.2f}%")
        lines.append("</pre>")
        lines.append("<b>Последние:</b>")
        labels = {"target": "цель", "stop": "стоп", "expired": "время",
                  "active": "активна", "prepared": "ждёт", "cancelled": "без входа"}
        for idea in sorted(rows, key=lambda x: x.created_ts, reverse=True)[:8]:
            result = (f" · {idea.net_pct:+.2f}%" if idea.trigger_ts and idea.status in TERMINAL
                      else "")
            lines.append(f"• {idea.ticker} · {labels.get(idea.status, idea.status)}{result}")
        lines.append(f"<i>Бумажная симуляция. Комиссия {COMMISSION_PCT_PER_SIDE:g}% учтена "
                     "на входе и выходе; проскальзывание не включено.</i>")
        return "\n".join(lines)


class BounceEngine:
    """Detect confirmed reclaims and trend-support bounces without look-ahead."""

    def __init__(self) -> None:
        self.cooldown: dict[tuple[str, str], float] = {}

    @staticmethod
    def _points(history: list[tuple[float, float]], start: float, end: float) -> list[tuple[float, float]]:
        return [(ts, price) for ts, price in history if start <= ts <= end and price > 0]

    @staticmethod
    def _support(low: float, ctx: IdeaContext) -> tuple[str, float, float]:
        refs = [("VWAP", ctx.vwap)] if ctx.vwap > 0 else []
        refs += [(name, price) for name, price in ctx.levels if price > 0]
        if not refs:
            return "", 0.0, 99.0
        name, price = min(refs, key=lambda row: abs(low - row[1]))
        return name, price, abs(low - price) / price * 100

    @staticmethod
    def _confirmed(ctx: IdeaContext) -> bool:
        flow = (ctx.buy_pct5 or 0) >= 58
        short_turn = (ctx.ch5 or 0) >= 0.05
        micro = ((ctx.book_imbalance or 0) >= 0.05 or
                 (ctx.microprice_edge_pct or 0) >= 0.005 or
                 (ctx.volume_mult or 0) >= 1.3)
        return flow and short_turn and micro

    @staticmethod
    def _safe_market(ctx: IdeaContext) -> bool:
        if not ctx.fresh or ctx.price <= 0:
            return False
        if ctx.spread_pct is None or ctx.spread_pct > 0.12:
            return False
        if ctx.turnover_rub_min is None or ctx.turnover_rub_min < 300_000:
            return False
        if (ctx.ch60 or 0) <= -0.45 or (ctx.market15 or 0) <= -0.35:
            return False
        if ctx.breadth is not None and ctx.breadth < 35:
            return False
        if ctx.event_risk or ctx.news_risk:
            return False
        return True

    def _make(self, ctx: IdeaContext, setup: str, local_low: float,
              reasons: list[str], confidence: int, minutes: int = 35) -> Optional[Idea]:
        spread = max(ctx.spread_pct or 0.02, 0.02)
        trigger = ctx.price * (1 + max(0.03, spread * 1.2) / 100)
        stop_buffer_pct = max(0.10, spread * 1.5)
        stop = local_low * (1 - stop_buffer_pct / 100)
        risk = trigger - stop
        risk_pct = risk / trigger * 100 if trigger else 0.0
        if risk <= 0 or not 0.20 <= risk_pct <= 1.50:
            return None

        resistances = sorted({price for _, price in ctx.levels if price > trigger}
                             | ({ctx.vwap} if ctx.vwap > trigger else set()))
        target1 = trigger + risk * 1.5
        if resistances:
            nearest = resistances[0]
            room_r = (nearest - trigger) / risk
            if room_r < 1.20:
                return None
            if room_r <= 2.20:
                target1 = nearest
        target2 = max(target1 + risk * 0.7, trigger + risk * 2.5)
        entry_lo, entry_hi = sorted((trigger - ctx.price * spread / 200,
                                     trigger + ctx.price * spread / 200))
        now = time.time()
        regime, _ = IdeaEngine.regime(ctx)
        tf_score, _ = IdeaEngine.timeframe_alignment(ctx, 1)
        idea = Idea(
            id=uuid4().hex[:12], ticker=ctx.ticker, direction=1, setup=setup,
            created_ts=now, updated_ts=now, expires_ts=now + minutes * 60,
            status="prepared", trigger=trigger, entry_lo=entry_lo, entry_hi=entry_hi,
            stop=stop, target1=target1, target2=target2,
            confidence=max(1, min(confidence, 89)), reasons=reasons,
            cautions=["бумажная стратегия: реальная заявка не выставляется"],
            level_name="локальный минимум", level=local_low,
            spread_pct=ctx.spread_pct or 0.0, lot=ctx.lot, decimals=ctx.decimals,
            market_regime=regime, timeframe_score=tf_score,
            liquidity_rub_min=ctx.turnover_rub_min or 0.0)
        return None if idea_geometry_error(idea, ctx.price) else idea

    def candidates(self, data: BounceContext) -> list[Idea]:
        ctx, history = data.market, sorted(data.history)
        if not self._safe_market(ctx) or not self._confirmed(ctx) or len(history) < 20:
            return []
        now = history[-1][0]
        price = ctx.price
        out: list[Idea] = []

        # 1) A known 45-minute low is swept during the last five minutes and
        # reclaimed. The prior window excludes recent prices, avoiding look-ahead.
        prior = self._points(history, now - 45 * 60, now - 5 * 60)
        recent = self._points(history, now - 5 * 60, now)
        if prior and recent:
            prior_low = min(price_ for _, price_ in prior)
            low_ts, sweep_low = min(recent, key=lambda row: row[1])
            sweep_pct = (prior_low - sweep_low) / prior_low * 100
            rebound_pct = (price / sweep_low - 1) * 100
            reclaimed = price >= prior_low * 1.0003
            age = now - low_ts
            if (0.03 <= sweep_pct <= 1.50 and 0.12 <= rebound_pct <= 1.20
                    and reclaimed and 30 <= age <= 8 * 60):
                reasons = [f"вынос минимума {prior_low:g} до {sweep_low:g}",
                           f"возврат выше уровня, отскок {rebound_pct:+.2f}%",
                           f"покупатели в потоке 5м: {ctx.buy_pct5:.0f}%"]
                idea = self._make(ctx, "bounce_reclaim", sweep_low, reasons,
                                  76 + (6 if (ctx.book_imbalance or 0) >= .08 else 0), 30)
                if idea:
                    out.append(idea)

        # 2) An established uptrend pulls into VWAP/volume support and has
        # already rebounded; no attempt is made to catch the exact bottom.
        trend_points = self._points(history, now - 25 * 60, now)
        if (ctx.ch60 or 0) >= 0.45 and (ctx.ch15 or 0) >= -0.30 and trend_points:
            low_ts, local_low = min(trend_points, key=lambda row: row[1])
            rebound_pct = (price / local_low - 1) * 100
            support_name, support, distance = self._support(local_low, ctx)
            age = now - low_ts
            if (support and distance <= 0.30 and 0.15 <= rebound_pct <= 0.90
                    and 60 <= age <= 18 * 60):
                reasons = [f"тренд 60м {ctx.ch60:+.2f}%",
                           f"минимум удержал {support_name} {support:g}",
                           f"подтверждённый отскок {rebound_pct:+.2f}%",
                           f"покупатели в потоке 5м: {ctx.buy_pct5:.0f}%"]
                idea = self._make(ctx, "bounce_trend", local_low, reasons,
                                  74 + (5 if (ctx.ch15 or 0) >= 0 else 0), 40)
                if idea:
                    out.append(idea)

        unique = []
        for idea in sorted(out, key=lambda x: -x.confidence):
            key = (idea.ticker, idea.setup)
            if now - self.cooldown.get(key, 0.0) < BOUNCE_COOLDOWN_SEC:
                continue
            self.cooldown[key] = now
            unique.append(idea)
            break  # one bounce scenario per instrument at a time
        return unique


def card(idea: Idea, risk_rub: float = 0.0) -> str:
    d = idea.decimals
    price = lambda value: f"{value:.{d}f}".replace(".", ",")
    status = {"prepared": "🌱 ГОТОВИТСЯ", "active": "🚀 ВХОД АКТИВИРОВАН",
              "target": "✅ ОТСКОК ОТРАБОТАН", "stop": "🛑 СТОП",
              "cancelled": "↩️ ОТМЕНЁН ДО ВХОДА", "expired": "⌛ ВЫХОД ПО ВРЕМЕНИ"}[idea.status]
    risk = abs((idea.entry_price or idea.trigger) - idea.stop)
    r1 = abs(idea.target1 - (idea.entry_price or idea.trigger)) / risk if risk else 0.0
    r2 = abs(idea.target2 - (idea.entry_price or idea.trigger)) / risk if risk else 0.0
    lines = [f"{status} · <b>{idea.ticker} LONG ↑</b>",
             f"Стратегия: <b>{BOUNCE_SETUP_NAMES.get(idea.setup, idea.setup)}</b> · "
             f"оценка {idea.confidence}/100", "",
             f"Локальный минимум: <b>{price(idea.level)} ₽</b>"]
    if idea.status == "prepared":
        lines += [f"Триггер подтверждения: <b>{price(idea.trigger)} ₽</b>",
                  f"Стоп/отмена: {price(idea.stop)} ₽"]
    else:
        lines += [f"Виртуальный вход: <b>{price(idea.entry_price or idea.trigger)} ₽</b>",
                  f"Стоп: {price(idea.stop)} ₽"]
    lines += [f"Цель 1: {price(idea.target1)} ₽ ({r1:.1f}R до комиссии) · "
              f"цель 2: {price(idea.target2)} ₽ ({r2:.1f}R)"]
    lots, rub = position_size(idea, risk_rub)
    if lots:
        lines.append(f"При риске {risk_rub:,.0f} ₽: до <b>{lots} лот.</b> "
                     f"(стоп + комиссия ≈{rub:,.0f} ₽)")
    lines += ["", "<b>Подтверждения:</b>"] + [f"• {reason}" for reason in idea.reasons[:4]]
    if idea.status in TERMINAL:
        lines += ["", f"Итог: <b>{idea.close_reason}</b>"]
        if idea.trigger_ts:
            lines.append(f"Результат после комиссии: <b>{idea.net_pct:+.2f}%</b> · "
                         f"MFE {idea.mfe_pct:+.2f}% · MAE {idea.mae_pct:+.2f}%")
    else:
        lines.append("<i>Бумажная сделка: бот не отправляет заявку брокеру.</i>")
    return "\n".join(lines)
