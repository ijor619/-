"""Read-only analytics for real broker executions.

The broker remains the source of truth.  This module only matches fills with
FIFO lots and formats statistics; it never places, changes or cancels orders.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from tinkoff import BrokerFill


@dataclass(frozen=True)
class ClosedTrade:
    account_id: str
    ticker: str
    direction: int
    quantity: float
    entry_price: float
    exit_price: float
    entry_ts: datetime
    exit_ts: datetime
    gross_pnl: float
    commission: float
    net_pnl: float
    entry_id: str
    exit_id: str


@dataclass(frozen=True)
class Performance:
    trades: tuple[ClosedTrade, ...]
    net_pnl: float
    gross_pnl: float
    commission: float
    wins: int
    losses: int
    breakeven: int
    win_rate: float
    profit_factor: Optional[float]
    average: float


@dataclass
class _Lot:
    sign: int
    quantity: float
    price: float
    commission_per_unit: float
    ts: datetime
    fill_id: str


def fifo_closed_trades(fills: Iterable[BrokerFill]) -> list[ClosedTrade]:
    """Match executions FIFO, including long and short positions.

    One exit execution can close several entry lots.  Those pieces are merged
    back into one result so the trade count is not inflated by partial fills.
    """
    books: dict[tuple[str, str], deque[_Lot]] = defaultdict(deque)
    pieces: list[ClosedTrade] = []
    for fill in sorted(fills, key=lambda x: (x.ts, x.account_id, x.fill_id)):
        if fill.quantity <= 0 or fill.price <= 0 or fill.side not in ("B", "S"):
            continue
        key = (fill.account_id, fill.ticker)
        book = books[key]
        sign = 1 if fill.side == "B" else -1
        remaining = float(fill.quantity)
        exit_fee_per_unit = abs(fill.commission) / fill.quantity if fill.quantity else 0.0
        while remaining > 1e-12 and book and book[0].sign != sign:
            lot = book[0]
            matched = min(remaining, lot.quantity)
            gross = (fill.price - lot.price) * matched * lot.sign
            fee = (lot.commission_per_unit + exit_fee_per_unit) * matched
            pieces.append(ClosedTrade(
                fill.account_id, fill.ticker, lot.sign, matched,
                lot.price, fill.price, lot.ts, fill.ts,
                gross, fee, gross - fee, lot.fill_id, fill.fill_id))
            remaining -= matched
            lot.quantity -= matched
            if lot.quantity <= 1e-12:
                book.popleft()
        if remaining > 1e-12:
            book.append(_Lot(sign, remaining, fill.price, exit_fee_per_unit,
                             fill.ts, fill.fill_id))

    # Merge pieces closed by the same execution and direction.
    grouped: dict[tuple[str, str, str, int], list[ClosedTrade]] = defaultdict(list)
    for row in pieces:
        grouped[(row.account_id, row.ticker, row.exit_id, row.direction)].append(row)
    out: list[ClosedTrade] = []
    for rows in grouped.values():
        qty = sum(x.quantity for x in rows)
        entry = sum(x.entry_price * x.quantity for x in rows) / qty
        gross = sum(x.gross_pnl for x in rows)
        fee = sum(x.commission for x in rows)
        first = rows[0]
        out.append(ClosedTrade(
            first.account_id, first.ticker, first.direction, qty, entry,
            first.exit_price, min(x.entry_ts for x in rows), first.exit_ts,
            gross, fee, gross - fee,
            ",".join(sorted({x.entry_id for x in rows})), first.exit_id))
    return sorted(out, key=lambda x: x.exit_ts)


def summarize(rows: Iterable[ClosedTrade]) -> Performance:
    trades = tuple(rows)
    net = sum(x.net_pnl for x in trades)
    gross = sum(x.gross_pnl for x in trades)
    fee = sum(x.commission for x in trades)
    wins = [x for x in trades if x.net_pnl > 0.005]
    losses = [x for x in trades if x.net_pnl < -0.005]
    flat = len(trades) - len(wins) - len(losses)
    win_sum = sum(x.net_pnl for x in wins)
    loss_sum = abs(sum(x.net_pnl for x in losses))
    return Performance(
        trades, net, gross, fee, len(wins), len(losses), flat,
        len(wins) / (len(wins) + len(losses)) * 100 if wins or losses else 0.0,
        win_sum / loss_sum if loss_sum else (None if not win_sum else float("inf")),
        net / len(trades) if trades else 0.0)


def closed_for_period(fills: Iterable[BrokerFill], days: int, ticker: str = "",
                      now: Optional[datetime] = None) -> list[ClosedTrade]:
    """Run FIFO on full history, then select exits in the requested window."""
    selected = [x for x in fills if not ticker or x.ticker == ticker]
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    cutoff = current - timedelta(days=max(1, min(int(days), 3650)))
    return [x for x in fifo_closed_trades(selected) if x.exit_ts >= cutoff]


def idea_matches(rows: Iterable[ClosedTrade], ideas: Iterable, uid: int) -> tuple[int, list[float]]:
    """Conservatively link real entries to delivered ideas by time and side."""
    matched = 0
    actual_r: list[float] = []
    used: set[str] = set()
    for trade in rows:
        ts = trade.entry_ts.timestamp()
        candidates = [idea for idea in ideas
                      if idea.id not in used
                      and idea.ticker == trade.ticker
                      and idea.direction == trade.direction
                      and idea.delivery(uid) is not None
                      and idea.created_ts - 15 * 60 <= ts <= idea.expires_ts + 15 * 60]
        if not candidates:
            continue
        idea = min(candidates, key=lambda x: abs(ts - (x.trigger_ts or x.created_ts)))
        used.add(idea.id)
        matched += 1
        risk = abs(idea.trigger - idea.stop) * trade.quantity
        if risk > 0:
            actual_r.append(trade.net_pnl / risk)
    return matched, actual_r


def _rub(value: float) -> str:
    return f"{value:+,.0f}".replace(",", " ") + " ₽"


def format_performance(report: Performance, days: int, ticker: str = "",
                       matched_ideas: int = 0,
                       actual_r: Optional[list[float]] = None) -> str:
    title = f"📒 <b>Реальные сделки · {days} дн.</b>" + (f" · {ticker}" if ticker else "")
    if not report.trades:
        return title + "\nЗакрытых сделок за период нет. Открытые позиции в результат не включаются."
    pf = "∞" if report.profit_factor == float("inf") else (
        f"{report.profit_factor:.2f}" if report.profit_factor is not None else "—")
    lines = [title,
             f"Закрыто: <b>{len(report.trades)}</b> · прибыльных {report.wins} · убыточных {report.losses}",
             f"Win rate: <b>{report.win_rate:.0f}%</b> · profit factor: <b>{pf}</b>",
             f"Результат после комиссий: <b>{_rub(report.net_pnl)}</b>",
             f"Комиссии в операциях: {abs(report.commission):,.0f} ₽".replace(",", " "),
             f"Средняя закрытая сделка: {_rub(report.average)}"]
    if matched_ideas:
        rtxt = (f" · средний факт {sum(actual_r) / len(actual_r):+.2f}R"
                if actual_r else "")
        lines.append(f"Совпало с идеями бота: <b>{matched_ideas}</b>{rtxt}")
    lines += ["", "<b>Последние закрытия:</b>"]
    for row in report.trades[-8:][::-1]:
        side = "L" if row.direction > 0 else "S"
        lines.append(f"• {row.exit_ts:%d.%m} {row.ticker} {side} · {_rub(row.net_pnl)}")
    lines.append("<i>FIFO-оценка по исполненным операциям; открытые позиции, налоги и дивиденды не входят.</i>")
    return "\n".join(lines)
