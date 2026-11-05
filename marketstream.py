"""Optional T-Invest gRPC stream with an automatic REST fallback.

The main bot does not depend on this module being available: if the official
SDK cannot be imported or the stream disconnects, FlowMonitor keeps using its
existing REST client.  The SDK manager reconnects and re-subscribes itself.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from datetime import timezone
from typing import Optional

from tinkoff import Level, OrderBook, TOKEN, Trade

log = logging.getLogger(__name__)

try:  # official current SDK, distributed from T-Bank's package registry
    from t_tech.invest import Client
    from t_tech.invest.grpc.schemas import (OrderBookInstrument, TradeDirection,
                                            TradeInstrument)
    SDK_AVAILABLE = True
except Exception:  # pragma: no cover - exercised on hosts without the optional SDK
    Client = OrderBookInstrument = TradeInstrument = TradeDirection = None
    SDK_AVAILABLE = False


def _q(value) -> float:
    return float(getattr(value, "units", 0) or 0) + float(getattr(value, "nano", 0) or 0) / 1e9


class MarketStream:
    def __init__(self) -> None:
        self.available = bool(SDK_AVAILABLE and TOKEN)
        self._lock = threading.RLock()
        self._wanted: dict[str, str] = {}       # ticker -> uid
        self._by_uid: dict[str, str] = {}
        self._trades: dict[str, deque] = defaultdict(lambda: deque(maxlen=50_000))
        self._books: dict[str, tuple[float, OrderBook]] = {}
        self._manager = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.last_error = ""
        self.connected_at = 0.0
        self._last_start = 0.0

    def ensure(self, ticker: str, uid: str) -> None:
        if not self.available or not uid:
            return
        with self._lock:
            new_subscription = self._wanted.get(ticker) != uid
            if new_subscription:
                self._wanted[ticker] = uid
                self._by_uid[uid] = ticker
            manager = self._manager
        if manager is not None and new_subscription:
            try:
                manager.order_book.subscribe([OrderBookInstrument(instrument_id=uid, depth=20)])
                manager.trades.subscribe([TradeInstrument(instrument_id=uid)])
            except Exception as e:
                self.last_error = str(e)
        if (self._thread is None or not self._thread.is_alive()) and time.time() - self._last_start >= 30:
            self._stop.clear()
            self._last_start = time.time()
            self._thread = threading.Thread(target=self._run, name="tbank-market-stream", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        manager = self._manager
        if manager is not None:
            try:
                manager.stop()
            except Exception:
                pass

    def _subscribe_all(self, manager) -> None:
        with self._lock:
            ids = list(dict.fromkeys(self._wanted.values()))
        if ids:
            manager.order_book.subscribe([OrderBookInstrument(instrument_id=x, depth=20) for x in ids])
            manager.trades.subscribe([TradeInstrument(instrument_id=x) for x in ids])

    def _run(self) -> None:  # blocking SDK lives in its own daemon thread
        try:
            with Client(TOKEN, app_name="moex-stockbot") as services:
                manager = services.create_market_data_stream()
                with self._lock:
                    self._manager = manager
                self._subscribe_all(manager)
                self.connected_at = time.time()
                for response in manager:
                    if self._stop.is_set():
                        break
                    if response.trade is not None:
                        self._on_trade(response.trade)
                    if response.orderbook is not None:
                        self._on_book(response.orderbook)
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            log.warning("market stream unavailable, REST fallback active: %s", self.last_error)
        finally:
            with self._lock:
                self._manager = None

    def _ticker(self, obj) -> str:
        ticker = str(getattr(obj, "ticker", "") or "").upper()
        uid = str(getattr(obj, "instrument_uid", "") or "")
        with self._lock:
            return ticker or self._by_uid.get(uid, "")

    def _on_trade(self, row) -> None:
        ticker = self._ticker(row)
        if not ticker or _q(row.price) <= 0:
            return
        direction = getattr(row, "direction", None)
        side = "B" if direction == TradeDirection.TRADE_DIRECTION_BUY else \
            "S" if direction == TradeDirection.TRADE_DIRECTION_SELL else ""
        if not side:  # never turn an unknown side into a sell
            return
        dt = row.time
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        trade = Trade(dt.astimezone(timezone.utc), _q(row.price), int(row.quantity), side)
        with self._lock:
            self._trades[ticker].append(trade)

    def _on_book(self, row) -> None:
        ticker = self._ticker(row)
        if not ticker or not bool(getattr(row, "is_consistent", True)):
            return
        bids = [Level(_q(x.price), int(x.quantity)) for x in row.bids if _q(x.price) > 0]
        asks = [Level(_q(x.price), int(x.quantity)) for x in row.asks if _q(x.price) > 0]
        if not bids or not asks:
            return
        dt = row.time
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        last = (bids[0].price + asks[0].price) / 2
        with self._lock:
            self._books[ticker] = (time.time(), OrderBook(dt.astimezone(timezone.utc), bids, asks, last))

    def snapshot(self, ticker: str, minutes: int = 10) -> tuple[list[Trade], Optional[OrderBook]]:
        now = time.time()
        cutoff = now - minutes * 60
        with self._lock:
            trades = [x for x in self._trades.get(ticker, ()) if x.ts.timestamp() >= cutoff]
            pair = self._books.get(ticker)
            book = pair[1] if pair and now - pair[0] <= 15 else None
        return trades, book

    def healthy(self) -> bool:
        with self._lock:
            fresh_book = any(time.time() - ts <= 15 for ts, _ in self._books.values())
        return bool(self.available and self._thread and self._thread.is_alive() and fresh_book)
