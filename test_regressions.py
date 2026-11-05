"""Offline regressions: no Telegram requests, broker account or real orders."""
import asyncio
import json
import tempfile
import time
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import clusters
import flow
import journal
import moex
import tape
import tinkoff
from monitor import Monitor
from screener import Sec
from store import Store, UserProfile

UTC = timezone.utc


class TempCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.now = datetime.now(UTC).replace(second=30, microsecond=0)

    def trade(self, price=100, seconds=0, qty=5, side="B"):
        return tinkoff.Trade(self.now + timedelta(seconds=seconds), price, qty, side)


class DirectionTests(TempCase):
    def test_sell_whale_with_buy_context_is_short(self):
        trades = [self.trade(seconds=-20 + i, qty=1) for i in range(6)]
        trades += [self.trade(qty=60_000, side="S")]
        sig = tape.whale_signals("RUAL", trades, 1, 2, None)[0]
        self.assertIn("покупки", sig.text)
        self.assertEqual(sig.direction, -1)
        self.assertEqual(flow._direction(sig), -1)

    def test_text_cannot_override_direction(self):
        for kind in ("whale", "rhythm", "imbalance", "wall", "spoof", "eaten", "breakout"):
            sig = tape.Signal(kind, "X", "покупки вверх бид", "key", direction=-1)
            self.assertEqual(flow._direction(sig), -1)
        self.assertEqual(flow._direction(tape.Signal("wall", "X", "покупки", "k")), 0)

    def test_sell_iceberg_rhythm_and_imbalance(self):
        trades = [self.trade(seconds=-120 + i * 2, side="S") for i in range(60)]
        sigs = tape.analyze_trades("X", trades, 1, 2)
        selected = [s for s in sigs if s.kind in ("iceberg", "rhythm", "imbalance")]
        self.assertEqual({s.kind for s in selected}, {"iceberg", "rhythm", "imbalance"})
        self.assertTrue(all(s.direction == -1 for s in selected))

    def test_bid_and_ask_walls(self):
        levels = [tinkoff.Level(100 + i, 2000 if i == 0 else 10) for i in range(10)]
        self.assertEqual(tape._wall(levels, "B", "X", 2, 1).direction, 1)
        self.assertEqual(tape._wall(levels, "S", "X", 2, 1).direction, -1)


class ClusterTests(TempCase):
    def store(self):
        return clusters.ClusterStore(str(self.path / "clusters.json"))

    def test_replay_after_restart_does_not_add_volume(self):
        cs = self.store()
        trades = [self.trade(seconds=-i) for i in range(30)]
        self.assertEqual(cs.ingest("X", trades), 30)
        cs.save(force=True)
        cs = self.store()
        self.assertEqual(cs.ingest("X", trades), 0)
        self.assertEqual(sum(c.n for c in cs.window("X", 60)), 30)
        self.assertEqual(sum(c.buy for c in cs.window("X", 60)), 150)

    def test_equal_timestamp_multiplicity_survives_restart(self):
        cs = self.store()
        tr = self.trade()
        self.assertEqual(cs.ingest("X", [tr, tr]), 2)
        cs.save(force=True)
        cs = self.store()
        self.assertEqual(cs.ingest("X", [tr, tr, tr]), 1)
        self.assertEqual(cs.ingest("X", [tr, tr, tr]), 0)

    def test_hour_backfill_after_short_poll(self):
        cs = self.store()
        self.assertEqual(cs.ingest("X", [self.trade()]), 1)
        self.assertEqual(cs.ingest("X", [self.trade(seconds=-1800), self.trade()]), 1)
        self.assertEqual(sum(c.n for c in cs.window("X", 60)), 2)

    def test_falling_price_in_same_minute_keeps_chronology(self):
        cs = self.store()
        cs.ingest("X", [self.trade(110, -10), self.trade(100)])
        cells = cs.window("X", 5)
        self.assertEqual(clusters.price_endpoints(cells), (110, 100))
        a = clusters.analyze("X", "5m", cells, 1, 2)
        self.assertEqual((a.first, a.last), (110, 100))
        monitor = flow.FlowMonitor(None, None, None, None, cs)
        self.assertAlmostEqual(monitor.price_change("X", 5), -100 / 11)

    def test_legacy_aggregate_backed_up_and_not_reused(self):
        path = self.path / "clusters.json"
        raw = {"X": {"2026-11-05T12:00": {"100": [10, 0, 2]}}}
        path.write_text(json.dumps(raw))
        cs = self.store()
        self.assertEqual(cs.data, {})
        self.assertEqual(json.loads(Path(str(path) + ".legacy-v1.bak").read_text()), raw)
        cs.ingest("X", [self.trade()])
        cs.save(force=True)
        self.assertEqual(json.loads(path.read_text())["version"], 2)

    def test_expired_fingerprints_do_not_allow_replays(self):
        cs = self.store()
        old = self.trade(seconds=-4000)
        cs.ingest("X", [old])
        cs.ingest("X", [self.trade()])
        cs.save(force=True)
        cs = self.store()
        self.assertEqual(cs.ingest("X", [old]), 0)

    def test_ambiguous_timestamp_does_not_invent_price_direction(self):
        cs = self.store()
        cs.ingest("X", [self.trade(110), self.trade(100)])
        self.assertEqual(clusters.price_endpoints(cs.window("X", 5)), (0, 0))

    def test_corrupt_file_is_not_overwritten(self):
        p = self.path / "clusters.json"
        p.write_text("{broken")
        with self.assertRaises(json.JSONDecodeError):
            self.store()
        self.assertEqual(p.read_text(), "{broken")


class JournalTests(TempCase):
    def entry(self, **kw):
        data = dict(ts=time.time(), uid=1, chat_id=1, msg_id=2, ticker="X", kind="whale",
                    price=100, direction=-1, event_id="event", signal_key="whale:key")
        data.update(kw)
        return journal.Entry(**data)

    def store(self):
        return journal.Journal(str(self.path / "signals.json"))

    def test_multiple_recipients_are_one_quality_observation(self):
        j = self.store()
        for uid in range(40):
            j.add(self.entry(uid=uid, results={"15": 1.0}))
        self.assertEqual(j.quality("X", "whale"), (1, 0.0))
        self.assertFalse(j.is_muted("X", "whale"))

    def test_duplicate_delivery_and_reload(self):
        j = self.store()
        j.add(self.entry())
        j.add(self.entry())
        self.assertEqual(len(self.store().entries), 1)
        self.assertTrue(self.store().recently_sent(1, "X", "whale", "different", time.time(), 600, 1800))

    def test_short_profit_and_missing_results(self):
        j = self.store()
        j.add(self.entry(results={"15": -1.0}))
        j.add(self.entry(event_id="missing", missing=["15"]))
        self.assertEqual(j.quality("X", "whale"), (1, 100.0))
        text = j.stats(1)
        self.assertIn("+1.00%", text)
        self.assertIn("1/2", text)

    def test_legacy_journal_is_preserved_but_excluded(self):
        p = self.path / "signals.json"
        old = [asdict(self.entry(results={"15": 1.0}))]
        p.write_text(json.dumps(old))
        j = self.store()
        self.assertEqual(j.entries, [])
        self.assertEqual(json.loads(Path(str(p) + ".legacy-v1.bak").read_text()), old)
        self.assertEqual(json.loads(p.read_text())["version"], 2)


class FollowupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.j = journal.Journal(str(Path(self.tmp.name) / "j.json"))
        self.bot = SimpleNamespace(edit_message_text=AsyncMock(), send_message=AsyncMock())
        self.monitor = flow.FlowMonitor(self.bot, None, None, self.j)
        self.start = time.time() - 1000

    def entry(self, **kw):
        data = dict(ts=self.start, uid=1, chat_id=1, msg_id=1, ticker="X", kind="whale",
                    price=100, direction=-1, event_id="event", price_ts=self.start)
        data.update(kw)
        e = journal.Entry(**data)
        self.j.add(e)
        return e

    async def test_distinct_target_prices_not_latest_price(self):
        e = self.entry()
        self.monitor._prices["X"] = {self.start + 299: 99, self.start + 899: 98, self.start + 999: 120}
        self.monitor._last_price["X"] = 120
        self.monitor._observed_until["X"] = time.time()
        await self.monitor.followups()
        self.assertEqual(e.results, {"5": -1.0, "15": -2.0})
        self.assertTrue(e.done)
        self.assertIn("✅", self.bot.edit_message_text.call_args.args[0])

    async def test_missing_checkpoints_are_not_fabricated_after_restart(self):
        e = self.entry()
        self.monitor._last_price["X"] = 150
        self.monitor._prices["X"] = {time.time(): 150}
        self.monitor._observed_until["X"] = time.time()
        await self.monitor.followups()
        self.assertEqual(e.results, {})
        self.assertEqual(e.missing, ["5", "15"])
        self.assertTrue(e.done)

    async def test_stale_price_or_uncovered_target_is_not_a_measurement(self):
        e = self.entry()
        self.monitor._prices["X"] = {self.start + 239: 100, self.start + 899: 110}
        self.monitor._observed_until["X"] = self.start + 899
        await self.monitor.followups()
        self.assertEqual(e.results, {})

    async def test_each_signal_in_digest_uses_own_direction(self):
        self.entry()
        self.entry(event_id="opposite", direction=1, kind="rhythm")
        self.monitor._prices["X"] = {self.start + 299: 99, self.start + 899: 98}
        self.monitor._observed_until["X"] = time.time()
        await self.monitor.followups()
        text = self.bot.edit_message_text.call_args.args[0]
        self.assertIn("кит → 5м: -1,00% ✅", text)
        self.assertIn("ритм → 5м: -1,00% ❌", text)

    async def test_telegram_edit_failure_does_not_lose_results(self):
        e = self.entry()
        self.bot.edit_message_text.side_effect = RuntimeError("offline")
        self.monitor._prices["X"] = {self.start + 299: 99, self.start + 899: 98}
        self.monitor._observed_until["X"] = time.time()
        await self.monitor.followups()
        j = journal.Journal(self.j.path)
        self.assertEqual(j.entries[0].results, e.results)
        self.assertEqual(self.j.quality("X", "whale"), (1, 100))

    async def test_delivery_to_two_users_keeps_event_baseline_and_one_recent_fact(self):
        mon = self.monitor
        mon._info = AsyncMock(return_value=None)
        mon._daystats = AsyncMock(return_value=flow.DayStats())
        mon.imoex = AsyncMock(return_value={})
        mon._last_price["X"] = 120  # цена уже изменилась за время дайджеста
        self.bot.send_message.return_value = SimpleNamespace(chat=SimpleNamespace(id=1), message_id=5)
        sig = tape.Signal("whale", "X", "продажа, контекст покупки", "key", direction=-1,
                          detected_at=self.start, price=100, price_ts=self.start)
        await mon._send_signals(None, 1, "X", [sig])
        await mon._send_signals(None, 2, "X", [sig])
        self.assertEqual(len(self.j.entries), 2)
        self.assertEqual(len(mon._recent["X"]), 1)
        self.assertTrue(all(e.price == 100 and e.ts == self.start and e.direction == -1 for e in self.j.entries))
        self.assertEqual(self.j.entries[0].event_id, self.j.entries[1].event_id)


class ApiTests(unittest.IsolatedAsyncioTestCase):
    def inst(self):
        return tinkoff.Instrument("RUAL", "uid", "figi", 10, "РУСАЛ")

    def candle(self, ts, price, complete=True):
        return {"time": ts, "close": {"units": str(int(price)), "nano": round((price % 1) * 1e9)}, "isComplete": complete}

    async def test_0400_reference_excludes_later_and_incomplete_candles(self):
        tk = tinkoff.TinkoffClient()
        tk._call = AsyncMock(return_value={"candles": [
            self.candle("2026-11-05T00:59:00Z", 23.085),
            self.candle("2026-11-05T01:00:00Z", 30),
            self.candle("2026-11-05T00:59:30Z", 31, False)]})
        now = datetime(2026, 11, 5, 18, tzinfo=UTC)
        ref = await tk.daily_reference(self.inst(), now)
        self.assertAlmostEqual(ref.price, 23.085)
        self.assertEqual(ref.cutoff.hour, 4)
        body = tk._call.call_args.args[1]
        self.assertEqual(datetime.fromisoformat(body["to"]), ref.cutoff)
        self.assertEqual(body["candleSourceType"], "CANDLE_SOURCE_EXCHANGE")
        await tk.daily_reference(self.inst(), now)
        self.assertEqual(tk._call.await_count, 1)

    async def test_holiday_search_and_0400_rollover(self):
        tk = tinkoff.TinkoffClient()
        tk._call = AsyncMock(side_effect=[{"candles": []}, {"candles": [self.candle("2026-11-03T20:00:00Z", 23.085)]}])
        ref = await tk.daily_reference(self.inst(), datetime(2026, 11, 5, 18, tzinfo=UTC))
        self.assertEqual(ref.candle_time.day, 3)
        tk._call = AsyncMock(return_value={"candles": [self.candle("2026-11-05T20:00:00Z", 24)]})
        ref2 = await tk.daily_reference(self.inst(), datetime(2026, 11, 6, 2, tzinfo=UTC))
        self.assertEqual(ref2.cutoff.day, 6)
        self.assertEqual(ref2.price, 24)

    async def test_before_0400_uses_previous_boundary(self):
        tk = tinkoff.TinkoffClient()
        tk._call = AsyncMock(return_value={"candles": [self.candle("2026-11-04T00:59:00Z", 23)]})
        ref = await tk.daily_reference(self.inst(), datetime(2026, 11, 5, 0, tzinfo=UTC))
        self.assertEqual(ref.cutoff.day, 4)

    async def test_unknown_trade_side_is_not_a_sell(self):
        tk = tinkoff.TinkoffClient()
        trade = {"time": "2026-11-05T12:00:00Z", "price": {"units": "100"}, "quantity": "5"}
        tk._call = AsyncMock(return_value={"trades": [trade, dict(trade, direction="TRADE_DIRECTION_SELL")]})
        trades = await tk.last_trades(self.inst())
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].side, "S")

    async def test_orderbook_timestamp_comes_from_api(self):
        tk = tinkoff.TinkoffClient()
        tk._call = AsyncMock(return_value={"orderbookTs": "2026-11-05T12:00:00Z", "bids": [{"price": {"units": "100"}, "quantity": "10"}]})
        ob = await tk.order_book(self.inst())
        self.assertEqual(ob.ts, datetime(2026, 11, 5, 12, tzinfo=UTC))

    async def test_quote_screenshot_math_and_explicit_basis(self):
        tk = tinkoff.TinkoffClient()
        tk.instrument = AsyncMock(return_value=self.inst())
        tk.last_prices = AsyncMock(return_value={"RUAL": (23.88, datetime.now(UTC))})
        tk.daily_reference = AsyncMock(return_value=tinkoff.DayReference(23.085, datetime(2026, 11, 5, 4, tzinfo=moex.MSK), datetime(2026, 11, 3, 20, tzinfo=UTC)))
        info = moex.SecurityInfo("RUAL", "РУСАЛ", "TQBR", "SUR", 3, 23.27, "2026-11-03")
        q = (await moex.fetch_quotes(None, {"RUAL": info}, {}, tk))["RUAL"]
        self.assertAlmostEqual(q.change_pct, 3.4437946740, places=6)
        self.assertEqual(info.prev_close, 23.27)
        self.assertIn("04:00", q.details())
        self.assertIn("цена на", q.details())

    async def test_reference_failure_keeps_live_price_and_labels_fallback(self):
        tk = tinkoff.TinkoffClient()
        tk.instrument = AsyncMock(return_value=self.inst())
        tk.last_prices = AsyncMock(return_value={"RUAL": (23.955, datetime.now(UTC))})
        tk.daily_reference = AsyncMock(side_effect=RuntimeError("offline"))
        info = moex.SecurityInfo("RUAL", "РУСАЛ", "TQBR", "SUR", 3, 23.27, "2026-11-03")
        q = (await moex.fetch_quotes(None, {"RUAL": info}, {}, tk))["RUAL"]
        self.assertAlmostEqual(q.change_pct, 2.9437043403)
        self.assertIn("база 04:00 недоступна", q.details())
        self.assertIn("T‑Invest", q.source)


class MonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_cooldown_and_failed_send_do_not_consume_crossing(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(str(Path(tmp) / "users.json"))
            prof = store.get(1)
            prof.watchlist, prof.report_min, prof.threshold_pct = ["X"], 0, 1
            prof.cooldown_min, prof.quiet_from = 1, -1
            bot = SimpleNamespace(send_message=AsyncMock())
            mon = Monitor(bot, store)
            mon._load_infos = AsyncMock(return_value={"X": moex.SecurityInfo("X", "X", "TQBR", "SUR", 2, 100, "today")})
            q = moex.Quote(mon._load_infos.return_value["X"], 101, moex.now_msk(), 1.1, True, reference_key="base")
            with patch("monitor.moex.fetch_quotes", AsyncMock(return_value={"X": q})):
                await mon.tick(None)
                q.change_pct = 2.1
                mon._last_alert[(1, "X")] = time.time()
                await mon.tick(None)
                self.assertEqual(mon._step[(1, "X")][1], 1)
                mon._last_alert[(1, "X")] = 0
                bot.send_message.side_effect = RuntimeError("offline")
                await mon.tick(None)
                self.assertEqual(mon._step[(1, "X")][1], 1)
                bot.send_message.side_effect = None
                await mon.tick(None)
                self.assertEqual(mon._step[(1, "X")][1], 2)
                q.reference_key, q.change_pct = "other-base", 5.1
                calls = bot.send_message.await_count
                await mon.tick(None)
                self.assertEqual(bot.send_message.await_count, calls)

    async def test_negative_recovery_says_above(self):
        info = moex.SecurityInfo("X", "X", "TQBR", "SUR", 2, 100, "today")
        q = moex.Quote(info, 97.5, moex.now_msk(), -2.5, True)
        p = UserProfile()
        p.threshold_pct = 1
        self.assertIn("выше -3%", Monitor._alert_text(q, p, -2, -3))

    async def test_stale_screener_price_is_not_refreshed_by_poll_time(self):
        s = Sec("X", "X", "uid", 100, 1e8, last=110, last_ts=time.time() - 200)
        self.assertIsNone(s.change("1d"))
        s.hist.append((time.time() - 86400, 100))
        self.assertIsNone(s.price_at(900))


if __name__ == "__main__":
    unittest.main()
