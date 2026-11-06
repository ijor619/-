"""Offline regressions: no Telegram requests, broker account or real orders."""
import asyncio
import json
import os
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
import ideas
import keyboards
import moex
import tape
import tinkoff
from monitor import Monitor
from screener import Row, Sec, Screener
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

    def test_sell_flow_without_price_response_is_absorbed_by_buyer(self):
        trades = [self.trade(seconds=-120 + i * 2, side="S") for i in range(60)]
        sigs = tape.analyze_trades("X", trades, 1, 2)
        selected = [s for s in sigs if s.kind in ("iceberg", "rhythm", "imbalance")]
        self.assertEqual({s.kind for s in selected}, {"iceberg", "rhythm", "imbalance"})
        by_kind = {s.kind: s for s in selected}
        self.assertEqual(by_kind["iceberg"].direction, -1)
        self.assertEqual(by_kind["rhythm"].direction, -1)
        self.assertEqual(by_kind["imbalance"].direction, 1)
        self.assertIn("поглощается", by_kind["imbalance"].text)

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

    def test_legacy_aggregate_is_preserved_but_not_replayed(self):
        path = self.path / "clusters.json"
        minute = self.now.astimezone(clusters.MSK).strftime("%Y-%m-%dT%H:%M")
        raw = {"X": {minute: {"100": [10, 0, 2]}}}
        path.write_text(json.dumps(raw))
        cs = self.store()
        self.assertEqual(sum(c.n for c in cs.window("X", 60)), 2)
        self.assertEqual(cs.ingest("X", [self.trade(100, seconds=-10)]), 0)
        self.assertEqual(json.loads(Path(str(path) + ".legacy-v1.bak").read_text()), raw)
        cs.save(force=True)
        self.assertEqual(json.loads(path.read_text())["version"], 2)

    def test_mixed_legacy_and_new_cells_do_not_invent_price_change(self):
        old = clusters.Cell(self.now.astimezone(clusters.MSK).replace(tzinfo=None), 100, 10, 0, 2)
        new = clusters.Cell(old.minute + timedelta(minutes=1), 110, 5, 0, 1,
                            self.now.timestamp(), self.now.timestamp())
        self.assertEqual(clusters.price_endpoints([old, new]), (0, 0))

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

    async def test_orderbook_without_timestamp_still_opens(self):
        tk = tinkoff.TinkoffClient()
        tk._call = AsyncMock(return_value={
            "bids": [{"price": {"units": "100"}, "quantity": "10"}],
            "asks": [{"price": {"units": "101"}, "quantity": "7"}]})
        before = datetime.now(UTC)
        ob = await tk.order_book(self.inst())
        self.assertIsNotNone(ob)
        self.assertGreaterEqual(ob.ts, before)

    async def test_quote_screenshot_math_and_explicit_basis(self):
        tk = tinkoff.TinkoffClient()
        tk.instrument = AsyncMock(return_value=self.inst())
        tk.last_prices = AsyncMock(return_value={"RUAL": (23.88, datetime.now(UTC))})
        tk.portfolio_day_quotes = AsyncMock(return_value={
            "RUAL": tinkoff.PortfolioDayQuote(23.88, 3.4437946740, 23.085, 5800, 1)})
        tk.close_prices = AsyncMock(return_value={})
        info = moex.SecurityInfo("RUAL", "РУСАЛ", "TQBR", "SUR", 3, 23.27, "2026-11-03")
        q = (await moex.fetch_quotes(None, {"RUAL": info}, {}, tk))["RUAL"]
        self.assertAlmostEqual(q.change_pct, 3.4437946740, places=6)
        self.assertEqual(q.price, 23.88)
        self.assertEqual(q.reference_kind, "portfolio")
        self.assertEqual(info.prev_close, 23.27)
        self.assertIn("позиция T‑Invest", q.details())
        self.assertIn("цена на", q.details())

    async def test_portfolio_failure_uses_official_close_not_candle_guess(self):
        tk = tinkoff.TinkoffClient()
        tk.instrument = AsyncMock(return_value=self.inst())
        tk.last_prices = AsyncMock(return_value={"RUAL": (23.88, datetime.now(UTC))})
        tk.portfolio_day_quotes = AsyncMock(side_effect=RuntimeError("offline"))
        tk.close_prices = AsyncMock(return_value={
            "RUAL": (24.0, datetime(2026, 11, 4, 20, tzinfo=UTC), "закрытие вечерней сессии T‑Invest")})
        info = moex.SecurityInfo("RUAL", "РУСАЛ", "TQBR", "SUR", 3, 23.27, "2026-11-03")
        q = (await moex.fetch_quotes(None, {"RUAL": info}, {}, tk))["RUAL"]
        self.assertAlmostEqual(q.change_pct, -0.5)
        self.assertEqual(q.reference_price, 24.0)
        self.assertEqual(q.reference_kind, "tinvest_close")
        self.assertNotIn("04:00", q.details())

    async def test_portfolio_daily_yield_reproduces_plzl_minus_half_percent(self):
        tk = tinkoff.TinkoffClient()
        response = {"positions": [{
            "ticker": "PLZL", "quantity": {"units": "100"},
            "currentPrice": {"units": "1067", "nano": 200_000_000},
            "dailyYield": {"units": "-536"}}]}
        tk._call = AsyncMock(return_value=response)
        with patch.object(tinkoff.config, "TINKOFF_ACCOUNT_ID", "account-1"):
            result = await tk.portfolio_day_quotes()
        quote = result["PLZL"]
        self.assertEqual(quote.price, 1067.2)
        self.assertAlmostEqual(quote.base_price, 1072.56)
        self.assertAlmostEqual(quote.change_pct, -0.49973894, places=6)
        method, body = tk._call.call_args.args
        self.assertEqual(method, "OperationsService/GetPortfolio")
        self.assertEqual(body, {"accountId": "account-1"})

    async def test_portfolio_aggregates_same_ticker_across_accounts(self):
        tk = tinkoff.TinkoffClient()
        tk._account_ids = AsyncMock(return_value=["a", "b"])
        one = {"positions": [{"ticker": "PLZL", "quantity": {"units": "50"},
                               "currentPrice": {"units": "1067", "nano": 200_000_000},
                               "dailyYield": {"units": "-268"}}]}
        tk._call = AsyncMock(side_effect=[one, one])
        quote = (await tk.portfolio_day_quotes())["PLZL"]
        self.assertEqual(quote.quantity, 100)
        self.assertEqual(quote.account_count, 2)
        self.assertAlmostEqual(quote.base_price, 1072.56)

    async def test_portfolio_rejects_partial_account_result(self):
        tk = tinkoff.TinkoffClient()
        tk._account_ids = AsyncMock(return_value=["a", "b"])
        one = {"positions": [{"ticker": "PLZL", "quantity": {"units": "50"},
                               "currentPrice": {"units": "1067"},
                               "dailyYield": {"units": "-250"}}]}
        tk._call = AsyncMock(side_effect=[one, RuntimeError("account unavailable")])
        with self.assertRaisesRegex(RuntimeError, "1 счёта"):
            await tk.portfolio_day_quotes()

    async def test_candle_guess_is_never_used_for_portfolio_percentage(self):
        tk = tinkoff.TinkoffClient()
        tk.instrument = AsyncMock(return_value=self.inst())
        tk.last_prices = AsyncMock(return_value={"RUAL": (1067.2, datetime.now(UTC))})
        tk.portfolio_day_quotes = AsyncMock(return_value={
            "RUAL": tinkoff.PortfolioDayQuote(1067.2, -0.5, 1072.56, 100, 1)})
        tk.close_prices = AsyncMock(return_value={})
        info = moex.SecurityInfo("RUAL", "РУСАЛ", "TQBR", "SUR", 1, 1031.6, "2026-11-03")
        q = (await moex.fetch_quotes(None, {"RUAL": info}, {}, tk))["RUAL"]
        self.assertEqual(q.change_pct, -0.5)

    async def test_get_close_prices_prefers_later_evening_session(self):
        tk = tinkoff.TinkoffClient()
        tk._call = AsyncMock(return_value={"closePrices": [{
            "instrumentUid": "uid", "price": {"units": "1050"},
            "time": "2026-11-04T15:45:00Z",
            "eveningSessionPrice": {"units": "1072", "nano": 600_000_000},
            "eveningSessionPriceTime": "2026-11-04T20:49:00Z"}]})
        price, ts, label = (await tk.close_prices([self.inst()]))["RUAL"]
        self.assertEqual(price, 1072.6)
        self.assertEqual(ts, datetime(2026, 11, 4, 20, 49, tzinfo=UTC))
        self.assertIn("вечерней", label)
        await tk.close_prices([self.inst()])
        self.assertEqual(tk._call.await_count, 1)

    async def test_empty_hint_range_continues_searching_older_moex_candle(self):
        row = [100, 101, 102, 99, 1000, 10,
               "2026-11-04 18:49:00", "2026-11-04 18:50:00"]
        with patch("moex._candles", AsyncMock(side_effect=[[], [row]])) as candles:
            got = await moex.get_last_candle(None, "X", "TQBR", moex.now_msk())
        self.assertEqual(got, row)
        self.assertEqual(candles.await_count, 2)


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

    async def test_crossing_from_plus_to_minus_keeps_real_direction(self):
        info = moex.SecurityInfo("X", "X", "TQBR", "SUR", 2, 100, "today")
        p = UserProfile()
        p.threshold_pct = 1
        down = moex.Quote(info, 99, moex.now_msk(), -1.1, True)
        up = moex.Quote(info, 101, moex.now_msk(), 1.1, True)
        self.assertIn("прошла -1%", Monitor._alert_text(down, p, -1, 1))
        self.assertIn("прошла +1%", Monitor._alert_text(up, p, 1, -1))

    async def test_closed_market_screener_keeps_last_real_snapshot(self):
        end = time.time() - 3600
        s = Sec("X", "X", "uid", 100, 1e8, last=110, last_ts=end)
        for minutes in range(300, -1, -5):
            s.hist.append((end - minutes * 60, 100 + (300 - minutes) / 300 * 10))
        self.assertAlmostEqual(s.change("1d"), 10)
        self.assertIsNotNone(s.change("15m"))
        self.assertIsNotNone(s.change("30m"))
        self.assertIsNotNone(s.change("1h"))
        self.assertIsNotNone(s.change("4h"))

    async def test_screener_warmup_restores_last_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            tk = tinkoff.TinkoffClient()
            begin = datetime.now(moex.MSK).replace(tzinfo=None) - timedelta(minutes=10)
            tk.candles = AsyncMock(return_value=[
                tinkoff.TCandle(begin, 99, 101, 98, 100, 5, True)])
            scr = Screener(tk, tmp)
            sec = Sec("X", "X", "uid", 90, 1e8)
            await scr._warm_up([sec])
            self.assertEqual(sec.last, 100)
            self.assertGreater(sec.last_ts, 0)


class ButtonTests(unittest.TestCase):
    def test_every_generated_callback_has_a_handler_and_fits_telegram_limit(self):
        markups = [
            keyboards.report_kb(["SBER", "RUAL"]), keyboards.alert_kb("SBER", True),
            keyboards.flow_kb("SBER"), keyboards.setup_kb("SBER"),
            keyboards.book_kb("SBER"), keyboards.tape_kb("SBER"),
            keyboards.cluster_kb("SBER", "4h"), keyboards.chart_kb("SBER", "5m", ["RUAL"]),
            keyboards.screener_kb("4h", False, ["SBER", "RUAL"]),
            keyboards.scr_pick_kb("SBER", False),
        ]
        exact = {"refresh", "settings", "noop", "flow:off", "chart:close", "msg:close", "scr:close"}
        prefixes = ("chart:", "book:", "tape:", "clu:", "setup:", "cscalp:",
                    "scr:", "watch:", "unwatch:", "pick:")
        callbacks = [b.callback_data for kb in markups for row in kb.inline_keyboard
                     for b in row if b.callback_data]
        self.assertTrue(callbacks)
        for value in callbacks:
            self.assertLessEqual(len(value.encode()), 64)
            self.assertTrue(value in exact or value.startswith(prefixes), value)


class ButtonTransitionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import bot as bot_module
        self.mod = bot_module
        self.mod._scr_msg.clear()

    async def test_screener_photo_to_empty_text_replaces_old_message(self):
        current = SimpleNamespace(photo=[object()], message_id=7,
                                  delete=AsyncMock(), edit_text=AsyncMock())
        sent = SimpleNamespace(message_id=8)
        client = SimpleNamespace(send_message=AsyncMock(return_value=sent),
                                 delete_message=AsyncMock())
        scr = SimpleNamespace(secs={"X": object()}, rows=lambda *_: ([], [], 0),
                              status_text=lambda: "цена на 18:50 МСК")
        await self.mod.show_screener(client, 1, scr, "4h", current=current)
        current.delete.assert_awaited_once()
        client.send_message.assert_awaited_once()
        self.assertEqual(self.mod._scr_msg[1], 8)

    async def test_screener_text_to_photo_replaces_old_message(self):
        current = SimpleNamespace(photo=[], message_id=7,
                                  delete=AsyncMock(), edit_media=AsyncMock())
        sent = SimpleNamespace(message_id=9)
        client = SimpleNamespace(send_photo=AsyncMock(return_value=sent),
                                 delete_message=AsyncMock())
        row = Row("X", "X", 1.0, 101.0, 2, 1e8)
        scr = SimpleNamespace(secs={"X": object()}, rows=lambda *_: ([row], [], 1))
        with patch("screener.render_png", return_value=b"png"):
            await self.mod.show_screener(client, 1, scr, "15m", current=current)
        current.delete.assert_awaited_once()
        client.send_photo.assert_awaited_once()
        self.assertEqual(self.mod._scr_msg[1], 9)


class FlowSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_day_context_uses_moex_without_extra_broker_candle_calls(self):
        tk = SimpleNamespace(candles=AsyncMock())
        mon = flow.FlowMonitor(None, None, tk, None)
        info = moex.SecurityInfo("X", "X", "TQBR", "SUR", 2, 100, "2026-11-05")
        mon._info = AsyncMock(return_value=info)
        today = moex.now_msk().strftime("%Y-%m-%d")
        row = [100, 101, 102, 99, 1000, 10,
               f"{today} 10:00:00", f"{today} 10:01:00"]
        with patch("moex._candles", AsyncMock(return_value=[row])), \
                patch("moex.prev_day_hl", AsyncMock(return_value=(105, 95))):
            stats = await mon._daystats(None, "X")
        self.assertEqual((stats.high, stats.low, stats.vwap), (102, 99, 100))
        tk.candles.assert_not_awaited()


class StartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_menu_is_published_for_private_and_russian_scopes(self):
        import bot as bot_module
        client = SimpleNamespace(set_my_commands=AsyncMock())
        self.assertTrue(await bot_module.setup_menu(client))
        self.assertEqual(client.set_my_commands.await_count, 4)
        published = client.set_my_commands.await_args_list[0].args[0]
        names = {x.command for x in published}
        self.assertTrue({"version", "diag", "ideas", "risk"} <= names)


class StoreMigrationTests(TempCase):
    def test_legacy_root_users_file_moves_to_data_directory(self):
        previous = os.getcwd()
        os.chdir(self.path)
        self.addCleanup(os.chdir, previous)
        legacy = {"42": {"watchlist": ["SBER"], "threshold_pct": 1.5}}
        Path("users.json").write_text(json.dumps(legacy), encoding="utf-8")
        store = Store("data/users.json")
        self.assertEqual(store.get(42).watchlist, ["SBER"])
        self.assertTrue(Path("data/users.json").exists())


class IdeaTests(TempCase):
    def sample(self, direction=1):
        now = time.time()
        return ideas.Idea(
            "abc", "SBER", direction, "pullback", now, now, now + 1000,
            "prepared", 100.0, 99.9, 100.1, 99.0 if direction > 0 else 101.0,
            101.5 if direction > 0 else 98.5, 102.5 if direction > 0 else 97.5,
            75, ["тренд", "поток"], lot=10)

    def test_pre_entry_invalidation_is_cancel_not_stop(self):
        x = self.sample()
        event = ideas.update_lifecycle(x, 98.9, now=1100)
        self.assertEqual(event, "cancelled")
        self.assertEqual(x.status, "cancelled")
        self.assertFalse(x.trigger_ts)

    def test_trigger_then_target_records_r_and_excursions(self):
        x = self.sample()
        self.assertEqual(ideas.update_lifecycle(x, 100.1, now=1100), "active")
        self.assertIsNone(ideas.update_lifecycle(x, 100.8, now=1200))
        self.assertEqual(ideas.update_lifecycle(x, 101.6, now=1300), "target")
        self.assertEqual(x.status, "target")
        self.assertAlmostEqual(x.realized_r, 1.5)
        self.assertGreater(x.mfe_pct, 0)

    def test_risk_size_includes_lot_and_costs(self):
        x = self.sample()
        lots, risk = ideas.position_size(x, 1000, costs_bps=10)
        self.assertGreater(lots, 0)
        self.assertLessEqual(risk, 1000)
        self.assertLess((lots + 1) * ((100 - 99) + .1) * 10, 1101)

    def test_journal_roundtrip_and_stats_are_unique_market_ideas(self):
        path = str(self.path / "ideas.json")
        j = ideas.IdeaJournal(path)
        x = self.sample()
        x.deliveries = [ideas.Delivery(1, 1, 10), ideas.Delivery(2, 2, 20)]
        j.add(x)
        loaded = ideas.IdeaJournal(path)
        self.assertEqual(len(loaded.ideas), 1)
        self.assertIn("всего: <b>1</b>", loaded.stats(1).lower())


if __name__ == "__main__":
    unittest.main()
