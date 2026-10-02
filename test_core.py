import json
import os
import tempfile
import time
import unittest

import cscalp
from journal import Entry, Journal
from screener import Sec
from store import Store


class SecurityTests(unittest.TestCase):
    def setUp(self):
        self.old_key = cscalp.CSCALP_KEY
        cscalp.CSCALP_KEY = "x" * 32

    def tearDown(self):
        cscalp.CSCALP_KEY = self.old_key

    def test_signature_rejects_tampering(self):
        msg = cscalp._signed({"ticker": "SBER", "ts": time.time(), "nonce": "n"})
        self.assertTrue(cscalp._verify(msg))
        msg["ticker"] = "GAZP"
        self.assertFalse(cscalp._verify(msg))

    def test_queue_keeps_multiple_commands(self):
        queue = cscalp.CScalpQueue()
        queue.push("SBER")
        queue.push("GAZP")
        self.assertEqual([x["ticker"] for x in queue.pending], ["SBER", "GAZP"])


class StatisticsTests(unittest.TestCase):
    def test_quality_deduplicates_deliveries(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = Journal(os.path.join(tmp, "signals.json"))
            for uid in range(30):
                journal.entries.append(Entry(
                    ts=time.time(), uid=uid, chat_id=uid, msg_id=uid,
                    ticker="SBER", kind="wall", price=100,
                    event_id="same-market-event", direction=1,
                    results={"15": 0.5}, done=True,
                ))
            n, hit = journal.quality("SBER", "wall")
            self.assertEqual(n, 1)
            self.assertEqual(hit, 100.0)


class FreshnessTests(unittest.TestCase):
    def test_stale_screener_price_is_not_a_result(self):
        sec = Sec("SBER", "Sber", "uid", 100, 1_000_000)
        sec.last = 101
        sec.last_ts = time.time() - 3600
        sec.hist.append((time.time() - 3600, 100))
        self.assertIsNone(sec.change("15m"))


class PersistenceTests(unittest.TestCase):
    def test_store_ignores_valid_json_with_wrong_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "users.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump([], fh)
            self.assertEqual(list(Store(path).all()), [])


if __name__ == "__main__":
    unittest.main()
