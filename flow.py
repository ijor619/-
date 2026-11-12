"""Фоновый монитор ленты сделок и стакана через T-Invest API.

Что делает каждые FLOW_INTERVAL_SEC секунд:
  1. по каждой бумаге из списков пользователей тянет ленту (10 мин) и стакан;
  2. обновляет адаптивные «нормы» бумаги (Baseline) и дневную статистику
     (high/low/VWAP из минутных свечей MOEX);
  3. прогоняет детекторы tape.py; сигналы копятся в дайджест и уходят
     одним сообщением на бумагу (не чаще раза в DIGEST_SEC);
  4. каждый отправленный сигнал пишется в журнал; через 5 и 15 минут
     к сообщению дописывается результат («→ 15м: +0,42%»).
"""
from __future__ import annotations

import asyncio
import os
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import BufferedInputFile, InputMediaPhoto

import charts
import market
import moex
import setup as stp
import tape
from marketstream import MarketStream
from journal import CHECKPOINTS, Entry, Journal, HIT_PCT
from ideas import (Delivery, IdeaContext, IdeaEngine, IdeaJournal, card,
                   is_tradeable, update_lifecycle)
from keyboards import flow_kb, idea_kb
from formatting import fmt_pct
from moex import now_msk
from store import Store
from tinkoff import OrderBook, TinkoffClient, Trade, enabled
from clusters import price_endpoints

log = logging.getLogger(__name__)

FLOW_INTERVAL_SEC = max(10, int(os.getenv("FLOW_INTERVAL_SEC", "15")))
SIGNAL_COOLDOWN_SEC = 30 * 60   # один и тот же сигнал по бумаге — не чаще
KIND_COOLDOWN_SEC = 10 * 60     # один и тот же ТИП сигнала по бумаге — не чаще
DIGEST_SEC = 300                # сигналы по бумаге копятся и уходят пачкой
MAX_SIGNALS_PER_MSG = 4
DAYSTATS_TTL = 60               # как часто обновлять high/low/VWAP

# направление сигнала для оценки «попаданий»
def _direction(sig: tape.Signal) -> int:
    return sig.direction if sig.direction in (-1, 1) else 0


def _hm(s: str, default: int) -> int:
    try:
        h, m = (s.split(":") + ["0"])[:2]
        return int(h) * 60 + int(m)
    except Exception:
        return default


SIGNAL_FROM_MIN = _hm(os.getenv("SIGNAL_FROM", "07:00"), 7 * 60)
SIGNAL_TO_MIN = _hm(os.getenv("SIGNAL_TO", "19:00"), 19 * 60)


class DayStats:
    __slots__ = ("high", "low", "vwap", "ts", "prev_high", "prev_low", "prev_date",
                 "hl_hist", "day")

    def __init__(self) -> None:
        self.high = self.low = self.vwap = 0.0
        self.ts = 0.0
        self.day = now_msk().date()
        self.prev_high = self.prev_low = 0.0     # вчерашние экстремумы
        self.prev_date = ""                      # на какой день загружены
        self.hl_hist: list[tuple[float, float, float]] = []   # (ts, high, low) по опросам

    def high_asof(self, age_sec: float) -> tuple[float, float]:
        """Максимум/минимум дня, какими они были не позже age_sec назад
        (уровень, который «постоял»)."""
        cut = time.time() - age_sec
        best = (0.0, 0.0)
        for ts, h, l in self.hl_hist:
            if ts <= cut:
                best = (h, l)
            else:
                break
        return best


# «второй факт»: сигнал считается подтверждённым, если согласны >= CONFIRM_MIN
# из фактов: дельта 15 мин, сторона VWAP, сила к рынку, близость к ключевому уровню
CONFIRM_MIN = 2
LEVEL_NEAR_PCT = 0.3            # «у уровня» для подтверждения
BREAKOUT_COOLDOWN_SEC = 2 * 3600
BREAKOUT_HOLD_SEC = 60 * 60     # максимум/минимум дня должен простоять ≥ 1 ч


class FlowMonitor:
    def __init__(self, bot: Bot, store: Store, tk: TinkoffClient,
                 journal: Journal, clusters=None, idea_journal: Optional[IdeaJournal] = None) -> None:
        self.bot, self.store, self.tk, self.journal = bot, store, tk, journal
        self.clusters = clusters
        self._books: Dict[str, tape.BookState] = {}
        self._base: Dict[str, tape.Baseline] = {}
        self._day: Dict[str, DayStats] = {}
        self._sent: Dict[tuple[int, str], float] = {}     # (uid, key) -> ts
        self._digest: Dict[tuple[int, str], list[tape.Signal]] = {}
        self._digest_ts: Dict[tuple[int, str], float] = {}
        self._dec: Dict[str, int] = {}
        self._infos: Dict[str, Optional[moex.SecurityInfo]] = {}
        self._infos_ts: dict[str, float] = {}
        self._last_price: Dict[str, float] = {}
        self._last_price_ts: dict[str, float] = {}
        self._prices: dict[str, dict[float, float]] = {}
        self._observed_until: dict[str, float] = {}
        self._recent_events: dict[str, float] = {}
        self._last_book: Dict[str, OrderBook] = {}
        self._imoex: dict = {"15m": None, "day": None}
        self._imoex_ts = 0.0
        self._levels: Dict[str, list] = {}          # ticker -> [Level]
        self._levels_ts: Dict[str, float] = {}
        self._level_side: Dict[str, bool] = {}      # ticker -> цена была выше ближайшего уровня
        self._level_sent: Dict[tuple[str, float], float] = {}
        self._recent: Dict[str, list[tuple[str, int, float]]] = {}  # ticker -> [(kind, dir, ts)]
        self._prev_price: Dict[str, float] = {}
        self._breakout_sent: Dict[tuple[str, float], float] = {}
        self._digest_first: Dict[tuple[int, str], float] = {}   # когда в пачке появился первый сигнал
        self._confirmed: Dict[int, dict] = {}                  # id(sig) -> {"facts": [...], "ctx": {...}}
        self.idea_journal = idea_journal
        self.idea_engine = IdeaEngine() if idea_journal is not None else None
        self.stream = MarketStream()
        self._rest_trades: Dict[str, list[Trade]] = {}
        self._rest_trades_ts: Dict[str, float] = {}
        self._rest_book_ts: Dict[str, float] = {}
        self.screener = None
        self.newsmon = None
        self.calendar = None

    def close(self) -> None:
        self.stream.stop()

    # ------------------------------------------------------------------ run
    async def run(self, sess) -> None:
        if not enabled():
            log.info("flow: TINKOFF_TOKEN не задан — анализ ленты/стакана выключен")
            return
        log.info("flow: анализ ленты и стакана запущен, опрос каждые %s c", FLOW_INTERVAL_SEC)
        while True:
            t0 = time.monotonic()
            try:
                await self.tick(sess)
            except Exception:
                log.exception("flow: ошибка тика")
            try:
                await self.followups()
            except Exception:
                log.exception("flow: ошибка follow-up")
            await asyncio.sleep(max(5.0, FLOW_INTERVAL_SEC - (time.monotonic() - t0)))

    # -------------------------------------------------------------- helpers
    async def _info(self, sess, t: str) -> Optional[moex.SecurityInfo]:
        if t not in self._infos or time.time() - self._infos_ts.get(t, 0) >= 300:
            try:
                fresh = await moex.get_security_info(sess, t)
                if fresh is not None:
                    self._infos[t] = fresh
                    self._infos_ts[t] = time.time()
            except Exception:
                # Временный сбой MOEX не должен ломать кнопки сетапа и
                # графика, если справочник уже был успешно загружен.
                return self._infos.get(t)
        return self._infos.get(t)

    async def _daystats(self, sess, t: str) -> DayStats:
        ds = self._day.setdefault(t, DayStats())
        if ds.day != now_msk().date():
            ds = self._day[t] = DayStats()
        if time.time() - ds.ts < DAYSTATS_TTL:
            return ds
        ds.ts = time.time()
        info = await self._info(sess, t)
        if info is None:
            return ds
        try:
            day0 = now_msk().replace(hour=0, minute=0, second=0, microsecond=0)
            # Дневные H/L и VWAP не требуют брокерского API. ISS снимает
            # лишнюю нагрузку с T-Invest и не мешает кнопкам стакана/ленты.
            rows = await moex._candles(sess, t, info.board, day0)
            rows = [r for r in rows if r[6][:10] == day0.strftime("%Y-%m-%d")]
            if rows:
                ds.high = max(float(r[2]) for r in rows)
                ds.low = min(float(r[3]) for r in rows)
                val = sum(float(r[4] or 0) for r in rows)
                vol = sum(float(r[5] or 0) for r in rows)
                ds.vwap = val / vol if vol else 0.0
                ds.hl_hist.append((time.time(), ds.high, ds.low))
                if len(ds.hl_hist) > 1500:
                    ds.hl_hist = ds.hl_hist[-1000:]
            today = day0.strftime("%Y-%m-%d")
            if ds.prev_date != today:
                ph, pl = await moex.prev_day_hl(sess, t, info.board)
                if ph:
                    ds.prev_high, ds.prev_low, ds.prev_date = ph, pl, today
        except Exception as e:
            log.debug("flow: daystats %s: %s", t, e)
        return ds

    # ------------------------------------------------------- подтверждение
    def key_levels(self, t: str, ds: DayStats) -> list[tuple[str, float]]:
        """Ключевые уровни: вчерашние high/low и постоявшие ≥1 ч max/min дня."""
        out = []
        if ds.prev_high:
            out.append(("вчерашний максимум", ds.prev_high))
        if ds.prev_low:
            out.append(("вчерашний минимум", ds.prev_low))
        h, l = ds.high_asof(BREAKOUT_HOLD_SEC)
        if h and abs(h - ds.prev_high) / h > 0.001:
            out.append(("максимум дня", h))
        if l and abs(l - ds.prev_low) / l > 0.001:
            out.append(("минимум дня", l))
        return out

    def _delta15(self, t: str) -> int:
        if self.clusters is None:
            return 0
        c15 = self.clusters.window(t, 15)
        b = sum(c.buy for c in c15); s_ = sum(c.sell for c in c15)
        if not b + s_:
            return 0
        bp = b / (b + s_)
        return 1 if bp >= 0.58 else -1 if bp <= 0.42 else 0

    def _rel_strength(self, t: str, im: dict) -> int:
        ch15 = self.price_change(t, 15)
        if ch15 is None or im.get("15m") is None:
            return 0
        d = ch15 - im["15m"]
        return 1 if d >= 0.3 else -1 if d <= -0.3 else 0

    def facts(self, t: str, direction: int, price: float, ds: DayStats, im: dict,
              dec: int) -> tuple[list[str], dict]:
        """Какие независимые факты согласны с направлением сигнала.
        Возвращает (список подписей, ctx для журнала)."""
        d15 = self._delta15(t)
        rs = self._rel_strength(t, im)
        vw = (1 if price >= ds.vwap else -1) if ds.vwap and price else 0
        near = None
        for name, lv in self.key_levels(t, ds):
            if lv and abs(price - lv) / lv * 100 <= LEVEL_NEAR_PCT:
                near = (name, lv); break
        ok = []
        if direction and d15 == direction:
            ok.append("дельта 15м " + ("за покупки" if direction > 0 else "за продажи"))
        if direction and vw == direction:
            ok.append(("выше" if direction > 0 else "ниже") + " VWAP")
        if direction and rs == direction:
            ok.append("сильнее рынка" if direction > 0 else "слабее рынка")
        # A level confirms only from the protective side: support below a
        # long, resistance above a short. Mere proximity is not directional.
        if near and ((direction > 0 and price >= near[1]) or
                     (direction < 0 and price <= near[1])):
            ok.append(f"у уровня {near[0]} {near[1]:.{dec}f}".replace(".", ","))
        ctx = {"vwap": vw or None, "hour": now_msk().hour, "delta15": d15, "rs": rs,
               "near": 1 if near else 0, "conf": len(ok)}
        return ok, ctx

    def _breakout_signals(self, t: str, price: float, prev_price: float, ds: DayStats,
                          dec: int) -> list[tape.Signal]:
        """Пробой ключевого уровня: цена пересекла его между двумя опросами."""
        out = []
        if not price or not prev_price or price == prev_price:
            return out
        now = time.time()
        for name, lv in self.key_levels(t, ds):
            if not lv:
                continue
            up = prev_price < lv <= price
            down = prev_price > lv >= price
            if not (up or down):
                continue
            k = (t, round(lv, 6))
            if now - self._breakout_sent.get(k, 0) < BREAKOUT_COOLDOWN_SEC:
                continue
            self._breakout_sent[k] = now
            arrow = "🚀 пробой вверх" if up else "🔻 пробой вниз"
            d15 = self._delta15(t)
            dtxt = {1: "дельта 15м за покупки", -1: "дельта 15м за продажи", 0: "дельта 15м нейтральна"}[d15]
            lvl = f"{lv:.{dec}f}".replace(".", ",")
            ptxt = f"{price:.{dec}f}".replace(".", ",")
            out.append(tape.Signal(
                kind="breakout", ticker=t,
                text=f"{arrow}: <b>{t}</b> прошла {name} <b>{lvl}</b> — цена {ptxt} ₽, {dtxt}",
                key=f"breakout:{name}:{'up' if up else 'down'}", direction=1 if up else -1))
        return out

    async def imoex(self, sess) -> dict:
        """{'15m': %, 'day': %}: день и 15 мин — из T-Invest в реальном времени
        (market.snapshot + собственная история); пока истории < 15 мин — 15m от MOEX."""
        if time.time() - self._imoex_ts > 60:
            self._imoex_ts = time.time()
            try:
                snap = await market.snapshot(sess)
                rt15 = market.imoex_change(15)
                if snap.ok() and rt15 is not None:
                    self._imoex = {"15m": rt15, "day": snap.imoex_pct}
                else:
                    mx = await moex.index_changes(sess)
                    if snap.ok():
                        mx["day"] = snap.imoex_pct
                    self._imoex = mx
            except Exception as e:
                log.debug("flow: imoex: %s", e)
        return self._imoex

    def levels(self, t: str, price: float, decimals: int, force: bool = False) -> list:
        """Уровни по объёму из кластеров, пересчёт раз в 5 минут."""
        if self.clusters is None:
            return []
        if force or time.time() - self._levels_ts.get(t, 0) > 300:
            self._levels_ts[t] = time.time()
            cells = self.clusters.window(t, stp.LEVEL_LOOKBACK_MIN)
            lv = stp.find_levels(cells, price, decimals)
            for l in lv:
                l.tests = stp.level_tests(l, cells, decimals)
            self._levels[t] = lv
        return self._levels.get(t, [])

    def recent_signals(self, t: str) -> list[tuple[str, int, float]]:
        """[(kind, direction, age_min)] за последние 30 минут."""
        now = time.time()
        rs = [(k, d, (now - ts) / 60) for k, d, ts in self._recent.get(t, []) if now - ts < 1800]
        self._recent[t] = [(k, d, ts) for k, d, ts in self._recent.get(t, []) if now - ts < 1800]
        return rs

    def price_change(self, t: str, minutes: int) -> Optional[float]:
        """Изменение цены бумаги за N минут по кластерам (%)."""
        if self.clusters is None:
            return None
        cells = self.clusters.window(t, minutes)
        if len(cells) < 2:
            return None
        first, last = price_endpoints(cells)
        return (last - first) / first * 100 if first else None

    def _level_signals(self, t: str, price: float, decimals: int) -> list[tape.Signal]:
        """Подход к уровню по объёму: сигнал при пересечении зоны LEVEL_NEAR_PCT."""
        lv = self.levels(t, price, decimals)
        if not lv or not price:
            return []
        nearest = min(lv, key=lambda l: abs(l.price - price))
        dist = abs(nearest.price - price) / price * 100
        was_above = self._level_side.get(t)
        now_above = price >= nearest.price
        out = []
        if dist <= stp.LEVEL_NEAR_PCT:
            key = (t, nearest.price)
            if time.time() - self._level_sent.get(key, 0) > stp.LEVEL_ALERT_COOLDOWN:
                # подход к уровню только если пришли издалека (в прошлый тик были дальше)
                prev = self._level_side.get((t, "dist"), 99.0)
                if prev > stp.LEVEL_NEAR_PCT:
                    self._level_sent[key] = time.time()
                    from_above = was_above if was_above is not None else now_above
                    text = stp.level_alert_text(t, nearest, price, decimals, bool(from_above))
                    out.append(tape.Signal("level", t, text, f"level:{nearest.price}"))
        self._level_side[(t, "dist")] = dist
        self._level_side[t] = now_above
        return out

    def _volume_signal(self, t: str, decimals: int) -> list[tape.Signal]:
        if self.clusters is None:
            return []
        hist = self.clusters.window(t, stp.LEVEL_LOOKBACK_MIN)
        if not hist:
            return []
        last = hist[-1].minute
        now5 = [c for c in hist if c.minute >= last - timedelta(minutes=4)]
        va = stp.volume_anomaly(now5, hist, 5)
        if not va or va[0] < 4:
            return []
        k, v, med = va
        b = sum(c.buy for c in now5); s = sum(c.sell for c in now5)
        bp = b / (b + s) * 100 if b + s else 50
        side = "покупок" if bp >= 60 else "продаж" if bp <= 40 else "в обе стороны"
        text = (f"📊 <b>{t}</b> · аномальный объём: за 5 мин {tape.fmt_n(v)} лот — "
                f"в <b>{k:.1f}×</b> выше нормы для этого времени дня (медиана {tape.fmt_n(med)} лот). "
                f"Перевес {side} ({bp:.0f}% покупок).")
        return [tape.Signal("volume", t, text, f"volume:{last:%H:%M}")]

    @staticmethod
    def _main_session() -> bool:
        """Окно сигналов 07:00–19:00 МСК (см. SIGNAL_FROM/SIGNAL_TO).
        По статистике недели вечерняя сессия давала 20% сигналов с 10% попадания —
        для сигналов закрыта; лента для кластеров копится всё равно (см. tick)."""
        n = now_msk()
        m = n.hour * 60 + n.minute
        return SIGNAL_FROM_MIN <= m <= SIGNAL_TO_MIN

    @staticmethod
    def _trading_time() -> bool:
        n = now_msk()
        m = n.hour * 60 + n.minute
        return 6 * 60 + 50 <= m <= 23 * 60 + 50

    # ------------------------------------------------------------------ tick
    async def tick(self, sess) -> None:
        # Raw microstructure events are diagnostic context, not entries.  They
        # are opt-in separately; actionable ideas are handled below.
        users = [(u, p) for u, p in self.store.all()
                 if p.flow_alerts and p.raw_flow_alerts and p.watchlist]
        # ленту копим для кластеров по всем спискам, даже если сигналы выключены
        tickers = sorted({t for _, p in self.store.all() for t in p.watchlist})
        if not tickers:
            return
        results = await asyncio.gather(*(self._analyze(sess, t) for t in tickers),
                                       return_exceptions=True)
        signals: Dict[str, list[tape.Signal]] = {}
        for t, r in zip(tickers, results):
            if isinstance(r, BaseException):
                log.warning("flow: %s: %s", t, r)
            else:
                signals[t] = r

        now = time.time()
        hour = now_msk().hour
        im = await self.imoex(sess)
        if self.idea_journal is not None:
            try:
                await self._process_ideas(sess, tickers, im)
            except Exception:
                log.exception("ideas: ошибка обработки")
        if not self._main_session():
            return   # новых идей/сырых push нет, но старые идеи сопровождаются
        # «второй факт»: считаем подтверждение один раз на бумагу и направление
        for t, sigs in signals.items():
            if not sigs:
                continue
            price = self._last_price.get(t, 0.0)
            ds = await self._daystats(sess, t)
            info = self._infos.get(t)
            dec = info.decimals if info else 2
            cache: Dict[int, tuple[list[str], dict]] = {}
            for sig in sigs:
                d = _direction(sig)
                if d not in cache:
                    cache[d] = self.facts(t, d, price, ds, im, dec)
                ok, ctx = cache[d]
                if sig.kind == "breakout":
                    ok = [x for x in ok if not x.startswith("у уровня")]
                    ctx = dict(ctx, conf=len(ok))
                self._confirmed[id(sig)] = {"facts": ok, "ctx": ctx,
                                            "ok": d != 0 and len(ok) >= CONFIRM_MIN}

        for uid, prof in users:
            if prof.is_quiet(hour):
                continue
            for t in prof.watchlist:
                for sig in signals.get(t, []):
                    # авто-фильтр по статистике: слабые пары «бумага × тип» не шлём
                    if self.journal.is_muted(t, sig.kind):
                        continue
                    if self.journal.recently_sent(uid, t, sig.kind, sig.key, now,
                                                  KIND_COOLDOWN_SEC, SIGNAL_COOLDOWN_SEC):
                        continue
                    k = (uid, f"{t}:{sig.key}")
                    kk = (uid, f"{t}:{sig.kind}")
                    if now - self._sent.get(k, 0) < SIGNAL_COOLDOWN_SEC:
                        continue
                    if now - self._sent.get(kk, 0) < KIND_COOLDOWN_SEC:
                        continue
                    self._sent[k] = self._sent[kk] = now
                    if not self._digest.get((uid, t)):
                        self._digest_first[(uid, t)] = now
                    self._digest.setdefault((uid, t), []).append(sig)

        # отправка: подтверждённые — сразу; одиночные копятся DIGEST_SEC и уходят сводкой
        for (uid, t), sigs in list(self._digest.items()):
            if not sigs:
                continue
            prof = self.store.get(uid)
            if not prof.flow_alerts or t not in prof.watchlist or prof.is_quiet(hour):
                self._digest[(uid, t)] = []
                continue
            first = self._digest_first.get((uid, t), now)
            has_conf = any(self._confirmed.get(id(s), {}).get("ok") for s in sigs)
            if not has_conf and now - first < DIGEST_SEC and len(sigs) < MAX_SIGNALS_PER_MSG:
                continue
            if await self._send_signals(sess, uid, t, sigs[:MAX_SIGNALS_PER_MSG]):
                self._digest[(uid, t)] = sigs[MAX_SIGNALS_PER_MSG:]
                self._digest_ts[(uid, t)] = now
        if len(self._confirmed) > 2000:
            self._confirmed = {}

        if len(self._sent) > 5000:
            self._sent = {k: v for k, v in self._sent.items() if now - v < 3600}

    # ---------------------------------------------------------- trade ideas
    def _book_metrics(self, t: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """(spread %, distance-weighted queue imbalance, microprice edge %)."""
        ob = self._last_book.get(t)
        if not ob or not ob.bids or not ob.asks:
            return None, None, None
        bid, ask = ob.bids[0], ob.asks[0]
        mid = (bid.price + ask.price) / 2
        if mid <= 0 or ask.price <= bid.price:
            return None, None, None
        spread = (ask.price - bid.price) / mid * 100
        bq = sum(x.qty / (i + 1) for i, x in enumerate(ob.bids[:10]))
        aq = sum(x.qty / (i + 1) for i, x in enumerate(ob.asks[:10]))
        qi = (bq - aq) / (bq + aq) if bq + aq else 0.0
        micro = (ask.price * bid.qty + bid.price * ask.qty) / (bid.qty + ask.qty) \
            if bid.qty + ask.qty else mid
        return spread, qi, (micro / mid - 1) * 100

    def _buy_pct(self, t: str, minutes: int) -> Optional[float]:
        if self.clusters is None:
            return None
        cells = self.clusters.window(t, minutes)
        b, s = sum(x.buy for x in cells), sum(x.sell for x in cells)
        return b / (b + s) * 100 if b + s else None

    def _technical_context(self, t: str) -> tuple[Optional[float], Optional[float]]:
        """RSI14 and EMA9-vs-EMA21; context only, never an entry trigger."""
        if self.clusters is None:
            return None, None
        cells = self.clusters.window(t, 90)
        by_min: dict[datetime, list[float]] = {}
        for x in cells:
            v = x.buy + x.sell
            row = by_min.setdefault(x.minute, [0.0, 0.0])
            row[0] += x.price * v
            row[1] += v
        prices = [pv / v for _, (pv, v) in sorted(by_min.items()) if v]
        if len(prices) < 15:
            return None, None
        diffs = [b - a for a, b in zip(prices[-15:], prices[-14:])]
        gain = sum(max(0.0, x) for x in diffs) / 14
        loss = sum(max(0.0, -x) for x in diffs) / 14
        rsi = 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
        def ema(period: int) -> float:
            a = 2 / (period + 1)
            value = prices[0]
            for p in prices[1:]:
                value += a * (p - value)
            return value
        e9, e21 = ema(9), ema(21)
        edge = (e9 / e21 - 1) * 100 if e21 else None
        return rsi, edge

    def _idea_context(self, t: str, im: dict) -> Optional[IdeaContext]:
        price = self._last_price.get(t, 0.0)
        if price <= 0 or time.time() - self._last_price_ts.get(t, 0) > 90:
            return None
        inst = self.tk._instr.get(t)
        info = self._infos.get(t)
        ds = self._day.get(t)
        levels: list[tuple[str, float]] = []
        if ds:
            levels.extend(self.key_levels(t, ds))
        for lv in self.levels(t, price, info.decimals if info else 2):
            levels.append(("POC" if lv.kind == "poc" else "объёмный уровень", lv.price))
        # Deduplicate nearly identical structural references.
        uniq: list[tuple[str, float]] = []
        for name, level in sorted(levels, key=lambda x: abs(x[1] - price)):
            if level > 0 and not any(abs(level - p) / p < 0.0003 for _, p in uniq):
                uniq.append((name, level))
        spread, qi, micro = self._book_metrics(t)
        tech_rsi, ema_edge = self._technical_context(t)
        sc = self.screener.idea_context(t) if self.screener is not None else {}
        va = None
        turnover_rub_min = None
        if self.clusters is not None:
            hist = self.clusters.window(t, stp.LEVEL_LOOKBACK_MIN)
            if hist:
                end = hist[-1].minute
                now5 = [x for x in hist if x.minute >= end - timedelta(minutes=4)]
                x = stp.volume_anomaly(now5, hist, 5)
                va = x[0] if x else None
                recent = [x for x in hist if x.minute >= end - timedelta(minutes=29)]
                minute_turnover: dict[datetime, float] = {}
                lot = inst.lot if inst else 1
                for cell in recent:
                    minute_turnover[cell.minute] = minute_turnover.get(cell.minute, 0.0) + (
                        cell.price * (cell.buy + cell.sell) * lot)
                if minute_turnover:
                    first_minute = min(x.minute for x in hist)
                    observed = min(30, int((end - first_minute).total_seconds() // 60) + 1)
                    if observed >= 10:
                        # Divide by elapsed minutes, including minutes without
                        # trades; otherwise one isolated print looks liquid.
                        turnover_rub_min = sum(minute_turnover.values()) / observed
        return IdeaContext(
            ticker=t, price=price, prev_price=self._prev_price.get(t, 0.0),
            decimals=info.decimals if info else 2, lot=inst.lot if inst else 1,
            vwap=ds.vwap if ds else 0.0, levels=uniq[:10],
            ch5=self.price_change(t, 5), ch15=self.price_change(t, 15),
            ch60=self.price_change(t, 60), market15=im.get("15m"),
            buy_pct5=self._buy_pct(t, 5), buy_pct15=self._buy_pct(t, 15),
            volume_mult=va, spread_pct=spread, book_imbalance=qi,
            microprice_edge_pct=micro, breadth=sc.get("breadth"),
            sector_edge=sc.get("sector_edge"), rsi14=tech_rsi,
            ema_edge_pct=ema_edge, turnover_rub_min=turnover_rub_min, fresh=True,
            event_risk=self.calendar.risk_for(t) if self.calendar is not None else "",
            news_risk=self.newsmon.risk_for(t) if self.newsmon is not None else "")

    async def _idea_update_messages(self, sess, idea, transition: str) -> None:
        """Edit the card and keep one entry/result chart for the virtual trade."""
        note = {
            "active": f"🚀 <b>{idea.ticker} {idea.side}</b>: виртуальный вход по триггеру {idea.entry_price:.{idea.decimals}f} ₽.",
            "target": f"✅ <b>{idea.ticker} {idea.side}</b>: цель достигнута, итог после комиссии {idea.realized_r:+.2f}R.",
            "stop": f"🛑 <b>{idea.ticker} {idea.side}</b>: стоп, итог после комиссии {idea.realized_r:+.2f}R.",
            "cancelled": f"↩️ <b>{idea.ticker} {idea.side}</b>: идея отменилась до входа — {idea.close_reason}.",
            "expired": f"⌛ <b>{idea.ticker} {idea.side}</b>: выход по времени, итог после комиссии {idea.realized_r:+.2f}R.",
        }.get(transition)
        chart_png = None
        if transition in ("active", "target", "stop", "expired") and idea.trigger_ts:
            try:
                info = await self._info(sess, idea.ticker)
                if info is not None:
                    chart_png = await charts.build_chart(
                        sess, info, "5m", self.tk,
                        trade_ideas=self.idea_journal.for_chart(idea.ticker, 12))
            except Exception:
                log.exception("ideas: не построен график сделки %s", idea.id)
        for delivery in list(idea.deliveries):
            prof = self.store.get(delivery.uid)
            try:
                await self.bot.edit_message_text(
                    card(idea, prof.risk_rub), chat_id=delivery.chat_id,
                    message_id=delivery.msg_id,
                    reply_markup=idea_kb(idea.id, idea.ticker, idea.status))
            except TelegramBadRequest as e:
                if "not modified" not in str(e):
                    log.debug("ideas: edit %s: %s", idea.id, e)
            except Exception as e:
                log.debug("ideas: edit %s: %s", idea.id, e)
            if note and prof.idea_alerts:
                try:
                    kb = idea_kb(idea.id, idea.ticker, idea.status)
                    if chart_png:
                        media = InputMediaPhoto(
                            media=BufferedInputFile(chart_png, f"{idea.ticker}_{idea.id}.png"),
                            caption=note)
                        if delivery.chart_msg_id:
                            try:
                                await self.bot.edit_message_media(
                                    media=media, chat_id=delivery.chat_id,
                                    message_id=delivery.chart_msg_id, reply_markup=kb)
                                continue
                            except Exception as e:
                                log.debug("ideas: trade chart edit %s: %s", idea.id, e)
                        msg = await self.bot.send_photo(
                            delivery.chat_id,
                            BufferedInputFile(chart_png, f"{idea.ticker}_{idea.id}.png"),
                            caption=note, reply_markup=kb)
                        delivery.chart_msg_id = msg.message_id
                    else:
                        await self.bot.send_message(delivery.chat_id, note, reply_markup=kb)
                except Exception:
                    log.exception("ideas: не отправлен переход %s", transition)

    async def _process_ideas(self, sess, tickers: list[str], im: dict) -> None:
        if self.idea_journal is None or self.idea_engine is None:
            return
        contexts = {t: self._idea_context(t, im) for t in tickers}
        dirty = False
        # Life cycle first: an existing prepared idea is never duplicated.
        for idea in list(self.idea_journal.active()):
            ctx = contexts.get(idea.ticker)
            if ctx is None:
                continue  # stale/no price: never invent an outcome
            transition = update_lifecycle(idea, ctx.price)
            if transition:
                dirty = True
                if (transition == "active" and not any(d.msg_id for d in idea.deliveries)
                        and self.idea_journal.publication_slot(idea.ticker)):
                    recipients = [(uid, p) for uid, p in self.store.all()
                                  if p.idea_alerts and idea.ticker in p.watchlist
                                  and not p.is_quiet(now_msk().hour)
                                  and idea.confidence >= p.idea_min_conf]
                    for uid, prof in recipients:
                        try:
                            msg = await self.bot.send_message(
                                uid, card(idea, prof.risk_rub),
                                reply_markup=idea_kb(idea.id, idea.ticker, idea.status))
                            idea.deliveries.append(
                                Delivery(uid, msg.chat.id, msg.message_id,
                                         sent_ts=time.time()))
                        except Exception:
                            log.exception("ideas: не отправлен вход %s %s", idea.ticker, uid)
                await self._idea_update_messages(sess, idea, transition)
        if not self._main_session():
            if dirty:
                self.idea_journal.save()
            return
        try:
            positions = await self.tk.portfolio_day_quotes()
        except Exception:
            positions = {}
        pool = []
        for t, ctx in contexts.items():
            if ctx is None:
                continue
            for idea in self.idea_engine.candidates(ctx):
                adjustment, sample_n, expectancy = self.idea_journal.adaptive_adjustment(
                    idea.setup, idea.direction)
                if adjustment:
                    idea.confidence = max(1, min(95, idea.confidence + adjustment))
                    history = f"история сетапа: {sample_n} исходов, {expectancy:+.2f}R"
                    if adjustment > 0:
                        idea.reasons.append(history)
                    else:
                        idea.cautions.append(history)
                if not is_tradeable(idea):
                    continue
                if t in positions:
                    idea.cautions.append("уже есть открытая позиция: проверь суммарный риск")
                if self.idea_journal.has_active(t, idea.direction):
                    continue
                pool.append((t, idea))

        # Prefer the setups that survived costs in the first forward sample.
        priority = {"false_breakout": 30, "pullback": 20, "breakout": 10}
        pool.sort(key=lambda row: (priority.get(row[1].setup, 0), row[1].confidence),
                  reverse=True)
        for t, idea in pool:
            recipients = [(uid, p) for uid, p in self.store.all()
                          if p.idea_alerts and t in p.watchlist
                          and not p.is_quiet(now_msk().hour)
                          and idea.confidence >= p.idea_min_conf]
            if not recipients:
                continue
            # Keep a strong setup internally until its trigger. Only a real
            # activation consumes the daily limit and reaches Telegram.
            self.idea_journal.ideas.append(idea)
            dirty = True
        if dirty:
            self.idea_journal.save()

    async def _send_signals(self, sess, uid: int, t: str,
                            sigs: list[tape.Signal]) -> bool:
        info = await self._info(sess, t)
        dec = info.decimals if info else 2
        price = self._last_price.get(t, 0.0)
        ds = await self._daystats(sess, t)
        ctx = tape.context_text(price, info.prev_close if info else 0,
                                ds.high, ds.low, ds.vwap, dec,
                                self._last_book.get(t))
        im = await self.imoex(sess)
        ch15 = self.price_change(t, 15)
        rs = self._rel_strength(t, im)
        if im.get("15m") is not None and ch15 is not None:
            ctx += (f"\n📊 15 мин: бумага {fmt_pct(ch15)}, IMOEX {fmt_pct(im['15m'])} — "
                    + ("сильнее рынка" if rs > 0 else "слабее рынка" if rs < 0 else "с рынком"))
        confs = [self._confirmed.get(id(s)) or {"facts": [], "ctx": {}, "ok": False} for s in sigs]
        parts = []
        for s, c in zip(sigs, confs):
            when = datetime.fromtimestamp(s.detected_at, moex.MSK)
            stamp = f"\n<i>Обнаружено {when:%H:%M:%S} МСК · база оценки {s.price:.{dec}f} ₽</i>"
            if c["ok"] and c["facts"]:
                parts.append(s.text + "\n⭐ подтверждено: " + ", ".join(c["facts"]) + stamp)
            else:
                parts.append(s.text + stamp)
        body = "\n\n".join(parts)
        if not any(c["ok"] for c in confs):
            body = "🗂 <i>Сводка за 5 мин (без подтверждения)</i>\n\n" + body
        # под китами контекст не показываем — перегружает (по просьбе пользователя)
        if all(s.kind in ("whale", "whale_series") for s in sigs):
            ctx = ""
        text = body + ("\n\n" + ctx if ctx else "")
        try:
            msg = await self.bot.send_message(uid, text, reply_markup=flow_kb(t))
        except Exception:
            log.exception("flow: не отправлено %s", uid)
            return False
        if sigs:
            # журналируем каждый сигнал; результат дописываем к одному сообщению
            for s, c in zip(sigs, confs):
                ctx_d = c["ctx"] or {"hour": now_msk().hour}
                self.journal.add(Entry(ts=s.detected_at, uid=uid, chat_id=msg.chat.id,
                                       msg_id=msg.message_id, ticker=t, kind=s.kind,
                                       price=s.price, direction=_direction(s), text=text,
                                       ctx=ctx_d, event_id=s.event_id, signal_key=s.key,
                                       price_ts=s.price_ts))
                if s.event_id not in self._recent_events:
                    self._recent.setdefault(t, []).append((s.kind, _direction(s), s.detected_at))
                    self._recent_events[s.event_id] = s.detected_at
            self._recent_events = {k: v for k, v in self._recent_events.items() if time.time() - v < 3600}
        return True

    async def _analyze(self, sess, t: str) -> list[tape.Signal]:
        try:
            inst = await self.tk.instrument(t)
        except Exception as e:
            log.warning("flow: T-Invest недоступен (%s): %s", t, e)
            return []
        if inst is None:
            return []
        self.stream.ensure(t, inst.uid)
        info = await self._info(sess, t)
        dec = info.decimals if info else 2
        stream_trades, stream_book = self.stream.snapshot(t, minutes=10)
        now = time.time()
        # REST — только резерв потока. Частые фоновые запросы раньше могли
        # съедать лимит T-Invest и мешать ручным кнопкам стакана/ленты.
        need_trades = (not self._rest_trades.get(t) or
                       now - self._rest_trades_ts.get(t, 0) >= (300 if stream_trades else 60))
        need_book = stream_book is None and now - self._rest_book_ts.get(t, 0) >= 30
        calls = []
        if need_trades:
            calls.append(self.tk.last_trades(inst, minutes=10))
        if need_book:
            calls.append(self.tk.order_book(inst, depth=20))
        replies = await asyncio.gather(*calls, return_exceptions=True) if calls else []
        ri = 0
        rest_trades = None
        rest_book = None
        if need_trades:
            rest_trades = replies[ri]; ri += 1
            if isinstance(rest_trades, list):
                self._rest_trades[t] = rest_trades
                self._rest_trades_ts[t] = now
        if need_book:
            rest_book = replies[ri]; ri += 1
            if isinstance(rest_book, OrderBook):
                self._rest_book_ts[t] = now
        cached = self._rest_trades.get(t, [])
        if stream_trades:
            first_stream = stream_trades[0].ts
            trades = [x for x in cached if x.ts < first_stream] + stream_trades
            self._rest_trades[t] = trades
        else:
            trades = rest_trades if isinstance(rest_trades, (list, BaseException)) else cached
        ob = stream_book or (rest_book if isinstance(rest_book, (OrderBook, BaseException)) else self._last_book.get(t))
        out: list[tape.Signal] = []
        if isinstance(trades, list) and trades:
            trades.sort(key=lambda tr: tr.ts)
            observed = time.time()
            self._observed_until[t] = observed
            history = self._prices.setdefault(t, {})
            history.update((tr.ts.timestamp(), tr.price) for tr in trades)
            self._prices[t] = {ts: p for ts, p in history.items() if observed - ts <= 70 * 60}
            self._prev_price[t] = self._last_price.get(t, 0.0)
            self._last_price[t] = trades[-1].price
            self._last_price_ts[t] = trades[-1].ts.timestamp()
            if self.clusters is not None:
                try:
                    self.clusters.ingest(t, trades)
                except Exception:
                    log.exception("clusters: ingest %s", t)
            base = self._base.setdefault(t, tape.Baseline())
            base.update(trades[-300:], inst.lot)
            if self._main_session() and 0 <= observed - self._last_price_ts[t] <= 90:
                out += tape.analyze_trades(t, trades, inst.lot, dec, base)
                try:
                    out += self._level_signals(t, trades[-1].price, dec)
                    out += self._volume_signal(t, dec)
                    ds = await self._daystats(sess, t)
                    out += self._breakout_signals(t, trades[-1].price, self._prev_price.get(t, 0.0), ds, dec)
                except Exception:
                    log.exception("flow: уровни/объём %s", t)
        elif isinstance(trades, BaseException):
            log.warning("flow: лента %s: %s", t, trades)
        if isinstance(ob, OrderBook):
            self._last_book[t] = ob
            st = self._books.setdefault(t, tape.BookState())
            if self._main_session() and 0 <= time.time() - ob.ts.timestamp() <= 90:
                out += tape.analyze_book(t, ob, st, inst.lot, dec)
        elif isinstance(ob, BaseException):
            log.warning("flow: стакан %s: %s", t, ob)
        ts = time.time()
        if not 0 <= ts - self._last_price_ts.get(t, 0) <= 90:
            return []  # нет свежей стартовой цены для оценки сигнала
        for sig in out:
            sig.detected_at, sig.price, sig.price_ts = ts, self._last_price[t], self._last_price_ts[t]
        return out

    # ------------------------------------------------------------ follow-up
    async def followups(self) -> None:
        now = time.time()
        touched: set[tuple[int, int]] = set()
        # Замер строго к целевому времени. Последняя сделка должна быть
        # не старше 60 с на эту точку; опрос должен уже покрыть её.
        for e in self.journal.pending():
            for cp in CHECKPOINTS:
                key = str(cp)
                target = e.ts + cp * 60
                if key in e.results or key in e.missing or now < target:
                    continue
                points = [(ts, p) for ts, p in self._prices.get(e.ticker, {}).items()
                          if target - 60 <= ts <= target]
                if points and self._observed_until.get(e.ticker, 0) >= target and e.price > 0:
                    ts, price = max(points)
                    e.results[key] = round((price / e.price - 1) * 100, 3)
                    e.result_times[key] = ts
                elif now >= target + 90:
                    e.missing.append(key)
                else:
                    continue
                touched.add((e.chat_id, e.msg_id))
            e.done = all(str(cp) in e.results or str(cp) in e.missing for cp in CHECKPOINTS)
        if not touched:
            return
        self.journal.save()  # данные сохраняются независимо от успеха Telegram edit
        by_msg: Dict[tuple[int, int], list[Entry]] = {}
        for e in self.journal.entries:
            if (e.chat_id, e.msg_id) in touched:
                by_msg.setdefault((e.chat_id, e.msg_id), []).append(e)
        for (chat_id, msg_id), es in by_msg.items():
            first = es[0]
            rows = []
            for e in es:
                parts = []
                for cp in CHECKPOINTS:
                    if str(cp) in e.results:
                        v = e.results[str(cp)]
                        mark = ""
                        if e.direction:
                            mark = " ✅" if v * e.direction >= HIT_PCT else (
                                " ❌" if v * e.direction <= -HIT_PCT else " ➖")
                        parts.append(f"{cp}м: {v:+.2f}%{mark}".replace(".", ","))
                    elif str(cp) in e.missing:
                        parts.append(f"{cp}м: нет свежего замера")
                if parts:
                    from journal import KIND_NAME
                    rows.append(KIND_NAME.get(e.kind, e.kind) + " → " + " · ".join(parts))
            new_text = first.text + "\n\n⏱ Изменение цены от обнаружения:\n" + "\n".join(rows)
            try:
                await self.bot.edit_message_text(new_text, chat_id=chat_id,
                                                 message_id=msg_id,
                                                 reply_markup=flow_kb(first.ticker))
            except TelegramBadRequest as e:
                if "not modified" not in str(e):
                    log.debug("flow: edit follow-up: %s", e)
            except Exception as e:
                log.debug("flow: edit follow-up: %s", e)
