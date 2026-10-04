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

import market
import moex
import setup as stp
import tape
from journal import CHECKPOINTS, Entry, Journal, HIT_PCT
from keyboards import flow_kb
from formatting import fmt_pct
from moex import now_msk
from store import Store
from tinkoff import OrderBook, TinkoffClient, Trade, enabled

log = logging.getLogger(__name__)

FLOW_INTERVAL_SEC = 30          # опрос ленты/стакана
SIGNAL_COOLDOWN_SEC = 30 * 60   # один и тот же сигнал по бумаге — не чаще
KIND_COOLDOWN_SEC = 10 * 60     # один и тот же ТИП сигнала по бумаге — не чаще
DIGEST_SEC = 300                # сигналы по бумаге копятся и уходят пачкой
MAX_SIGNALS_PER_MSG = 4
DAYSTATS_TTL = 60               # как часто обновлять high/low/VWAP

# направление сигнала для оценки «попаданий»
def _direction(sig: tape.Signal) -> int:
    t = sig.text
    if sig.kind in ("iceberg", "whale", "whale_series", "imbalance", "rhythm"):
        return 1 if "покуп" in t else -1 if "прода" in t else 0
    if sig.kind == "eaten":
        return 1 if "вверх" in t else -1 if "вниз" in t else 0
    if sig.kind == "wall":
        return 1 if "бид" in t else -1      # поддержка -> ждём отскок вверх
    if sig.kind == "spoof":
        return -1 if "на покупку" in t else 1  # ложный бид -> реальный интерес продать
    if sig.kind == "breakout":
        return 1 if "вверх" in t else -1
    return 0


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
                 "hl_hist")

    def __init__(self) -> None:
        self.high = self.low = self.vwap = 0.0
        self.ts = 0.0
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
                 journal: Journal, clusters=None) -> None:
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
        self._last_price: Dict[str, float] = {}
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
        if t not in self._infos:
            try:
                self._infos[t] = await moex.get_security_info(sess, t)
            except Exception:
                return None
        return self._infos[t]

    async def _daystats(self, sess, t: str) -> DayStats:
        ds = self._day.setdefault(t, DayStats())
        if time.time() - ds.ts < DAYSTATS_TTL:
            return ds
        ds.ts = time.time()
        info = await self._info(sess, t)
        if info is None:
            return ds
        try:
            day0 = now_msk().replace(hour=0, minute=0, second=0, microsecond=0)
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
        if near:
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
                key=f"breakout:{name}:{'up' if up else 'down'}"))
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
        first, last = cells[0].price, cells[-1].price
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
        users = [(u, p) for u, p in self.store.all() if p.flow_alerts and p.watchlist]
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
        if not self._main_session():
            return   # вне окна сигналов: лента уже накоплена для кластеров, уведомлений нет
        # «второй факт»: считаем подтверждение один раз на бумагу и направление
        im = await self.imoex(sess)
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
                if sig.kind == "breakout":           # пробой — сам по себе факт у уровня
                    ok = [x for x in ok if not x.startswith("у уровня")]
                    ctx = dict(ctx, conf=len(ok) + 1)
                self._confirmed[id(sig)] = {"facts": ok, "ctx": ctx,
                                            "ok": sig.kind == "breakout" or (d != 0 and len(ok) >= CONFIRM_MIN)}

        for uid, prof in users:
            if prof.is_quiet(hour):
                continue
            for t in prof.watchlist:
                for sig in signals.get(t, []):
                    # авто-фильтр по статистике: слабые пары «бумага × тип» не шлём
                    if self.journal.is_muted(t, sig.kind):
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
            first = self._digest_first.get((uid, t), now)
            has_conf = any(self._confirmed.get(id(s), {}).get("ok") for s in sigs)
            if not has_conf and now - first < DIGEST_SEC and len(sigs) < MAX_SIGNALS_PER_MSG:
                continue
            self._digest[(uid, t)] = []
            self._digest_ts[(uid, t)] = now
            await self._send_signals(sess, uid, t, sigs[:MAX_SIGNALS_PER_MSG])
        if len(self._confirmed) > 2000:
            self._confirmed = {}

        if len(self._sent) > 5000:
            self._sent = {k: v for k, v in self._sent.items() if now - v < 3600}

    async def _send_signals(self, sess, uid: int, t: str,
                            sigs: list[tape.Signal]) -> None:
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
            if c["ok"] and c["facts"]:
                parts.append(s.text + "\n⭐ подтверждено: " + ", ".join(c["facts"]))
            else:
                parts.append(s.text)
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
            return
        if price:
            # журналируем каждый сигнал; результат дописываем к одному сообщению
            for s, c in zip(sigs, confs):
                ctx_d = c["ctx"] or {"hour": now_msk().hour}
                self.journal.add(Entry(ts=time.time(), uid=uid, chat_id=msg.chat.id,
                                       msg_id=msg.message_id, ticker=t, kind=s.kind,
                                       price=price, direction=_direction(s), text=text,
                                       ctx=ctx_d))
                self._recent.setdefault(t, []).append((s.kind, _direction(s), time.time()))

    async def _analyze(self, sess, t: str) -> list[tape.Signal]:
        try:
            inst = await self.tk.instrument(t)
        except Exception as e:
            log.warning("flow: T-Invest недоступен (%s): %s", t, e)
            return []
        if inst is None:
            return []
        info = await self._info(sess, t)
        dec = info.decimals if info else 2
        trades, ob = await asyncio.gather(
            self.tk.last_trades(inst, minutes=10),
            self.tk.order_book(inst, depth=20),
            return_exceptions=True)
        out: list[tape.Signal] = []
        if isinstance(trades, list) and trades:
            self._prev_price[t] = self._last_price.get(t, 0.0)
            self._last_price[t] = trades[-1].price
            if self.clusters is not None:
                try:
                    self.clusters.ingest(t, trades)
                except Exception:
                    log.exception("clusters: ingest %s", t)
            base = self._base.setdefault(t, tape.Baseline())
            base.update(trades[-300:], inst.lot)
            if self._main_session():
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
            if ob.last:
                self._last_price.setdefault(t, ob.last)
            st = self._books.setdefault(t, tape.BookState())
            if self._main_session():
                out += tape.analyze_book(t, ob, st, inst.lot, dec)
        elif isinstance(ob, BaseException):
            log.warning("flow: стакан %s: %s", t, ob)
        return out

    # ------------------------------------------------------------ follow-up
    async def followups(self) -> None:
        now = time.time()
        touched = False
        # группируем по сообщению — одно редактирование на сообщение
        by_msg: Dict[tuple[int, int], list[Entry]] = {}
        for e in self.journal.pending():
            by_msg.setdefault((e.chat_id, e.msg_id), []).append(e)
        for (chat_id, msg_id), es in by_msg.items():
            first = es[0]
            age_min = (now - first.ts) / 60
            price = self._last_price.get(first.ticker)
            if not price or not first.price:
                continue
            changed = False
            for cp in CHECKPOINTS:
                if age_min >= cp and str(cp) not in first.results:
                    chg = (price - first.price) / first.price * 100
                    for e in es:
                        e.results[str(cp)] = round(chg, 3)
                    changed = True
            if not changed:
                continue
            touched = True
            if all(str(cp) in first.results for cp in CHECKPOINTS):
                for e in es:
                    e.done = True
            parts = []
            for cp in CHECKPOINTS:
                if str(cp) in first.results:
                    v = first.results[str(cp)]
                    mark = ""
                    if first.direction:
                        mark = " ✅" if v * first.direction >= HIT_PCT else (
                            " ❌" if v * first.direction <= -HIT_PCT else " ➖")
                    parts.append(f"{cp}м: {v:+.2f}%{mark}".replace(".", ","))
            new_text = first.text + "\n\n⏱ После сигнала → " + " · ".join(parts)
            try:
                await self.bot.edit_message_text(new_text, chat_id=chat_id,
                                                 message_id=msg_id,
                                                 reply_markup=flow_kb(first.ticker))
            except TelegramBadRequest as e:
                if "not modified" not in str(e):
                    log.debug("flow: edit follow-up: %s", e)
            except Exception as e:
                log.debug("flow: edit follow-up: %s", e)
        if touched:
            self.journal.save()
