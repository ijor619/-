"""Календарь событий: дивидендные отсечки (T-Invest), заседания ЦБ по ставке,
отчётности и прочее из ручного списка (data/events.json, /event).

/calendar — ближайшие 14 дней по бумагам из списка.
Напоминание в 09:35 МСК: события сегодня и завтра.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot

log = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
REMIND_HOUR, REMIND_MIN = 9, 35
HORIZON_DAYS = 14
DIV_TTL = 6 * 3600

# Заседания Совета директоров Банка России по ключевой ставке (официальный график).
CBR_MEETINGS = {
    "2026-10-23": "ЦБ: ставка (опорное, прогноз, пресс-конференция)",
    "2026-12-18": "ЦБ: ставка (пресс-конференция)",
    # 2027 — добавить, когда ЦБ опубликует график (обычно в сентябре)
}

# Стартовый список отчётностей (3 кв. 2026, по данным календарей инвестора; даты
# «ожидаемые» — эмитенты иногда сдвигают). Пользователь правит через /event.
SEED_EVENTS = [
    ("2026-10-21", "PLZL", "Полюс: операционные результаты 3 кв. (ожид.)"),
    ("2026-10-23", "SMLT", "Самолёт: операционные результаты 3 кв. (ожид.)"),
    ("2026-10-28", "OZON", "Ozon: МСФО 3 кв. (ожид.)"),
    ("2026-10-29", "YDEX", "Яндекс: МСФО 3 кв. (ожид.)"),
    ("2026-10-30", "LKOH", "Лукойл: МСФО 3 кв. (ожид.)"),
    ("2026-11-30", "SBER", "Сбербанк: МСФО 3 кв. (ожид., уточни дату)"),
]


@dataclass
class Event:
    date: str           # YYYY-MM-DD
    ticker: str         # "" — общее (ЦБ, макро)
    text: str
    kind: str = "manual"   # manual / div / cbr
    id: int = 0


class Calendar:
    def __init__(self, path: str, tk=None) -> None:
        self.path = path
        self.tk = tk
        self.manual: list[Event] = []
        self._next_id = 1
        self._div: dict[str, list[Event]] = {}
        self._div_ts: dict[str, float] = {}
        self._load()

    # ------------------------------------------------------------ хранение
    def _load(self) -> None:
        if os.path.exists(self.path):
            try:
                with open(self.path, encoding="utf-8") as f:
                    d = json.load(f)
                self.manual = [Event(**e) for e in d.get("events", [])]
                self._next_id = int(d.get("next_id", 1))
                return
            except Exception as e:
                log.warning("events: не прочитан %s: %s", self.path, e)
        for dt, t, txt in SEED_EVENTS:
            self.manual.append(Event(dt, t, txt, "manual", self._next_id))
            self._next_id += 1
        self.save()

    def save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"events": [asdict(e) for e in self.manual], "next_id": self._next_id},
                          f, ensure_ascii=False, indent=1)
        except Exception as e:
            log.warning("events: не сохранён: %s", e)

    # ------------------------------------------------------------ ручные
    def add(self, dt: str, ticker: str, text: str) -> Event:
        e = Event(dt, ticker.upper(), text.strip(), "manual", self._next_id)
        self._next_id += 1
        self.manual.append(e)
        self.save()
        return e

    def remove(self, eid: int) -> bool:
        n = len(self.manual)
        self.manual = [e for e in self.manual if e.id != eid]
        if len(self.manual) != n:
            self.save()
            return True
        return False

    # ------------------------------------------------------------ дивиденды
    async def dividends(self, ticker: str) -> list[Event]:
        if self.tk is None:
            return []
        t = ticker.upper()
        if time.time() - self._div_ts.get(t, 0) < DIV_TTL:
            return self._div.get(t, [])
        self._div_ts[t] = time.time()
        out: list[Event] = []
        try:
            inst = await self.tk.instrument(t)
            if inst is None:
                return []
            today = datetime.now(timezone.utc)
            d = await self.tk._call("InstrumentsService/GetDividends", {
                "instrumentId": inst.uid,
                "from": (today - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z"),
                "to": (today + timedelta(days=120)).strftime("%Y-%m-%dT00:00:00Z"),
            })
            for dv in d.get("dividends", []):
                rec = (dv.get("recordDate") or "")[:10]
                lb = (dv.get("lastBuyDate") or "")[:10]
                amt = dv.get("dividendNet") or {}
                val = float(amt.get("units") or 0) + float(amt.get("nano") or 0) / 1e9
                yld = dv.get("yieldValue") or {}
                y = float(yld.get("units") or 0) + float(yld.get("nano") or 0) / 1e9
                base = f"{t}: дивиденд {val:g} ₽" + (f" ({y:.1f}%)" if y else "")
                if lb:
                    out.append(Event(lb, t, base + " — последний день покупки", "div"))
                if rec and rec != lb:
                    out.append(Event(rec, t, base + " — отсечка (запись в реестр)", "div"))
        except Exception as e:
            log.debug("events: дивиденды %s: %s", t, e)
        self._div[t] = out
        return out

    # ------------------------------------------------------------ выборка
    async def upcoming(self, tickers: list[str], days: int = HORIZON_DAYS,
                       start: Optional[date] = None) -> list[Event]:
        start = start or datetime.now(MSK).date()
        end = start + timedelta(days=days)
        tick = {t.upper() for t in tickers}
        evs: list[Event] = []
        for dt, txt in CBR_MEETINGS.items():
            evs.append(Event(dt, "", txt, "cbr"))
        evs += [e for e in self.manual if not e.ticker or e.ticker in tick]
        divs = await asyncio.gather(*(self.dividends(t) for t in sorted(tick)), return_exceptions=True)
        for r in divs:
            if isinstance(r, list):
                evs += r
        out = []
        for e in evs:
            try:
                d = date.fromisoformat(e.date)
            except ValueError:
                continue
            if start <= d <= end:
                out.append(e)
        out.sort(key=lambda e: (e.date, e.kind, e.ticker))
        return out

    # ------------------------------------------------------------ текст
    @staticmethod
    def _dlabel(d: date, today: date) -> str:
        wd = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"][d.weekday()]
        base = f"{d.day:02d}.{d.month:02d} {wd}"
        if d == today:
            return base + " — сегодня"
        if d == today + timedelta(days=1):
            return base + " — завтра"
        return base

    def text(self, evs: list[Event], tickers: list[str], esc, days: int = HORIZON_DAYS) -> str:
        today = datetime.now(MSK).date()
        head = f"📅 <b>Календарь на {days} дн.</b>"
        if not evs:
            return (head + f"\n\nПо {esc(', '.join(tickers)) or 'списку'} событий нет.\n"
                    "<i>Добавить: /event add SBER 2026-11-05 МСФО 3 кв.</i>")
        lines = [head, ""]
        cur = ""
        icon = {"div": "💰", "cbr": "🏦", "manual": "📌"}
        for e in evs:
            if e.date != cur:
                cur = e.date
                lines.append(f"<b>{self._dlabel(date.fromisoformat(e.date), today)}</b>")
            tag = f"#{e.id} " if e.kind == "manual" and e.id else ""
            lines.append(f"  {icon.get(e.kind, '•')} {esc(e.text)} <i>{tag}</i>".rstrip())
        lines.append("")
        lines.append("<i>💰 дивиденды (T-Invest) · 🏦 ЦБ · 📌 вручную. "
                     "/event add ТИКЕР ГГГГ-ММ-ДД текст · /event del N</i>")
        return "\n".join(lines)

    # ------------------------------------------------------------ напоминание
    async def run_reminders(self, bot: Bot, store, esc) -> None:
        """Каждый день в 09:35 МСК: события сегодня/завтра по списку пользователя."""
        await asyncio.sleep(20)
        last_day = ""
        while True:
            try:
                now = datetime.now(MSK)
                key = now.strftime("%Y-%m-%d")
                if (now.hour, now.minute) >= (REMIND_HOUR, REMIND_MIN) and key != last_day \
                        and now.weekday() < 5:
                    last_day = key
                    for uid, prof in store.all():
                        if not prof.watchlist:
                            continue
                        evs = await self.upcoming(prof.watchlist, days=1)
                        if not evs:
                            continue
                        txt = self.text(evs, prof.watchlist, esc, days=1).replace(
                            "Календарь на 1 дн.", "События сегодня и завтра")
                        try:
                            await bot.send_message(uid, txt)
                        except Exception as e:
                            log.warning("events: напоминание %s: %s", uid, e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("events: %s", e)
            await asyncio.sleep(60)
