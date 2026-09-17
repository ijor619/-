"""Журнал сигналов: что было отправлено и что стало с ценой потом.

Каждый сигнал записывается с ценой на момент отправки; через 5 и 15 минут
монитор дописывает результат к сообщению в Telegram и сохраняет в журнал.
По журналу считается статистика /stats — какие сигналы по каким бумагам
реально что-то предсказывают, а какие шум.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

KIND_EMOJI = {
    "iceberg": "🧊", "rhythm": "🤖", "imbalance": "⚖️", "burst": "🔥",
    "wall": "🧱", "spoof": "👻", "whale": "🐋", "eaten": "🍽",
    "level": "📏", "volume": "📊", "whale_series": "🐋🐋",
}
KIND_NAME = {
    "iceberg": "айсберг", "rhythm": "ритм", "imbalance": "перекос",
    "burst": "всплеск", "wall": "плотность", "spoof": "спуфинг",
    "whale": "кит", "eaten": "стену съели", "level": "уровень", "volume": "объём", "whale_series": "серия китов",
}
# ожидаемое направление сигнала: +1 — рост, -1 — падение, 0 — неизвестно
CHECKPOINTS = (5, 15)  # минуты
# «попадание»: ход в сторону сигнала через 15 мин не меньше HIT_PCT.
# 0.2% для ликвидных бумаг — это обычный шум, поэтому 0.4%.
HIT_PCT = 0.4
# авто-отключение пары «бумага × тип»: n ≥ MUTE_MIN_N за MUTE_DAYS дней и попадание < MUTE_HIT
MUTE_MIN_N = 30
MUTE_DAYS = 7
MUTE_HIT = 25.0
# типы без направления — оценить нельзя, поэтому не показываем
UNDIRECTED = {"volume", "level", "burst"}


@dataclass
class Entry:
    ts: float
    uid: int
    chat_id: int
    msg_id: int
    ticker: str
    kind: str
    price: float
    direction: int = 0                          # +1 / -1 / 0
    text: str = ""                              # исходный текст (HTML)
    results: dict = field(default_factory=dict)  # "5": +0.42, "15": -0.1
    done: bool = False
    ctx: dict = field(default_factory=dict)      # контекст: vwap (+1/-1), hour, delta15 (+1/0/-1), rs


class Journal:
    def __init__(self, path: str) -> None:
        self.path = path
        self.entries: list[Entry] = []
        self._load()

    # ----------------------------------------------------------- хранение
    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
            self.entries = [Entry(**e) for e in raw]
        except (FileNotFoundError, json.JSONDecodeError, TypeError):
            self.entries = []

    def save(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        # храним 14 дней
        cutoff = time.time() - 14 * 86400
        self.entries = [e for e in self.entries if e.ts >= cutoff]
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump([asdict(e) for e in self.entries], f, ensure_ascii=False)
        os.replace(tmp, self.path)

    # ----------------------------------------------------------- операции
    def add(self, e: Entry) -> None:
        self.entries.append(e)
        self.save()

    def pending(self) -> list[Entry]:
        return [e for e in self.entries if not e.done]

    # ---------------------------------------------------------- статистика
    def stats(self, uid: int, days: int = 7,
              ticker: Optional[str] = None) -> str:
        cutoff = time.time() - days * 86400
        es = [e for e in self.entries
              if e.uid == uid and e.ts >= cutoff and (not ticker or e.ticker == ticker)]
        if not es:
            return "Сигналов за период нет."
        by: dict[tuple[str, str], list[Entry]] = {}
        for e in es:
            by.setdefault((e.ticker, e.kind), []).append(e)

        lines = [f"📈 <b>Статистика сигналов</b> за {days} дн."
                 + (f" · {ticker}" if ticker else "")
                 + f" · всего {len(es)}"]
        lines.append("<pre>")
        lines.append(f"{'бумага':<6} {'сигнал':<12} {'n':>3} {'15м ср.':>8} {'попад.':>6}")
        for (t, k), grp in sorted(by.items(), key=lambda kv: -len(kv[1])):
            r15 = [e.results.get("15") for e in grp if "15" in e.results]
            avg = sum(r15) / len(r15) if r15 else None
            # «попадание» — движение ≥0,2% в сторону сигнала (если направление есть)
            hits = [e for e in grp if "15" in e.results and e.direction
                    and e.results["15"] * e.direction >= HIT_PCT]
            directed = [e for e in grp if "15" in e.results and e.direction]
            hit_s = f"{len(hits) / len(directed) * 100:.0f}%" if directed else "—"
            avg_s = f"{avg:+.2f}%" if avg is not None else "—"
            mark = "🔇" if self.is_muted(t, k) else " "
            lines.append(f"{mark}{t:<6} {KIND_EMOJI.get(k, '')}{KIND_NAME.get(k, k):<11} "
                         f"{len(grp):>3} {avg_s:>8} {hit_s:>6}")
        lines.append("</pre>")
        lines.append("<i>15м ср. — среднее изменение цены через 15 мин после сигнала; "
                     f"попад. — доля случаев, когда цена пошла в сторону сигнала на ≥{HIT_PCT:.1f}%. "
                     "🔇 — пара отключена автоматически (n≥30, попадание <25%).</i>")
        ctx = self._ctx_stats(es)
        if ctx:
            lines.append("")
            lines.append(ctx)
        return "\n".join(lines)

    # ------------------------------------------------- качество и авто-мьют
    def quality(self, ticker: str, kind: str, days: int = MUTE_DAYS) -> tuple[int, Optional[float]]:
        """(n оценённых направленных сигналов, попадание %) по всем пользователям."""
        cutoff = time.time() - days * 86400
        es = [e for e in self.entries if e.ticker == ticker and e.kind == kind
              and e.ts >= cutoff and e.direction and "15" in e.results]
        if not es:
            return 0, None
        hits = sum(1 for e in es if e.results["15"] * e.direction >= HIT_PCT)
        return len(es), hits / len(es) * 100

    def is_muted(self, ticker: str, kind: str) -> bool:
        """Пара отключена: тип без направления, либо статистика плохая."""
        if kind in UNDIRECTED:
            return True
        n, hit = self.quality(ticker, kind)
        return n >= MUTE_MIN_N and hit is not None and hit < MUTE_HIT

    def mutes_text(self, tickers: list[str]) -> str:
        rows = []
        for t in tickers:
            for k in KIND_NAME:
                if k in UNDIRECTED:
                    continue
                n, hit = self.quality(t, k)
                if n == 0:
                    continue
                st = "🔇" if self.is_muted(t, k) else "✅"
                rows.append(f"{st} {t:<6} {KIND_EMOJI.get(k, '')}{KIND_NAME[k]:<12} n={n:<4} {hit:>3.0f}%")
        if not rows:
            return "Пока нет оценённых сигналов."
        return ("<b>Авто-фильтр сигналов</b> (7 дн., порог хода "
                f"{HIT_PCT:.1f}%)\n<pre>" + "\n".join(rows) + "</pre>\n"
                "<i>🔇 — отключено (n≥30, попадание <25%). Пары с малой выборкой продолжают "
                f"приходить, пока не наберут {MUTE_MIN_N} сигналов. Объём/уровень/всплеск "
                "выключены целиком — направления нет, оценить нельзя.</i>")

    def _ctx_stats(self, es: list[Entry]) -> str:
        """В каких условиях сигналы работают: по VWAP, времени дня, дельте, силе к рынку."""
        directed = [e for e in es if "15" in e.results and e.direction and e.ctx]
        if len(directed) < 8:
            return ""

        def _grp(key: str, labeler) -> list[tuple[str, int, float, float]]:
            by: dict[str, list[Entry]] = {}
            for e in directed:
                v = e.ctx.get(key)
                if v is None:
                    continue
                by.setdefault(labeler(e, v), []).append(e)
            rows = []
            for lab, grp in by.items():
                if len(grp) < 4:
                    continue
                hits = sum(1 for e in grp if e.results["15"] * e.direction >= HIT_PCT)
                avg = sum(e.results["15"] * e.direction for e in grp) / len(grp)
                rows.append((lab, len(grp), hits / len(grp) * 100, avg))
            return sorted(rows, key=lambda r: -r[2])

        def _hour(e, v):
            return "утро 07–10" if v < 10 else "10–13" if v < 13 else "13–16" if v < 16 else "16–19" if v < 19 else "вечер 19+"

        sections = [
            ("По VWAP", _grp("vwap", lambda e, v: ("сигнал по тренду VWAP" if v * e.direction > 0 else "сигнал против VWAP"))),
            ("По дельте 15м", _grp("delta15", lambda e, v: ("дельта согласна" if v * e.direction > 0 else "дельта против" if v * e.direction < 0 else "дельта нейтральна"))),
            ("По силе к рынку", _grp("rs", lambda e, v: ("сильнее рынка" if v > 0 else "слабее рынка" if v < 0 else "с рынком"))),
            ("По времени дня", _grp("hour", _hour)),
        ]
        out = ["<b>В каких условиях сигналы работали</b>", "<pre>"]
        any_rows = False
        for title, rows in sections:
            if not rows:
                continue
            any_rows = True
            out.append(title)
            for lab, n, hit, avg in rows:
                out.append(f"  {lab:<24} n={n:<3} {hit:>3.0f}%  {avg:+.2f}%")
        out.append("</pre>")
        out.append("<i>% — попадания; последняя колонка — средний ход в сторону сигнала через 15 мин.</i>")
        return "\n".join(out) if any_rows else ""
