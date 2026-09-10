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
}
KIND_NAME = {
    "iceberg": "айсберг", "rhythm": "ритм", "imbalance": "перекос",
    "burst": "всплеск", "wall": "плотность", "spoof": "спуфинг",
    "whale": "кит", "eaten": "стену съели",
}
# ожидаемое направление сигнала: +1 — рост, -1 — падение, 0 — неизвестно
CHECKPOINTS = (5, 15)  # минуты


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
                    and e.results["15"] * e.direction >= 0.2]
            directed = [e for e in grp if "15" in e.results and e.direction]
            hit_s = f"{len(hits) / len(directed) * 100:.0f}%" if directed else "—"
            avg_s = f"{avg:+.2f}%" if avg is not None else "—"
            lines.append(f"{t:<6} {KIND_EMOJI.get(k, '')}{KIND_NAME.get(k, k):<11} "
                         f"{len(grp):>3} {avg_s:>8} {hit_s:>6}")
        lines.append("</pre>")
        lines.append("<i>15м ср. — среднее изменение цены через 15 мин после сигнала; "
                     "попад. — доля случаев, когда цена пошла в сторону сигнала на ≥0,2%.</i>")
        return "\n".join(lines)
