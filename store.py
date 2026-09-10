"""Хранилище настроек пользователей (JSON-файл, синхронно — данные крохотные)."""
from __future__ import annotations

import json
import os
from typing import Dict, Iterator


class UserProfile:
    __slots__ = ("watchlist", "threshold_pct", "cooldown_min",
                 "report_min", "last_report_ts")

    def __init__(self) -> None:
        self.watchlist: list[str] = []      # тикеры в верхнем регистре
        self.threshold_pct: float = 5.0     # алерт при |изменении за день| >= N%
        self.cooldown_min: float = 30.0     # пауза между повторными алертами, мин
        self.report_min: float = 60.0       # период сводки, мин (0 = выкл)
        self.last_report_ts: float = 0.0    # unix-время последней сводки

    def to_dict(self) -> dict:
        return {
            "watchlist": self.watchlist,
            "threshold_pct": self.threshold_pct,
            "cooldown_min": self.cooldown_min,
            "report_min": self.report_min,
            "last_report_ts": self.last_report_ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "UserProfile":
        p = cls()
        p.watchlist = [str(t).upper() for t in d.get("watchlist", [])]
        p.threshold_pct = float(d.get("threshold_pct", p.threshold_pct))
        p.cooldown_min = float(d.get("cooldown_min", p.cooldown_min))
        p.report_min = float(d.get("report_min", p.report_min))
        p.last_report_ts = float(d.get("last_report_ts", 0.0))
        return p


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._users: Dict[int, UserProfile] = {}
        self.load()

    def load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return
        for uid, d in raw.items():
            try:
                self._users[int(uid)] = UserProfile.from_dict(d)
            except (TypeError, ValueError):
                continue

    def save(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({str(k): v.to_dict() for k, v in self._users.items()},
                      f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def get(self, user_id: int) -> UserProfile:
        if user_id not in self._users:
            self._users[user_id] = UserProfile()
        return self._users[user_id]

    def all(self) -> Iterator[tuple[int, UserProfile]]:
        return list(self._users.items())
