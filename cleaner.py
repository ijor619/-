"""Ежедневная очистка чата: бот удаляет свои сообщения в личках.

Ограничения Telegram: бот удаляет только СВОИ сообщения и только не старше
48 часов; сообщения пользователя (команды) удалить нельзя.

Учёт исходящих — через session middleware aiogram: перехватываются ответы
всех send_*/copy/forward методов, поэтому ни один вызов в коде править не
нужно. Хранится в data/sent.json (переживает рестарт). Канал новостей и
группы не трогаем — только private-чаты.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import Bot
from aiogram.client.session.middlewares.base import BaseRequestMiddleware, NextRequestMiddlewareType
from aiogram.methods import TelegramMethod
from aiogram.methods.base import Response, TelegramType
from aiogram.types import Message

log = logging.getLogger(__name__)

MAX_AGE_H = 47.5          # лимит Telegram — 48 ч, оставляем запас
DEFAULT_HOUR = 3          # МСК


class SentLog:
    """chat_id -> [(message_id, ts)]"""

    def __init__(self, path: str) -> None:
        self.path = path
        self.data: Dict[str, list] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception:
            self.data = {}

    def save(self) -> None:
        if not self._dirty:
            return
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f)
            os.replace(tmp, self.path)
            self._dirty = False
        except Exception:
            log.exception("cleaner: save")

    def add(self, chat_id: int, message_id: int) -> None:
        lst = self.data.setdefault(str(chat_id), [])
        lst.append([message_id, time.time()])
        if len(lst) > 5000:
            del lst[: len(lst) - 5000]
        self._dirty = True

    def take(self, chat_id: int, older_than_sec: float = 0.0,
             keep: Optional[set] = None) -> list[int]:
        """Забрать id сообщений чата для удаления (и убрать из учёта)."""
        now = time.time()
        lst = self.data.get(str(chat_id), [])
        out, rest = [], []
        for mid, ts in lst:
            if now - ts > MAX_AGE_H * 3600:
                continue                      # уже не удалить — забываем
            if now - ts >= older_than_sec and not (keep and mid in keep):
                out.append(mid)
            else:
                rest.append([mid, ts])
        self.data[str(chat_id)] = rest
        self._dirty = True
        return out


class TrackOutgoing(BaseRequestMiddleware):
    """Записывает id всех сообщений, отправленных ботом в личные чаты."""

    def __init__(self, sent: SentLog) -> None:
        self.sent = sent

    async def __call__(self, make_request: NextRequestMiddlewareType[TelegramType],
                       bot: Bot, method: TelegramMethod[TelegramType]) -> Response[TelegramType]:
        resp = await make_request(bot, method)
        try:
            r = resp.result
            msgs = r if isinstance(r, list) else [r]
            for m in msgs:
                if isinstance(m, Message) and m.chat.type == "private":
                    self.sent.add(m.chat.id, m.message_id)
        except Exception:
            pass
        return resp


async def clear_chat(bot: Bot, sent: SentLog, chat_id: int,
                     older_than_sec: float = 0.0, keep: Optional[set] = None) -> int:
    ids = sent.take(chat_id, older_than_sec, keep)
    deleted = 0
    # delete_messages принимает до 100 id за раз
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            await bot.delete_messages(chat_id, chunk)
            deleted += len(chunk)
        except Exception:
            # часть могла быть удалена вручную — добиваем по одному
            for mid in chunk:
                try:
                    await bot.delete_message(chat_id, mid)
                    deleted += 1
                except Exception:
                    pass
        await asyncio.sleep(0.2)
    sent.save()
    return deleted


async def run_daily(bot: Bot, sent: SentLog, store, now_msk: Callable[[], Any]) -> None:
    """Раз в минуту проверяем: у кого включена автоочистка и наступил её час."""
    last_day: Dict[int, str] = {}
    while True:
        try:
            n = now_msk()
            for uid, prof in list(store.all()):
                hour = getattr(prof, "clear_hour", -1)
                if hour < 0 or n.hour != hour:
                    continue
                key = n.strftime("%Y-%m-%d")
                if last_day.get(uid) == key:
                    continue
                last_day[uid] = key
                d = await clear_chat(bot, sent, uid)
                log.info("cleaner: %s — удалено %d сообщений", uid, d)
            sent.save()
        except Exception:
            log.exception("cleaner: tick")
        await asyncio.sleep(60)
