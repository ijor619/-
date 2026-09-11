"""Очередь команд для CScalp-мостика (cscalp_bridge.py на ПК пользователя).

Бот не может достучаться до ПК за NAT, поэтому мостик сам опрашивает бота:
  GET  /cscalp/next?key=…   → {"commands": [{"id", "ticker", "ts"}]}
  POST /cscalp/ack?key=…    ← {"id", "result"}
  GET  /cscalp/status?key=… → когда мостик последний раз выходил на связь

Включается переменной CSCALP_KEY (любая длинная случайная строка); порт —
PORT (Bothost прокидывает его наружу как публичный URL приложения).
Кнопка «⚡ CScalp» показывается только владельцу (OWNER_ID), команды
ставятся в очередь тоже только от него.
"""
from __future__ import annotations

import logging
import os
import secrets
import time
from typing import Optional

from aiohttp import web

log = logging.getLogger(__name__)

CSCALP_KEY = os.getenv("CSCALP_KEY", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
PORT = int(os.getenv("PORT", "8080"))


def enabled() -> bool:
    return bool(CSCALP_KEY)


class CScalpQueue:
    def __init__(self) -> None:
        self.pending: list[dict] = []
        self.last_seen = 0.0          # когда мостик последний раз опрашивал
        self.last_result: Optional[tuple[str, str, float]] = None  # (ticker, result, ts)

    def push(self, ticker: str) -> str:
        cid = secrets.token_hex(4)
        # одна свежая команда важнее очереди старых
        self.pending = [{"id": cid, "ticker": ticker.upper(), "ts": time.time()}]
        return cid

    @property
    def online(self) -> bool:
        return time.time() - self.last_seen < 10

    def status_text(self) -> str:
        if not self.last_seen:
            return "мостик ещё ни разу не выходил на связь"
        ago = time.time() - self.last_seen
        s = f"мостик {'на связи' if self.online else f'не отвечает {ago / 60:.0f} мин'}"
        if self.last_result:
            t, r, ts = self.last_result
            s += f"; последняя команда {t}: {r} ({(time.time() - ts) / 60:.0f} мин назад)"
        return s

    # ----------------------------------------------------------- http
    def _auth(self, req: web.Request) -> bool:
        return bool(CSCALP_KEY) and secrets.compare_digest(req.query.get("key", ""), CSCALP_KEY)

    async def h_next(self, req: web.Request) -> web.Response:
        if not self._auth(req):
            return web.json_response({"error": "forbidden"}, status=403)
        self.last_seen = time.time()
        # команды старше 60 с не выполняем — пользователь уже не ждёт
        cmds = [c for c in self.pending if time.time() - c["ts"] < 60]
        self.pending = []
        return web.json_response({"commands": cmds})

    async def h_ack(self, req: web.Request) -> web.Response:
        if not self._auth(req):
            return web.json_response({"error": "forbidden"}, status=403)
        try:
            d = await req.json()
        except Exception:
            d = {}
        self.last_result = (str(d.get("ticker", "")), str(d.get("result", "")), time.time())
        log.info("cscalp: ack %s", d)
        return web.json_response({"ok": True})

    async def h_status(self, req: web.Request) -> web.Response:
        if not self._auth(req):
            return web.json_response({"error": "forbidden"}, status=403)
        return web.json_response({"online": self.online, "last_seen": self.last_seen})

    async def h_root(self, req: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def start(self) -> Optional[web.AppRunner]:
        app = web.Application()
        app.router.add_get("/", self.h_root)
        app.router.add_get("/cscalp/next", self.h_next)
        app.router.add_post("/cscalp/ack", self.h_ack)
        app.router.add_get("/cscalp/status", self.h_status)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner, "0.0.0.0", PORT).start()
            log.info("cscalp: HTTP-очередь слушает порт %s", PORT)
        except OSError as e:
            log.warning("cscalp: не удалось открыть порт %s: %s", PORT, e)
            return None
        return runner
