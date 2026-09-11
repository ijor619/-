"""Клиент T-Invest API (REST/JSON) — стакан и лента обезличенных сделок.

Только чтение: используются MarketDataService и InstrumentsService.
Токен берётся из переменной окружения TINKOFF_TOKEN и никуда не логируется.
Сертификаты Минцифры (certs/) нужны, т.к. invest-public-api.tinkoff.ru
подписан российским CA, которого нет в системных хранилищах вне РФ.
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp

import config  # noqa: F401  (подгружает .env до чтения токена)

log = logging.getLogger(__name__)

BASE = os.getenv("TINKOFF_API_BASE", "https://invest-public-api.tbank.ru").rstrip("/") \
    + "/rest/tinkoff.public.invest.api.contract.v1."
TOKEN = os.getenv("TINKOFF_TOKEN", "").strip()
CERTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")


def enabled() -> bool:
    return bool(TOKEN)


# Корневой и промежуточный сертификаты Минцифры РФ (Russian Trusted CA).
# Встроены прямо в код, чтобы деплой не зависел от наличия папки certs/.
_RU_CA_PEM = """
-----BEGIN CERTIFICATE-----
MIIFwjCCA6qgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwcDELMAkGA1UEBhMCUlUx
PzA9BgNVBAoMNlRoZSBNaW5pc3RyeSBvZiBEaWdpdGFsIERldmVsb3BtZW50IGFu
ZCBDb21tdW5pY2F0aW9uczEgMB4GA1UEAwwXUnVzc2lhbiBUcnVzdGVkIFJvb3Qg
Q0EwHhcNMjIwMzAxMjEwNDE1WhcNMzIwMjI3MjEwNDE1WjBwMQswCQYDVQQGEwJS
VTE/MD0GA1UECgw2VGhlIE1pbmlzdHJ5IG9mIERpZ2l0YWwgRGV2ZWxvcG1lbnQg
YW5kIENvbW11bmljYXRpb25zMSAwHgYDVQQDDBdSdXNzaWFuIFRydXN0ZWQgUm9v
dCBDQTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMfFOZ8pUAL3+r2n
qqE0Zp52selXsKGFYoG0GM5bwz1bSFtCt+AZQMhkWQheI3poZAToYJu69pHLKS6Q
XBiwBC1cvzYmUYKMYZC7jE5YhEU2bSL0mX7NaMxMDmH2/NwuOVRj8OImVa5s1F4U
zn4Kv3PFlDBjjSjXKVY9kmjUBsXQrIHeaqmUIsPIlNWUnimXS0I0abExqkbdrXbX
YwCOXhOO2pDUx3ckmJlCMUGacUTnylyQW2VsJIyIGA8V0xzdaeUXg0VZ6ZmNUr5Y
Ber/EAOLPb8NYpsAhJe2mXjMB/J9HNsoFMBFJ0lLOT/+dQvjbdRZoOT8eqJpWnVD
U+QL/qEZnz57N88OWM3rabJkRNdU/Z7x5SFIM9FrqtN8xewsiBWBI0K6XFuOBOTD
4V08o4TzJ8+Ccq5XlCUW2L48pZNCYuBDfBh7FxkB7qDgGDiaftEkZZfApRg2E+M9
G8wkNKTPLDc4wH0FDTijhgxR3Y4PiS1HL2Zhw7bD3CbslmEGgfnnZojNkJtcLeBH
BLa52/dSwNU4WWLubaYSiAmA9IUMX1/RpfpxOxd4Ykmhz97oFbUaDJFipIggx5sX
ePAlkTdWnv+RWBxlJwMQ25oEHmRguNYf4Zr/Rxr9cS93Y+mdXIZaBEE0KS2iLRqa
OiWBki9IMQU4phqPOBAaG7A+eP8PAgMBAAGjZjBkMB0GA1UdDgQWBBTh0YHlzlpf
BKrS6badZrHF+qwshzAfBgNVHSMEGDAWgBTh0YHlzlpfBKrS6badZrHF+qwshzAS
BgNVHRMBAf8ECDAGAQH/AgEEMA4GA1UdDwEB/wQEAwIBhjANBgkqhkiG9w0BAQsF
AAOCAgEAALIY1wkilt/urfEVM5vKzr6utOeDWCUczmWX/RX4ljpRdgF+5fAIS4vH
tmXkqpSCOVeWUrJV9QvZn6L227ZwuE15cWi8DCDal3Ue90WgAJJZMfTshN4OI8cq
W9E4EG9wglbEtMnObHlms8F3CHmrw3k6KmUkWGoa+/ENmcVl68u/cMRl1JbW2bM+
/3A+SAg2c6iPDlehczKx2oa95QW0SkPPWGuNA/CE8CpyANIhu9XFrj3RQ3EqeRcS
AQQod1RNuHpfETLU/A2gMmvn/w/sx7TB3W5BPs6rprOA37tutPq9u6FTZOcG1Oqj
C/B7yTqgI7rbyvox7DEXoX7rIiEqyNNUguTk/u3SZ4VXE2kmxdmSh3TQvybfbnXV
4JbCZVaqiZraqc7oZMnRoWrXRG3ztbnbes/9qhRGI7PqXqeKJBztxRTEVj8ONs1d
WN5szTwaPIvhkhO3CO5ErU2rVdUr89wKpNXbBODFKRtgxUT70YpmJ46VVaqdAhOZ
D9EUUn4YaeLaS8AjSF/h7UkjOibNc4qVDiPP+rkehFWM66PVnP1Msh93tc+taIfC
EYVMxjh8zNbFuoc7fzvvrFILLe7ifvEIUqSVIC/AzplM/Jxw7buXFeGP1qVCBEHq
391d/9RAfaZ12zkwFsl+IKwE/OZxW8AHa9i1p4GO0YSNuczzEm4=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIHQjCCBSqgAwIBAgICEAIwDQYJKoZIhvcNAQELBQAwcDELMAkGA1UEBhMCUlUx
PzA9BgNVBAoMNlRoZSBNaW5pc3RyeSBvZiBEaWdpdGFsIERldmVsb3BtZW50IGFu
ZCBDb21tdW5pY2F0aW9uczEgMB4GA1UEAwwXUnVzc2lhbiBUcnVzdGVkIFJvb3Qg
Q0EwHhcNMjIwMzAyMTEyNTE5WhcNMjcwMzA2MTEyNTE5WjBvMQswCQYDVQQGEwJS
VTE/MD0GA1UECgw2VGhlIE1pbmlzdHJ5IG9mIERpZ2l0YWwgRGV2ZWxvcG1lbnQg
YW5kIENvbW11bmljYXRpb25zMR8wHQYDVQQDDBZSdXNzaWFuIFRydXN0ZWQgU3Vi
IENBMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA9YPqBKOk19NFymrE
wehzrhBEgT2atLezpduB24mQ7CiOa/HVpFCDRZzdxqlh8drku408/tTmWzlNH/br
HuQhZ/miWKOf35lpKzjyBd6TPM23uAfJvEOQ2/dnKGGJbsUo1/udKSvxQwVHpVv3
S80OlluKfhWPDEXQpgyFqIzPoxIQTLZ0deirZwMVHarZ5u8HqHetRuAtmO2ZDGQn
vVOJYAjls+Hiueq7Lj7Oce7CQsTwVZeP+XQx28PAaEZ3y6sQEt6rL06ddpSdoTMp
BnCqTbxW+eWMyjkIn6t9GBtUV45yB1EkHNnj2Ex4GwCiN9T84QQjKSr+8f0psGrZ
vPbCbQAwNFJjisLixnjlGPLKa5vOmNwIh/LAyUW5DjpkCx004LPDuqPpFsKXNKpa
L2Dm6uc0x4Jo5m+gUTVORB6hOSzWnWDj2GWfomLzzyjG81DRGFBpco/O93zecsIN
3SL2Ysjpq1zdoS01CMYxie//9zWvYwzI25/OZigtnpCIrcd2j1Y6dMUFQAzAtHE+
qsXflSL8HIS+IJEFIQobLlYhHkoE3avgNx5jlu+OLYe0dF0Ykx1PGNjbwqvTX37R
Cn32NMjlotW2QcGEZhDKj+3urZizp5xdTPZitA+aEjZM/Ni71VOdiOP0igbw6asZ
2fxdozZ1TnSSYNYvNATwthNmZysCAwEAAaOCAeUwggHhMBIGA1UdEwEB/wQIMAYB
Af8CAQAwDgYDVR0PAQH/BAQDAgGGMB0GA1UdDgQWBBTR4XENCy2BTm6KSo9MI7NM
XqtpCzAfBgNVHSMEGDAWgBTh0YHlzlpfBKrS6badZrHF+qwshzCBxwYIKwYBBQUH
AQEEgbowgbcwOwYIKwYBBQUHMAKGL2h0dHA6Ly9yb3N0ZWxlY29tLnJ1L2NkcC9y
b290Y2Ffc3NsX3JzYTIwMjIuY3J0MDsGCCsGAQUFBzAChi9odHRwOi8vY29tcGFu
eS5ydC5ydS9jZHAvcm9vdGNhX3NzbF9yc2EyMDIyLmNydDA7BggrBgEFBQcwAoYv
aHR0cDovL3JlZXN0ci1wa2kucnUvY2RwL3Jvb3RjYV9zc2xfcnNhMjAyMi5jcnQw
gbAGA1UdHwSBqDCBpTA1oDOgMYYvaHR0cDovL3Jvc3RlbGVjb20ucnUvY2RwL3Jv
b3RjYV9zc2xfcnNhMjAyMi5jcmwwNaAzoDGGL2h0dHA6Ly9jb21wYW55LnJ0LnJ1
L2NkcC9yb290Y2Ffc3NsX3JzYTIwMjIuY3JsMDWgM6Axhi9odHRwOi8vcmVlc3Ry
LXBraS5ydS9jZHAvcm9vdGNhX3NzbF9yc2EyMDIyLmNybDANBgkqhkiG9w0BAQsF
AAOCAgEARBVzZls79AdiSCpar15dA5Hr/rrT4WbrOfzlpI+xrLeRPrUG6eUWIW4v
Sui1yx3iqGLCjPcKb+HOTwoRMbI6ytP/ndp3TlYua2advYBEhSvjs+4vDZNwXr/D
anbwIWdurZmViQRBDFebpkvnIvru/RpWud/5r624Wp8voZMRtj/cm6aI9LtvBfT9
cfzhOaexI/99c14dyiuk1+6QhdwKaCRTc1mdfNQmnfWNRbfWhWBlK3h4GGE9JK33
Gk8ZS8DMrkdAh0xby4xAQ/mSWAfWrBmfzlOqGyoB1U47WTOeqNbWkkoAP2ys94+s
Jg4NTkiDVtXRF6nr6fYi0bSOvOFg0IQrMXO2Y8gyg9ARdPJwKtvWX8VPADCYMiWH
h4n8bZokIrImVKLDQKHY4jCsND2HHdJfnrdL2YJw1qFskNO4cSNmZydw0Wkgjv9k
F+KxqrDKlB8MZu2Hclph6v/CZ0fQ9YuE8/lsHZ0Qc2HyiSMnvjgK5fDc3TD4fa8F
E8gMNurM+kV8PT8LNIM+4Zs+LKEV8nqRWBaxkIVJGekkVKO8xDBOG/aN62AZKHOe
GcyIdu7yNMMRihGVZCYr8rYiJoKiOzDqOkPkLOPdhtVlgnhowzHDxMHND/E2WA5p
ZHuNM/m0TXt2wTTPL7JH2YC0gPz/BvvSzjksgzU5rLbRyUKQkgU=
-----END CERTIFICATE-----
"""


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        ctx.load_verify_locations(cadata=_RU_CA_PEM)
    except ssl.SSLError as e:
        log.warning("встроенные сертификаты не загружены: %s", e)
    if os.path.isdir(CERTS_DIR):  # дополнительные, если положат
        for f in os.listdir(CERTS_DIR):
            if f.endswith(".pem"):
                try:
                    ctx.load_verify_locations(os.path.join(CERTS_DIR, f))
                except ssl.SSLError as e:
                    log.warning("сертификат %s не загружен: %s", f, e)
    return ctx


def q2f(q: Optional[dict]) -> float:
    """Quotation {units, nano} -> float."""
    if not q:
        return 0.0
    return int(q.get("units") or 0) + int(q.get("nano") or 0) / 1e9


@dataclass
class Instrument:
    ticker: str
    uid: str
    figi: str
    lot: int
    name: str


@dataclass
class Trade:
    ts: datetime      # UTC
    price: float
    qty: int          # в лотах
    side: str         # "B" / "S"


@dataclass
class Level:
    price: float
    qty: int


@dataclass
class OrderBook:
    ts: datetime
    bids: list[Level]  # от лучшей (высшей) цены вниз
    asks: list[Level]  # от лучшей (низшей) цены вверх
    last: float


@dataclass
class TCandle:
    begin: datetime   # MSK, naive (как у MOEX-свечей)
    open: float
    high: float
    low: float
    close: float
    volume: float     # в лотах
    complete: bool


INTERVALS = {1: "CANDLE_INTERVAL_1_MIN", 5: "CANDLE_INTERVAL_5_MIN",
             15: "CANDLE_INTERVAL_15_MIN", 30: "CANDLE_INTERVAL_30_MIN",
             60: "CANDLE_INTERVAL_HOUR", 240: "CANDLE_INTERVAL_4_HOUR",
             1440: "CANDLE_INTERVAL_DAY"}
# максимальный период одного запроса по документации T-Invest
_MAX_SPAN = {1: timedelta(days=1), 5: timedelta(days=7), 15: timedelta(days=21),
             30: timedelta(days=21), 60: timedelta(days=90), 240: timedelta(days=90),
             1440: timedelta(days=365 * 6)}
_MSK = timezone(timedelta(hours=3))


class TinkoffClient:
    def __init__(self) -> None:
        self._sess: Optional[aiohttp.ClientSession] = None
        self._instr: dict[str, Optional[Instrument]] = {}

    async def _session(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {TOKEN}",
                         "Content-Type": "application/json",
                         "x-app-name": "moex-stockbot"},
                connector=aiohttp.TCPConnector(ssl=ssl_context()),
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._sess

    async def close(self) -> None:
        if self._sess and not self._sess.closed:
            await self._sess.close()

    async def _call(self, method: str, body: dict[str, Any]) -> dict[str, Any]:
        sess = await self._session()
        last: Optional[Exception] = None
        for attempt in range(3):
            try:
                async with sess.post(BASE + method, json=body) as r:
                    if r.status == 429:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    data = await r.json(content_type=None)
                    if r.status >= 400:
                        msg = data.get("message") if isinstance(data, dict) else str(data)
                        if r.status == 401:
                            msg = "токен не принят (401) — проверь TINKOFF_TOKEN"
                        raise RuntimeError(f"T-Invest {method.split('/')[-1]}: {msg}")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = e
                await asyncio.sleep(1.0 * (attempt + 1))
        raise last or RuntimeError("T-Invest: rate limit")

    # ------------------------------------------------------------ методы
    async def instrument(self, ticker: str) -> Optional[Instrument]:
        t = ticker.upper()
        if t in self._instr:
            return self._instr[t]
        inst: Optional[Instrument] = None
        try:
            d = await self._call("InstrumentsService/ShareBy", {
                "idType": "INSTRUMENT_ID_TYPE_TICKER", "classCode": "TQBR", "id": t})
            i = d.get("instrument") or {}
            if i:
                inst = Instrument(t, i["uid"], i.get("figi", ""),
                                  int(i.get("lot") or 1), i.get("name", t))
        except Exception as e:
            log.warning("T-Invest: инструмент %s: %s", t, e)
            raise  # пусть пользователь увидит настоящую причину (401, сеть...)
        self._instr[t] = inst
        return inst

    async def order_book(self, inst: Instrument, depth: int = 20) -> Optional[OrderBook]:
        d = await self._call("MarketDataService/GetOrderBook",
                             {"instrumentId": inst.uid, "depth": depth})
        bids = [Level(q2f(x["price"]), int(x["quantity"])) for x in d.get("bids", [])]
        asks = [Level(q2f(x["price"]), int(x["quantity"])) for x in d.get("asks", [])]
        if not bids and not asks:
            return None
        return OrderBook(datetime.now(timezone.utc), bids, asks, q2f(d.get("lastPrice")))

    async def candles(self, inst: Instrument, minutes: int,
                      depth: timedelta) -> list[TCandle]:
        """Свечи в реальном времени (последняя — незавершённая)."""
        interval = INTERVALS[minutes]
        now = datetime.now(timezone.utc)
        span = _MAX_SPAN[minutes]
        # бьём на окна, если глубина больше допустимого периода запроса
        windows = []
        start = now - depth
        while start < now:
            end = min(start + span, now)
            windows.append((start, end))
            start = end
        results = await asyncio.gather(*(self._call("MarketDataService/GetCandles", {
            "instrumentId": inst.uid, "interval": interval,
            "from": a.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": b.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "candleSourceType": "CANDLE_SOURCE_UNSPECIFIED", "limit": 2400,
        }) for a, b in windows), return_exceptions=True)
        seen: dict[str, TCandle] = {}
        for r in results:
            if isinstance(r, BaseException):
                log.warning("T-Invest: свечи %s: %s", inst.ticker, r)
                continue
            for c in r.get("candles", []):
                ts = datetime.fromisoformat(c["time"].replace("Z", "+00:00"))
                begin = ts.astimezone(_MSK).replace(tzinfo=None)
                seen[c["time"]] = TCandle(
                    begin, q2f(c["open"]), q2f(c["high"]), q2f(c["low"]),
                    q2f(c["close"]), float(c.get("volume") or 0),
                    bool(c.get("isComplete", True)))
        return [seen[k] for k in sorted(seen)]

    async def last_prices(self, insts: list[Instrument]) -> dict[str, tuple[float, datetime]]:
        """Последние цены: тикер -> (цена, время UTC)."""
        if not insts:
            return {}
        d = await self._call("MarketDataService/GetLastPrices",
                             {"instrumentId": [i.uid for i in insts]})
        by_uid = {i.uid: i.ticker for i in insts}
        out = {}
        for p in d.get("lastPrices", []):
            t = by_uid.get(p.get("instrumentUid"))
            if t and p.get("price"):
                ts = datetime.fromisoformat(p["time"].replace("Z", "+00:00"))
                out[t] = (q2f(p["price"]), ts)
        return out

    async def last_trades(self, inst: Instrument, minutes: int = 10) -> list[Trade]:
        now = datetime.now(timezone.utc)
        frm = now - timedelta(minutes=min(minutes, 60))
        d = await self._call("MarketDataService/GetLastTrades", {
            "instrumentId": inst.uid,
            "from": frm.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ")})
        out = []
        for x in d.get("trades", []):
            ts = datetime.fromisoformat(x["time"].replace("Z", "+00:00"))
            side = "B" if x.get("direction") == "TRADE_DIRECTION_BUY" else "S"
            out.append(Trade(ts, q2f(x["price"]), int(x.get("quantity") or 0), side))
        out.sort(key=lambda t: t.ts)
        return out
