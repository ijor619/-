"""Быстрый парсер новостей → Telegram-канал.

Источники опрашиваются параллельно каждые NEWS_INTERVAL_SEC; каждая новость
матчится с бумагами из watchlist'ов пользователей (тикер, название, синонимы)
и с рыночными триггерами (ЦБ, ставка, санкции, MOEX-приостановки…).
Под новостью — кнопки тикера: график / стакан / лента / Т-Инвестиции.
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from xml.etree import ElementTree as ET

import aiohttp

from formatting import esc

log = logging.getLogger(__name__)

NEWS_INTERVAL_SEC = int(os.getenv("NEWS_INTERVAL_SEC", "30"))
NEWS_CHANNEL = os.getenv("NEWS_CHANNEL", "").strip()   # @username или -100…
MAX_AGE_MIN = 90            # старше — не постим (после рестарта не заливать стену)
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
MSK = timezone(timedelta(hours=3))

# ------------------------------------------------------------ источники
# (id, название, url, тип)  тип: rss | moex
SOURCES: list[tuple[str, str, str, str]] = [
    ("interfax", "Интерфакс", "https://www.interfax.ru/rss.asp", "rss"),
    ("rbc", "РБК", "https://rssexport.rbc.ru/rbcnews/news/30/full.rss", "rss"),
    ("tass", "ТАСС", "https://tass.ru/rss/v2.xml", "rss"),
    ("kommersant", "Коммерсантъ", "https://www.kommersant.ru/RSS/news.xml", "rss"),
    ("finam", "Финам", "https://www.finam.ru/analysis/conews/rsspoint/", "rss"),
    ("smartlab", "Смарт-Лаб", "https://smart-lab.ru/news/rss/", "rss"),
    ("edisclosure", "Раскрытие", "https://www.e-disclosure.ru/rss/rss.aspx", "rss"),
    ("moex", "MOEX", "https://iss.moex.com/iss/sitenews.json?iss.meta=off", "moex"),
    ("cbr", "Банк России", "https://www.cbr.ru/rss/RssPress", "rss"),
    ("cbr_ev", "Банк России", "https://www.cbr.ru/rss/eventrss", "rss"),
]

# ------------------------------------------------------------ словари
# тикер -> синонимы (нижний регистр, основы слов)
COMPANY_ALIASES: dict[str, list[str]] = {
    "SBER": ["сбер", "сбербанк", "греф"],
    "GAZP": ["газпром", "миллер"],
    "LKOH": ["лукойл"],
    "ROSN": ["роснефт", "сечин"],
    "NVTK": ["новатэк", "михельсон"],
    "GMKN": ["норникел", "нornickel", "потанин"],
    "YDEX": ["яндекс", "yandex"],
    "PLZL": ["полюс", "polyus"],
    "RUAL": ["русал", "rusal", "дерипаск"],
    "VTBR": ["втб", "костин"],
    "TATN": ["татнефт"],
    "SNGS": ["сургутнефтегаз"],
    "MGNT": ["ритейлер магнит", "сеть магнит", "«магнит»", "магнит\" ", "магнита "],
    "MTSS": ["мтс "],
    "ALRS": ["алроса"],
    "CHMF": ["северстал", "мордашов"],
    "NLMK": ["нлмк", "лисин"],
    "MAGN": ["ммк", "магнитогорск"],
    "PHOR": ["фосагро"],
    "AFLT": ["аэрофлот"],
    "MOEX": ["мосбирж", "московская биржа", "московской бирж", "московскую бирж"],
    "OZON": ["ozon", "озон"],
    "T": ["т-банк", "тбанк", "т-технолог", "тинькофф", "т-инвестиц"],
    "POSI": ["positive technologies", "позитив текнолоджис", "группа позитив"],
    "AFKS": ["афк систем", "евтушенков"],
    "PIKK": ["гк пик", "группа пик", "«пик»", "пик\" ", "застройщик пик"],
    "SMLT": ["гк самолет", "группа самолет", "девелопер\u00a0самолет", "самолет\" ", "«самолет»", "застройщик самолет"],
    "IRAO": ["интер рао"],
    "HYDR": ["русгидро"],
    "FEES": ["россети "],
    "TRNFP": ["транснефт"],
    "BSPB": ["банк санкт-петербург"],
    "SELG": ["селигдар"],
    "UGLD": ["южуралзолото", "юггк"],
    "MTLR": ["мечел"],
    "RASP": ["распадск"],
    "SGZH": ["сегеж"],
    "VKCO": ["vk ", "вконтакте", "холдинг vk"],
    "HEAD": ["headhunter", "хедхантер", "hh.ru"],
    "ASTR": ["группа астра", "гк астра", "«астра»", "astra linux"],
    "SOFL": ["софтлайн"],
    "LEAS": ["европлан"],
    "RENI": ["ренессанс страхован"],
    "SVCB": ["совкомбанк"],
    "MBNK": ["мтс банк", "мтс-банк"],
    "X5": ["x5", "пятёрочк", "пятерочк", "перекресток"],
    "FLOT": ["совкомфлот"],
    "NMTP": ["нмтп", "новороссийский морской"],
    "ENPG": ["эн+", "en+"],
    "UPRO": ["юнипро"],
    "MSNG": ["мосэнерго"],
    "LSRG": ["лср ", "группа лср"],
    "ETLN": ["группа эталон", "гк эталон", "«эталон»"],
    "CIAN": ["циан"],
    "WUSH": ["whoosh", "вуш холдинг", "«вуш»"],
    "DELI": ["делимобил"],
    "ELMT": ["гк элемент", "группа элемент", "«элемент»"],
    "DATA": ["аренадата", "arenadata"],
    "IVAT": ["iva technologies"],
    "DIAS": ["диасофт"],
    "SIBN": ["газпром нефт", "газпромнефт"],
    "BANE": ["башнефт"],
    "RNFT": ["русснефт"],
    "AKRN": ["акрон"],
    "KAZT": ["куйбышевазот"],
    "NKNC": ["нижнекамскнефтехим", "нкнх"],
    "MRKC": ["россети центр"],
    "LENT": ["ритейлер лента", "сеть лента", "«лента»", "лента\" ", "гипермаркет"],
    "FIXP": ["fix price", "фикс прайс"],
    "MVID": ["м.видео", "мвидео"],
    "OKEY": ["о'кей", "«окей»"],
    "BELU": ["novabev", "белуга"],
    "ABRD": ["абрау"],
    "AQUA": ["инарктик"],
    "GCHE": ["черкизов"],
    "RTKM": ["ростелеком"],
    "MTLRP": [],
    "KMAZ": ["камаз"],
    "SVAV": ["соллерс"],
    "UWGN": ["объединенная вагонная", "объединённая вагонная", "вагоностроительн"],
    "TGKA": ["тгк-1"],
    "OGKB": ["огк-2"],
    "ELFV": ["эл5"],
    "MSRS": ["россети московск"],
    "DVEC": ["дэк", "дальневосточная энергет"],
    "APTK": ["аптечная сеть", "аптеки 36"],
    "PRMD": ["промомед"],
    "GECO": ["генетико"],
    "MDMG": ["мать и дитя", "md medical"],
    "OZPH": ["озон фармацевтик"],
    "LIFE": ["фармсинтез"],
    "VSMO": ["всмпо", "ависма"],
    "AMEZ": ["ашинск"],
    "TRMK": ["тмк", "трубная металлург"],
    "CHMK": ["чмк"],
    "LNZL": ["лензолото"],
    "POLY": ["polymetal", "полиметалл", "solidcore"],
    "GLTR": ["globaltrans", "глобалтранс"],
    "FESH": ["fesco", "феско", "дальневосточное морское"],
    "NKHP": ["нкхп"],
    "MSTT": ["мостотрест"],
    "SFIN": ["эсэфай", "холдинг сфи"],
    "MGKL": ["мосгорломбард"],
    "CARM": ["carmoney", "кармани"],
    "ZAYM": ["займер"],
    "CBOM": ["мкб ", "московский кредитный банк"],
    "SBERP": [],
    "TATNP": [],
    "SNGSP": [],
    "RTKMP": [],
    "BANEP": [],
    "KROT": ["фабрика красный октябрь"],
    "VEON": ["veon", "вымпелком", "билайн"],
    "QIWI": ["qiwi", "киви"],
    "CNRU": ["cian"],
    "GEMC": ["юмг", "европейский медицинский центр"],
    "ROLO": ["русолово"],
    "UNAC": ["оак ", "объединенная авиастроит", "объединённая авиастроит"],
    "IRKT": ["пао яковлев", "корпорация иркут", "мс-21"],
    "KLSB": ["калужская сбытов"],
    "NSVZ": ["наука-связь"],
    "NAUK": ["нпо наука"],
    "SPBE": ["спб биржа", "спб-биржа"],
}

# рыночные триггеры: (regex, метка, приоритет)
MARKET_TRIGGERS: list[tuple[re.Pattern, str, int]] = [
    # решение по ставке / сигнал регулятора — всегда важно
    (re.compile(r"(ключев\w*|учетн\w*) ставк\w*.{0,40}(сохран|повы[сш]|сниз|оставил|поднял|опустил|на уровне|до \d{1,2}(,\d+)?\s?%)|"
                r"(сохран|повы[сш]|сниз|оставил|поднял|опустил)\w*.{0,40}(ключев\w*|учетн\w*) ставк|"
                r"банк россии принял решение (сохранить|повысить|снизить)|решени\w* (по|о) ключевой ставк|"
                r"(заявлени|пресс-конференци)\w* (председателя )?(банка россии|набиуллин)\w*.{0,30}(по итогам|заседани)|"
                r"сигнал\w* (цб|банка россии) (по|о) ставк|среднесрочн\w* прогноз\w* (цб|банка россии)|"
                r"резюме обсуждения ключевой ставки|"
                r"снижени\w* (ключевой )?ставки .{0,20}(не )?обсуждал", re.I), "ЦБ ставка", 3),
    # анонс заседания / пресс-конференции по ставке — предупреждение заранее
    (re.compile(r"заседани\w* совета директоров( банка россии)? по (ключевой )?ставк|"
                r"состоится пресс-конференция по итогам заседания|"
                r"(накануне|перед|в ожидании) (заседани\w* цб|решени\w* (цб|банка россии) по ставк)", re.I), "ЦБ анонс", 2),
    (re.compile(r"ключев\w* ставк|набиуллин|заботкин|банк россии|\bцб\b", re.I), "ЦБ", 1),
    (re.compile(r"инфляци\w* в (россии|рф).{0,40}(за неделю|недел|ускорил|замедлил|составил)|"
                r"недельн\w* инфляци|инфляционн\w* ожидани", re.I), "инфляция", 2),
    (re.compile(r"(санкци\w*|sdn|ofac|блокирующ\w*).{0,60}(против (росси|рф\b|российск|москвы|сбер|втб|газпром|роснефт|лукойл|новатэк|мосбирж|нкц|нрд|альфа|т-банк|совкомбанк|российских (банк|компан))|"
                r"(в отношении|с) (росси|рф\b|российск)|нкц|нрд|spfs|swift|(нефтян|газов|энергетическ)\w* (сектор|компан)\w* (росси|рф))|"
                r"(евросоюз|ес|минфин сша|сша|великобритани|велико британи|g7|япони)\w*.{0,40}(ввел|ввод|расшир|утверд|принял|согласовал)\w*.{0,40}(пакет )?санкци\w*.{0,40}(росси|рф\b|российск)|"
                r"\d{1,2}-?й пакет (антироссийск\w* )?санкци|снят\w* санкци\w* с росси|отмен\w* санкци\w*.{0,20}росси", re.I),
     "санкции", 2),
    (re.compile(r"дискретн\w* аукцион|приостан\w* торг|возобнов\w* торг|дестабилизаци\w* цен", re.I), "MOEX торги", 3),
    (re.compile(r"brent|urals|опек|цен\w* на нефть|нефт\w* (подорож|подешев|обвал|рухн|взлет)|потолок цен", re.I), "нефть", 1),
    (re.compile(r"курс (рубл|доллар|юан)|рубл\w* (укреп|ослаб|обвал)", re.I), "рубль", 1),
    (re.compile(r"перемир|переговор\w* (по|об) украин|мирн\w* (план|соглашен)", re.I), "геополитика", 2),
    (re.compile(r"налог\w* на (прибыль|сверхприбыль)|windfall|ндпи", re.I), "налоги", 2),
    (re.compile(r"минфин.*(размещ|офз)|аукцион\w* офз", re.I), "ОФЗ", 1),
]
# триггеры по бумаге: усиливают приоритет
STOCK_TRIGGERS: list[tuple[re.Pattern, str, int]] = [
    (re.compile(r"дивиденд", re.I), "дивиденды", 3),
    (re.compile(r"совет директоров|наблюдательн\w* совет|\bсд\b", re.I), "СД", 2),
    (re.compile(r"отч[её]т|мсфо|рсбу|чист\w* прибыл|выручк|ebitda", re.I), "отчётность", 2),
    (re.compile(r"buyback|обратн\w* выкуп|байбэк", re.I), "buyback", 3),
    (re.compile(r"допэмисси|spo|ipo|размещени", re.I), "размещение", 2),
    (re.compile(r"сделк\w* (по )?(покупк|продаж|поглощ)|m&a|приобрет", re.I), "M&A", 2),
    (re.compile(r"рейтинг|прогноз|таргет|целев\w* цен|рекомендац", re.I), "аналитика", 1),
    (re.compile(r"(?<![а-яё])суд(?![а-яё])|(?<![а-яё])суд[аеу](?![а-яё])|арбитраж|(?<![а-яё])иск(?![а-яё])|(?<![а-яё])иск[аиу](?![а-яё])|штраф|(?<![а-яё])фас(?![а-яё])|арест|обыск|уголовн|банкрот", re.I), "суд/регулятор", 2),
    (re.compile(r"гендиректор|отставк|назнач|уволен|сменил", re.I), "менеджмент", 1),
    (re.compile(r"авари|пожар|взрыв|атак\w* (бпла|дрон)|остановк\w* (производ|завод|нпз)", re.I), "ЧП", 3),
]
PRIORITY_EMOJI = {3: "⚡️", 2: "❗️", 1: "ℹ️", 0: "📰"}
_STOP = {"россии", "россия", "банка", "банк", "заявил", "заявила", "сообщил", "сообщила",
         "рассказал", "рассказала", "назвал", "назвала", "стало", "может", "будет", "после",
         "того", "этом", "также", "году", "года", "компания", "компании"}


@dataclass
class NewsItem:
    uid: str
    source_id: str
    source: str
    title: str
    link: str
    published: datetime          # aware UTC
    summary: str = ""
    tickers: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    priority: int = 0


# --------------------------------------------------------------- парсинг

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def _parse_rss(source_id: str, source: str, xml: str) -> list[NewsItem]:
    out: list[NewsItem] = []
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as e:
        log.warning("news: %s: битый XML: %s", source_id, e)
        return out
    for it in root.iter("item"):
        title = _norm(it.findtext("title") or "")
        link = (it.findtext("link") or "").strip()
        if not title:
            continue
        pub = it.findtext("pubDate") or it.findtext("{http://purl.org/dc/elements/1.1/}date") or ""
        try:
            dt = parsedate_to_datetime(pub)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=MSK)
        except Exception:
            dt = datetime.now(timezone.utc)
        desc = _norm(it.findtext("description") or "")[:400]
        uid = hashlib.md5((link or title).encode()).hexdigest()[:16]
        out.append(NewsItem(uid, source_id, source, title, link,
                            dt.astimezone(timezone.utc), desc))
    return out


def _parse_moex(source_id: str, source: str, raw: str) -> list[NewsItem]:
    out: list[NewsItem] = []
    try:
        rows = json.loads(raw)["sitenews"]["data"]
    except Exception as e:
        log.warning("news: moex: %s", e)
        return out
    for nid, tag, title, published, _ in rows:
        dt = datetime.strptime(published, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
        out.append(NewsItem(f"moex{nid}", source_id, source, _norm(title),
                            f"https://www.moex.com/n{nid}", dt.astimezone(timezone.utc)))
    return out


# ----------------------------------------------------------- классификация

_W = "а-яёa-z0-9"


def _alias_match(alias: str, text: str) -> bool:
    """Синоним совпадает как начало слова: «сбер» → «сбербанка», но не «овк» → «новка».
    Синоним, заканчивающийся пробелом, требует полного слова."""
    a = alias.strip().lower()
    if not a:
        return False
    if alias.endswith(" "):
        return re.search(rf"(?<![{_W}]){re.escape(a)}(?![{_W}])", text) is not None
    return re.search(rf"(?<![{_W}]){re.escape(a)}", text) is not None


def classify(item: NewsItem, watch: dict[str, list[str]]) -> None:
    """Проставить tickers / tags / priority. watch: тикер -> синонимы."""
    text = f"{item.title} {item.summary}".lower()
    tickers = []
    for t, aliases in watch.items():
        if len(t) >= 4 and re.search(rf"(?<![a-z0-9]){re.escape(t.lower())}(?![a-z0-9])", text):
            tickers.append(t)
            continue
        for a in aliases:
            if _alias_match(a, text):
                tickers.append(t)
                break
    item.tickers = tickers
    tags, prio = [], 0
    if tickers:
        prio = 1
        for rx, tag, p in STOCK_TRIGGERS:
            if rx.search(text):
                tags.append(tag)
                prio = max(prio, p)
    for rx, tag, p in MARKET_TRIGGERS:
        if rx.search(text):
            tags.append(tag)
            prio = max(prio, p if not tickers else max(p, 1))
    # MOEX-новости про конкретную бумагу: "для ценной бумаги XXXX"
    if item.source_id == "moex":
        m = re.search(r"ценн\w* бумаг\w* ([A-Z0-9]{3,6})", item.title)
        if m and m.group(1) in watch:
            item.tickers = [m.group(1)]
            tags.append("MOEX торги")
            prio = 3
    item.tags = list(dict.fromkeys(tags))
    item.priority = prio


def is_relevant(item: NewsItem, mode: str) -> bool:
    """mode: 'watch' — только мои бумаги; 'stocks' — все бумаги РФ из словаря
    + рыночные триггеры; 'market' — мои бумаги + рыночные; 'all' — всё."""
    if mode == "all":
        return True
    if item.tickers:
        return True
    if mode == "watch":
        return False
    # рыночные события уровня ⚡️/❗️ (ставка, санкции, MOEX-торги, геополитика…)
    # важны для всех бумаг — пропускаем в любом режиме
    return item.priority >= 2


def format_item(item: NewsItem) -> str:
    t = item.published.astimezone(MSK).strftime("%H:%M")
    head = PRIORITY_EMOJI.get(item.priority, "📰")
    tk = " ".join(f"#{x}" for x in item.tickers)
    tags = " · ".join(item.tags)
    lines = [f"{head} <b>{esc(item.title)}</b>"]
    if item.summary and item.summary.lower() != item.title.lower():
        lines.append(esc(item.summary[:300]) + ("…" if len(item.summary) > 300 else ""))
    meta = f"<i>{esc(item.source)} · {t} МСК</i>"
    if tags:
        meta += f" · {esc(tags)}"
    lines.append(meta)
    if tk:
        lines.append(tk)
    if item.link:
        lines.append(f'<a href="{html.escape(item.link, quote=True)}">Источник</a>')
    return "\n".join(lines)


# ---------------------------------------------------------------- монитор

class NewsMonitor:
    def __init__(self, bot, store, seen_path: str, mode: str = "stocks") -> None:
        self.bot, self.store, self.mode = bot, store, mode
        self.seen_path = seen_path
        self.seen: dict[str, float] = {}
        self._load()
        self._first = True
        self.channel = NEWS_CHANNEL

    def _load(self) -> None:
        try:
            with open(self.seen_path, encoding="utf-8") as f:
                self.seen = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.seen = {}

    def _save(self) -> None:
        cutoff = time.time() - 3 * 86400
        self.seen = {k: v for k, v in self.seen.items() if v >= cutoff}
        d = os.path.dirname(self.seen_path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(self.seen_path, "w", encoding="utf-8") as f:
            json.dump(self.seen, f)

    def watch_dict(self) -> dict[str, list[str]]:
        tickers = {t for _, p in self.store.all() for t in p.watchlist}
        if self.mode == "stocks":
            # все российские эмитенты из словаря + то, что в watchlist'ах
            d = dict(COMPANY_ALIASES)
            for t in tickers:
                d.setdefault(t, [])
            return d
        return {t: COMPANY_ALIASES.get(t, []) for t in tickers}

    def _is_dup_story(self, it: "NewsItem") -> bool:
        """Тот же сюжет от другого агентства в течение 30 мин: те же тикеры/теги
        и ≥60% общих значимых слов заголовка — не постим повторно."""
        words = {w for w in re.findall(r"[а-яёa-z0-9]{4,}", it.title.lower())
                 if w not in _STOP}
        if not words:
            return False
        now = time.time()
        self._stories = [(ts, k, ws) for ts, k, ws in getattr(self, "_stories", [])
                         if now - ts < 1800]
        key = (tuple(it.tickers), tuple(t for t in it.tags if t not in ("ЦБ",)))
        for ts, k, ws in self._stories:
            if k == key and len(words & ws) / max(1, min(len(words), len(ws))) >= 0.6:
                return True
        self._stories.append((now, key, words))
        return False

    async def fetch_source(self, sess: aiohttp.ClientSession,
                           sid: str, name: str, url: str, kind: str) -> list[NewsItem]:
        try:
            async with sess.get(url, headers={"User-Agent": UA},
                                timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    log.debug("news: %s HTTP %s", sid, r.status)
                    return []
                raw = await r.text(errors="ignore")
        except Exception as e:
            log.debug("news: %s: %s", sid, e)
            return []
        if kind == "moex":
            return _parse_moex(sid, name, raw)
        if "<rss" not in raw[:2000] and "<feed" not in raw[:2000]:
            return []
        return _parse_rss(sid, name, raw)

    async def fetch_all(self, sess: aiohttp.ClientSession) -> list[NewsItem]:
        res = await asyncio.gather(*(self.fetch_source(sess, *s) for s in SOURCES),
                                   return_exceptions=True)
        items: list[NewsItem] = []
        for r in res:
            if isinstance(r, list):
                items.extend(r)
        items.sort(key=lambda i: i.published)
        return items

    async def run(self, sess: aiohttp.ClientSession) -> None:
        if not self.channel:
            log.info("news: NEWS_CHANNEL не задан — парсер новостей выключен")
            return
        log.info("news: парсер запущен → %s, опрос каждые %s c, режим %s",
                 self.channel, NEWS_INTERVAL_SEC, self.mode)
        while True:
            t0 = time.monotonic()
            try:
                await self.tick(sess)
            except Exception:
                log.exception("news: ошибка тика")
            await asyncio.sleep(max(10.0, NEWS_INTERVAL_SEC - (time.monotonic() - t0)))

    async def tick(self, sess: aiohttp.ClientSession) -> None:
        from keyboards import news_kb  # локальный импорт — избегаем цикла
        watch = self.watch_dict()
        items = await self.fetch_all(sess)
        now = datetime.now(timezone.utc)
        posted = 0
        for it in items:
            if it.uid in self.seen:
                continue
            self.seen[it.uid] = time.time()
            if self._first or (now - it.published) > timedelta(minutes=MAX_AGE_MIN):
                continue  # первый тик — только запоминаем, не заливаем канал
            classify(it, watch)
            if not is_relevant(it, self.mode):
                continue
            if self._is_dup_story(it):
                continue
            try:
                await self.bot.send_message(self.channel, format_item(it),
                                            reply_markup=news_kb(it.tickers),
                                            disable_web_page_preview=True)
                posted += 1
            except Exception as e:
                log.warning("news: не отправлено в %s: %s", self.channel, e)
            if posted >= 8:
                break
        self._first = False
        self._save()
