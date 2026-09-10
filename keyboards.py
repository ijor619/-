"""Инлайн-клавиатуры бота."""
from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from charts import DEFAULT_PERIOD, PERIOD_ORDER, PERIOD_TITLES


def tinvest_url(ticker: str) -> str:
    """Страница бумаги в Т-Инвестициях; на телефоне с приложением открывается в нём."""
    return f"https://www.tbank.ru/invest/stocks/{ticker}/"


def tinvest_btn(ticker: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text="🏦 Т-Инвестиции", url=tinvest_url(ticker))


def report_kb(tickers: list[str]) -> InlineKeyboardMarkup:
    """Под сводкой / списком: график по каждой бумаге + обновить/настройки."""
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for t in tickers:
        row.append(InlineKeyboardButton(text=f"📈 {t}", callback_data=f"chart:{t}:{DEFAULT_PERIOD}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh"),
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def alert_kb(ticker: str, flow: bool = False) -> InlineKeyboardMarkup:
    """Под алертом: график, (стакан/лента), быстрое удаление бумаги."""
    row = [InlineKeyboardButton(text="📈 График", callback_data=f"chart:{ticker}:{DEFAULT_PERIOD}")]
    if flow:
        row += [InlineKeyboardButton(text="📚 Стакан", callback_data=f"book:{ticker}"),
                InlineKeyboardButton(text="🧾 Лента", callback_data=f"tape:{ticker}")]
    return InlineKeyboardMarkup(inline_keyboard=[
        row,
        [tinvest_btn(ticker),
         InlineKeyboardButton(text="🔕 Не следить", callback_data=f"unwatch:{ticker}")],
    ])


def flow_kb(ticker: str) -> InlineKeyboardMarkup:
    """Под сигналом по ленте/стакану."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Стакан", callback_data=f"book:{ticker}"),
         InlineKeyboardButton(text="🧾 Лента", callback_data=f"tape:{ticker}"),
         InlineKeyboardButton(text="📈 График", callback_data=f"chart:{ticker}:1m")],
        [tinvest_btn(ticker),
         InlineKeyboardButton(text="🔕 Выкл. сигналы", callback_data="flow:off")],
    ])


def book_kb(ticker: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"book:{ticker}:r"),
         InlineKeyboardButton(text="🧾 Лента", callback_data=f"tape:{ticker}"),
         InlineKeyboardButton(text="📈 График", callback_data=f"chart:{ticker}:1m")],
        [tinvest_btn(ticker)],
    ])


def tape_kb(ticker: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"tape:{ticker}:r"),
         InlineKeyboardButton(text="📚 Стакан", callback_data=f"book:{ticker}"),
         InlineKeyboardButton(text="📈 График", callback_data=f"chart:{ticker}:1m")],
        [tinvest_btn(ticker)],
    ])


def chart_kb(ticker: str, period: str,
             watchlist: list[str] | None = None) -> InlineKeyboardMarkup:
    """Под графиком: переключение периода, бумаги из списка, обновить/закрыть."""
    periods = [
        InlineKeyboardButton(
            text=("● " if p == period else "") + PERIOD_TITLES[p],
            callback_data=f"chart:{ticker}:{p}",
        )
        for p in PERIOD_ORDER
    ]
    rows = [periods[:4], periods[4:]]
    # переключение между бумагами списка прямо с графика
    tick_row: list[InlineKeyboardButton] = []
    for t in (watchlist or []):
        if t == ticker:
            continue
        tick_row.append(InlineKeyboardButton(text=t, callback_data=f"chart:{t}:{period}"))
        if len(tick_row) == 4:
            rows.append(tick_row)
            tick_row = []
    if tick_row:
        rows.append(tick_row)
    rows.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"chart:{ticker}:{period}:r"),
        tinvest_btn(ticker),
        InlineKeyboardButton(text="✖️ Закрыть", callback_data="chart:close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)
