"""Инлайн-клавиатуры бота."""
from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from charts import DEFAULT_PERIOD, PERIOD_ORDER, PERIOD_TITLES


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


def alert_kb(ticker: str) -> InlineKeyboardMarkup:
    """Под алертом: график и быстрое удаление бумаги."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📈 График", callback_data=f"chart:{ticker}:{DEFAULT_PERIOD}"),
        InlineKeyboardButton(text="🔕 Не следить", callback_data=f"unwatch:{ticker}"),
    ]])


def chart_kb(ticker: str, period: str) -> InlineKeyboardMarkup:
    """Под графиком: переключение периода и обновление."""
    periods = [
        InlineKeyboardButton(
            text=("● " if p == period else "") + PERIOD_TITLES[p],
            callback_data=f"chart:{ticker}:{p}",
        )
        for p in PERIOD_ORDER
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        periods[:4],
        periods[4:],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"chart:{ticker}:{period}:r")],
    ])
