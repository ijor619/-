"""Форматирование чисел для сообщений (русская локализованная запись)."""
import html


def fmt_price(x: float, decimals: int = 2) -> str:
    """285.1 -> '285,10'; 5206 -> '5 206,0'."""
    s = f"{x:,.{decimals}f}"
    return s.replace(",", "\u00a0").replace(".", ",")


def fmt_pct(x: float) -> str:
    """2.685 -> '+2,69%'; -1.39 -> '-1,39%'."""
    return f"{x:+.2f}%".replace(".", ",")


def cur_symbol(code: str) -> str:
    return {"SUR": "₽", "USD": "$", "EUR": "€"}.get(code, f"{code} ")


def esc(s: str) -> str:
    """Экранирование для HTML-режима Telegram."""
    return html.escape(str(s), quote=False)
