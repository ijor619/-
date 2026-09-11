"""CScalp bridge — мостик «кнопка в Telegram → инструмент в CScalp».

Запускается на Windows рядом с CScalp (обычный ПК или Windows в Parallels).
Раз в секунду спрашивает у бота, нет ли новой команды, и, получив тикер,
переключает стакан CScalp на него:
  1) ищет окно CScalp;
  2) кликает по заголовку стакана (там, где написан текущий тикер) —
     через UI Automation по тексту, если не выходит — по откалиброванной точке;
  3) вводит тикер в появившийся поиск и жмёт Enter.

Установка (один раз, в PowerShell):
    pip install requests pywinauto pyautogui pyperclip
    python cscalp_bridge.py --calibrate      # навести мышь на заголовок стакана, Enter
    python cscalp_bridge.py                  # рабочий режим

Настройки — в файле cscalp_bridge.ini рядом со скриптом (создаётся при первом
запуске): адрес бота и ключ. Ключ тот же, что в переменной CSCALP_KEY у бота.

Автозапуск: положить ярлык на `pythonw.exe cscalp_bridge.py` в
shell:startup. Лог — cscalp_bridge.log рядом со скриптом.
"""
from __future__ import annotations

import configparser
import json
import logging
import os
import sys
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
INI = os.path.join(HERE, "cscalp_bridge.ini")
LOG = os.path.join(HERE, "cscalp_bridge.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(LOG, encoding="utf-8"),
                              logging.StreamHandler(sys.stdout)])
log = logging.getLogger("bridge")

DEFAULT_INI = """[bot]
; адрес бота на Bothost (публичный URL приложения) и ключ CSCALP_KEY
url = https://YOUR-APP.bothost.ru
key = CHANGE_ME
poll_sec = 1.0

[cscalp]
; часть заголовка главного окна CScalp (регистр не важен)
window_title = cscalp
; откалиброванная точка заголовка стакана (заполняется --calibrate)
click_x = 0
click_y = 0
; задержки, сек
after_click = 0.35
after_type = 0.15
"""


def load_cfg() -> configparser.ConfigParser:
    if not os.path.exists(INI):
        with open(INI, "w", encoding="utf-8") as f:
            f.write(DEFAULT_INI)
        log.info("создан %s — заполните url и key", INI)
    cfg = configparser.ConfigParser()
    cfg.read(INI, encoding="utf-8")
    return cfg


def save_cfg(cfg: configparser.ConfigParser) -> None:
    with open(INI, "w", encoding="utf-8") as f:
        cfg.write(f)


# ------------------------------------------------------------- CScalp UI

def find_window(title_part: str):
    """Главное окно CScalp через pywinauto (UIA)."""
    from pywinauto import Desktop
    tp = title_part.lower()
    for w in Desktop(backend="uia").windows():
        try:
            if tp in (w.window_text() or "").lower():
                return w
        except Exception:
            continue
    return None


def click_header_by_text(win, current_ticker_hint: str | None) -> bool:
    """Пробуем найти в UIA-дереве текстовый элемент с тикером (заголовок стакана).
    CScalp рисует часть интерфейса сам, поэтому это может не сработать."""
    try:
        import re
        cands = []
        for el in win.descendants():
            try:
                txt = (el.window_text() or "").strip()
            except Exception:
                continue
            if not txt or len(txt) > 12:
                continue
            if re.fullmatch(r"[A-Z0-9]{3,6}", txt):
                cands.append(el)
        if current_ticker_hint:
            for el in cands:
                if el.window_text().strip() == current_ticker_hint:
                    el.click_input()
                    return True
        if cands:
            cands[0].click_input()
            return True
    except Exception as e:
        log.debug("UIA поиск заголовка: %s", e)
    return False


def switch_instrument(cfg: configparser.ConfigParser, ticker: str, state: dict) -> str:
    import pyautogui
    import pyperclip
    pyautogui.FAILSAFE = False
    title = cfg.get("cscalp", "window_title", fallback="cscalp")
    win = find_window(title)
    if win is None:
        return "окно CScalp не найдено"
    try:
        if win.is_minimized():
            win.restore()
        win.set_focus()
    except Exception as e:
        log.debug("фокус: %s", e)
    time.sleep(0.15)

    clicked = click_header_by_text(win, state.get("current"))
    if not clicked:
        x = cfg.getint("cscalp", "click_x", fallback=0)
        y = cfg.getint("cscalp", "click_y", fallback=0)
        if not x and not y:
            return "заголовок стакана не найден и точка не откалибрована: запустите с --calibrate"
        pyautogui.click(x, y)
    time.sleep(cfg.getfloat("cscalp", "after_click", fallback=0.35))

    # поле поиска получило фокус: очистить, вставить тикер, Enter.
    # Вставка через буфер обмена не зависит от раскладки клавиатуры.
    pyautogui.hotkey("ctrl", "a")
    old_clip = None
    try:
        old_clip = pyperclip.paste()
    except Exception:
        pass
    pyperclip.copy(ticker)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(cfg.getfloat("cscalp", "after_type", fallback=0.15))
    pyautogui.press("enter")
    if old_clip is not None:
        try:
            pyperclip.copy(old_clip)
        except Exception:
            pass
    state["current"] = ticker
    return "ok"


def calibrate(cfg: configparser.ConfigParser) -> None:
    import pyautogui
    print("Наведите мышь на заголовок стакана CScalp (где написан тикер) и нажмите Enter здесь…")
    input()
    x, y = pyautogui.position()
    cfg.set("cscalp", "click_x", str(x))
    cfg.set("cscalp", "click_y", str(y))
    save_cfg(cfg)
    print(f"Сохранено: ({x}, {y}). Проверка: python cscalp_bridge.py --test SBER")


# --------------------------------------------------------------- транспорт

def poll_loop(cfg: configparser.ConfigParser) -> None:
    url = cfg.get("bot", "url").rstrip("/")
    key = cfg.get("bot", "key")
    poll = cfg.getfloat("bot", "poll_sec", fallback=1.0)
    if key == "CHANGE_ME" or "YOUR-APP" in url:
        log.error("заполните url и key в %s", INI)
        return
    state: dict = {}
    sess = requests.Session()
    log.info("мостик запущен → %s", url)
    backoff = poll
    while True:
        try:
            r = sess.get(f"{url}/cscalp/next", params={"key": key}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                backoff = poll
                for cmd in data.get("commands", []):
                    t = str(cmd.get("ticker", "")).upper()
                    if t:
                        res = switch_instrument(cfg, t, state)
                        log.info("→ %s: %s", t, res)
                        try:
                            sess.post(f"{url}/cscalp/ack", params={"key": key},
                                      json={"id": cmd.get("id"), "ticker": t, "result": res}, timeout=10)
                        except Exception:
                            pass
            elif r.status_code == 403:
                log.error("бот отверг ключ (403) — проверьте key")
                time.sleep(30)
            else:
                log.warning("HTTP %s", r.status_code)
                time.sleep(5)
        except Exception as e:
            log.warning("связь: %s", e)
            backoff = min(backoff * 2, 30)
        time.sleep(backoff)


def main() -> None:
    cfg = load_cfg()
    args = sys.argv[1:]
    if args and args[0] == "--calibrate":
        calibrate(cfg)
    elif args and args[0] == "--test":
        t = args[1].upper() if len(args) > 1 else "SBER"
        print(switch_instrument(cfg, t, {}))
    else:
        poll_loop(cfg)


if __name__ == "__main__":
    main()
