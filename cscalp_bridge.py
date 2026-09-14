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
запуске). Достаточно вписать key — тот же, что в переменной CSCALP_KEY у бота:
команды идут через релей ntfy.sh, публичный адрес у бота не нужен.
(Если у бота есть публичный URL и CSCALP_HTTP=1 — впишите url, режим http.)

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
VERSION = "2026-09-12g"

DEFAULT_INI = """[bot]
; ключ — тот же, что CSCALP_KEY у бота (единственное обязательное поле)
key = CHANGE_ME
; relay — через ntfy (по умолчанию, url не нужен); http — бот слушает сам
mode = relay
relay = https://ntfy.sh
; только для mode = http: публичный URL приложения
url = https://YOUR-APP.bothost.ru
poll_sec = 1.0

[cscalp]
; часть заголовка главного окна CScalp (регистр не важен)
window_title = cscalp
; откалиброванная точка заголовка стакана (заполняется --calibrate)
click_x = 0
click_y = 0
; второй клик — по полю поиска в открывшемся окне (если фокус туда не попадает сам);
; заполняется --calibrate2, 0 0 = не кликать
search_x = 0
search_y = 0
; как вводить тикер: type — печатать по буквам (CScalp фильтрует список по нажатиям),
; paste — вставить из буфера
input = type
; как подтверждать выбор в окне «Выбор инструмента»:
;   auto     — найти строку с точно таким символом (SBER, а не SBERP) и дважды кликнуть;
;              если не нашлась — двойной клик по точке result_x/result_y (--calibrate3),
;              иначе Enter
;   dblclick — сразу двойной клик по result_x/result_y
;   enter | down_enter
confirm = dblclick
; точка первой строки результата: абсолютная (result_x/y) и смещение от левого
; верхнего угла окна поиска (result_dx/dy) — смещение в приоритете, так окно
; можно двигать между мониторами
result_x = 0
result_y = 0
result_dx = 0
result_dy = 0
; куда поставить курсор после смены инструмента (--calibrate4); 0 0 = не трогать
park_x = 0
park_y = 0
; заголовок окна поиска (часть, регистр не важен)
search_title = выбор инструмента
; задержки, сек (после клика ждём появления окна поиска, это лишь запас сверху)
after_click = 0.35
; пауза после ввода тикера, пока CScalp фильтрует список (проверено: 1.0)
after_type = 1.0
; сколько максимум ждать появления окна поиска
wait_dialog = 0.6
; пауза между повторными кликами, если список ещё не отфильтровался
retry_wait = 0.3
"""


def load_cfg() -> configparser.ConfigParser:
    if not os.path.exists(INI):
        with open(INI, "w", encoding="utf-8") as f:
            f.write(DEFAULT_INI)
        log.info("создан %s — заполните url и key", INI)
    cfg = configparser.ConfigParser(strict=False)  # дубли ключей — берём последний
    cfg.read(INI, encoding="utf-8-sig")  # -sig: терпим BOM от блокнота/PowerShell
    # дополнить старый ini новыми ключами (со значениями по умолчанию)
    ref = configparser.ConfigParser()
    ref.read_string(DEFAULT_INI)
    changed = False
    for sec in ref.sections():
        if not cfg.has_section(sec):
            cfg.add_section(sec)
        for k, v in ref.items(sec):
            if not cfg.has_option(sec, k):
                cfg.set(sec, k, v)
                changed = True
    if changed:
        save_cfg(cfg)
    return cfg


def save_cfg(cfg: configparser.ConfigParser) -> None:
    with open(INI, "w", encoding="utf-8") as f:
        cfg.write(f)


# ------------------------------------------------------------- CScalp UI

def find_window(title_part: str, exact: bool = False):
    """Главное окно CScalp через pywinauto (UIA). Точное совпадение заголовка
    в приоритете (чтобы не зацепить PyCharm с открытым cscalp_bridge.ini);
    редакторы/браузеры/проводник отсекаются."""
    from pywinauto import Desktop
    tp = title_part.lower()
    bad = ("pycharm", "visual studio", "проводник", "explorer", "chrome", "firefox",
           "edge", "блокнот", "notepad", "powershell", "терминал", ".py", ".ini", ".log")
    partial = None
    for w in Desktop(backend="uia").windows():
        try:
            t = (w.window_text() or "").strip()
            if w.element_info.control_type != "Window":
                continue
        except Exception:
            continue
        tl = t.lower()
        if tl == tp:
            return w
        if not exact and tp in tl and partial is None and not any(b in tl for b in bad):
            partial = w
    return partial


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


def _row_snapshot(cfg: configparser.ConfigParser):
    """Снимок пикселей области первой строки результата (по калибровке)."""
    try:
        import pyautogui
        pt = result_point(cfg)
        if pt is None:
            return None
        rx, ry = pt
        img = pyautogui.screenshot(region=(rx - 160, ry - 10, 320, 20))
        return img.tobytes()
    except Exception as e:
        log.debug("snapshot: %s", e)
        return None


def _wait_row_change(cfg: configparser.ConfigParser, before) -> float:
    """Ждём, пока первая строка списка изменится (фильтр сработал) и перестанет
    меняться. Возвращает, сколько ждали. Потолок — after_type из ini."""
    limit = cfg.getfloat("cscalp", "after_type", fallback=0.8)
    if before is None:
        time.sleep(limit)
        return limit
    t0 = time.time()
    changed = False
    last = before
    stable = 0
    while time.time() - t0 < limit:
        cur = _row_snapshot(cfg)
        if cur is not None and cur != before:
            changed = True
            if cur == last:
                stable += 1
                if stable >= 2:          # два одинаковых кадра подряд — список устоялся
                    break
            else:
                stable = 0
            last = cur
        time.sleep(0.03)
    dt = time.time() - t0
    if not changed:
        log.info("первая строка не изменилась за %.2f с (уже был нужный тикер?)", dt)
    return dt


def _focus_window_at(x: int, y: int) -> None:
    """Дёшево (Win32, без UIA) перевести клавиатурный фокус в окно под точкой:
    иначе буквы могут уйти в терминал/PyCharm."""
    try:
        import ctypes
        from ctypes import wintypes
        u = ctypes.windll.user32
        pt = wintypes.POINT(x, y)
        hwnd = u.WindowFromPoint(pt)
        root = u.GetAncestor(hwnd, 2)  # GA_ROOT
        if root:
            u.SetForegroundWindow(root)
    except Exception as e:
        log.debug("focus: %s", e)


def _typed_ok(expected: str) -> bool:
    return True


def dblclick_result_row(cfg: configparser.ConfigParser, ticker: str) -> bool:
    """В окне «Выбор инструмента» найти ячейку с точно таким символом и дважды кликнуть.
    Возвращает True, если строка найдена."""
    try:
        title = cfg.get("cscalp", "search_title", fallback="выбор инструмента")
        deadline = time.time() + 2.0
        while time.time() < deadline:
            dlg = find_window(title)
            if dlg is not None:
                for el in dlg.descendants():
                    try:
                        txt = (el.window_text() or "").strip().upper()
                    except Exception:
                        continue
                    if txt != ticker:
                        continue
                    try:
                        ct = el.element_info.control_type
                    except Exception:
                        ct = ""
                    if ct in ("Edit", "Window", "Pane", "TitleBar"):
                        continue  # поле ввода с тем же текстом — не строка
                    el.click_input(double=True)
                    log.info("выбрана строка %s (%s)", ticker, ct)
                    return True
            time.sleep(0.2)
    except Exception as e:
        log.debug("поиск строки результата: %s", e)
    return False


def switch_instrument(cfg: configparser.ConfigParser, ticker: str, state: dict) -> str:
    import pyautogui
    import pyperclip
    pyautogui.FAILSAFE = False
    # Главное окно CScalp через UIA не ищем (на многомониторных системах это
    # 0.3-0.5 с): клик по заголовку стакана сам поднимает CScalp. Наличие окна
    # проверяем только если поиск потом не открылся.
    # Клик строго по откалиброванной точке: у пользователя может быть много
    # стаканов, и автопоиск заголовка по тексту попадает в первый попавшийся.
    x = cfg.getint("cscalp", "click_x", fallback=0)
    y = cfg.getint("cscalp", "click_y", fallback=0)
    if not x and not y:
        return "точка заголовка стакана не откалибрована: запустите с --calibrate"
    pyautogui.moveTo(x, y)
    pyautogui.click(x, y)
    _focus_window_at(x, y)
    # ждём появления окна поиска, но не дольше wait_dialog; сколько ждали — в лог
    t_click = time.time()
    dlg = _search_dialog(cfg, wait=cfg.getfloat("cscalp", "wait_dialog", fallback=0.6))
    state["t_dialog"] = time.time() - t_click
    if dlg is None:
        if find_window(cfg.get("cscalp", "window_title", fallback="cscalp")) is None:
            return "окно CScalp не найдено"
        log.info("окно поиска не найдено за %.2f с (заголовок '%s'?) — иду по таймингам",
                 state["t_dialog"], cfg.get("cscalp", "search_title", fallback=""))
    time.sleep(cfg.getfloat("cscalp", "after_click", fallback=0.35))

    # 2) при необходимости кликнуть в поле поиска
    sx = cfg.getint("cscalp", "search_x", fallback=0)
    sy = cfg.getint("cscalp", "search_y", fallback=0)
    if sx or sy:
        pyautogui.click(sx, sy)
        time.sleep(0.2)

    # 3) ввести тикер. Печать по буквам не зависит от раскладки: используем
    #    латинские скан-коды через pywinauto.keyboard; paste — запасной вариант.
    pyautogui.hotkey("ctrl", "a")
    pyautogui.press("backspace")
    mode = cfg.get("cscalp", "input", fallback="type").lower()
    if mode == "paste":
        old_clip = None
        try:
            old_clip = pyperclip.paste()
        except Exception:
            pass
        pyperclip.copy(ticker)
        pyautogui.hotkey("ctrl", "v")
        if old_clip is not None:
            try:
                pyperclip.copy(old_clip)
            except Exception:
                pass
    else:
        _type_latin(ticker)
    waited = cfg.getfloat("cscalp", "after_type", fallback=1.0)
    time.sleep(waited)
    state["t_filter"] = waited
    # окно поиска должно быть активным; если нет — фокус ушёл, повторяем ввод в него
    try:
        dlg = _search_dialog(cfg, wait=0.0)
        if dlg is not None and not dlg.is_active():
            log.info("фокус был не в окне поиска — повторяю ввод")
            dlg.set_focus()
            time.sleep(0.1)
            pyautogui.hotkey("ctrl", "a")
            _type_latin(ticker)
            time.sleep(cfg.getfloat("cscalp", "after_type", fallback=1.0))
    except Exception as e:
        log.debug("проверка фокуса: %s", e)

    # 4) подтвердить выбор. Список в CScalp фильтруется с задержкой, и ранний
    #    двойной клик выбирает старую первую строку (ABIO). Поэтому: клик →
    #    проверяем, закрылось ли окно → если нет, ждём и повторяем (до 4 раз).
    confirm = cfg.get("cscalp", "confirm", fallback="auto").lower()
    if confirm in ("auto", "dblclick"):
        if confirm == "auto" and dblclick_result_row(cfg, ticker):
            pass
        else:
            pt = result_point(cfg)
            if pt is None:
                return "не откалибрована строка результата: python cscalp_bridge.py --calibrate3"
            rx, ry = pt
            retry_wait = cfg.getfloat("cscalp", "retry_wait", fallback=0.3)
            for attempt in range(4):
                pyautogui.moveTo(rx, ry)
                pyautogui.doubleClick(rx, ry, interval=0.05)
                t0 = time.time()
                closed = False
                while time.time() - t0 < 0.5:
                    if _search_dialog(cfg, wait=0.0) is None:
                        closed = True
                        break
                    time.sleep(0.03)
                if closed:
                    if attempt:
                        log.info("строка выбралась с попытки %d", attempt + 1)
                    break
                time.sleep(retry_wait)
    elif confirm == "down_enter":
        pyautogui.press("down")
        time.sleep(0.1)
        pyautogui.press("enter")
    else:
        pyautogui.press("enter")
    if _search_dialog(cfg, wait=0.0) is not None:
        pyautogui.press("esc")
        state["current"] = ticker
        return "строка не выбралась (окно поиска осталось открыто): проверьте --calibrate3"
    state["current"] = ticker
    px = cfg.getint("cscalp", "park_x", fallback=0)
    py_ = cfg.getint("cscalp", "park_y", fallback=0)
    if px or py_:
        pyautogui.moveTo(px, py_)
    return "ok"


def _type_latin(text: str) -> None:
    """Печатает латиницу/цифры независимо от активной раскладки (по скан-кодам)."""
    try:
        from pywinauto import keyboard
        # {VK_…} не нужны: keyboard.send_keys шлёт unicode-символы напрямую
        keyboard.send_keys(text, pause=0.01, with_spaces=True)
    except Exception:
        import pyautogui
        pyautogui.write(text, interval=0.01)


def _grab_point(what: str):
    """Ждём, пока мышь простоит неподвижно 3 с над нужным местом (без Enter,
    без Alt+Tab — терминал можно не трогать). Возвращает (x, y)."""
    import pyautogui
    print(f"Наведите мышь на {what} и ЗАМРИТЕ на 3 секунды. Терминал трогать не нужно.")
    time.sleep(2.0)  # время убрать руку с клавиатуры
    last = pyautogui.position()
    still = 0.0
    while True:
        time.sleep(0.25)
        p = pyautogui.position()
        if abs(p.x - last.x) <= 2 and abs(p.y - last.y) <= 2:
            still += 0.25
            if still >= 3.0:
                print(f"Зафиксировано: ({p.x}, {p.y})")
                try:
                    import winsound
                    winsound.Beep(1200, 150)
                except Exception:
                    pass
                return p.x, p.y
        else:
            still = 0.0
            last = p


def _calibrate_point(cfg: configparser.ConfigParser, kx: str, ky: str, what: str) -> None:
    x, y = _grab_point(what)
    cfg.set("cscalp", kx, str(x))
    cfg.set("cscalp", ky, str(y))
    save_cfg(cfg)
    print(f"Сохранено: ({x}, {y}).")


def inspect_search(cfg: configparser.ConfigParser, ticker: str) -> None:
    """Диагностика: открыть поиск, набрать тикер и распечатать элементы окна."""
    import pyautogui
    from pywinauto import Desktop
    pyautogui.FAILSAFE = False
    win = find_window(cfg.get("cscalp", "window_title", fallback="cscalp"))
    print("окно CScalp:", win.window_text() if win else "НЕ НАЙДЕНО")
    if win:
        win.set_focus(); time.sleep(0.2)
    pyautogui.click(cfg.getint("cscalp", "click_x"), cfg.getint("cscalp", "click_y"))
    time.sleep(0.8)
    _type_latin(ticker); time.sleep(1.0)
    print("--- окна верхнего уровня ---")
    for w in Desktop(backend="uia").windows():
        try:
            print(repr(w.window_text()), w.element_info.control_type, w.rectangle())
        except Exception:
            pass
    dlg = find_window(cfg.get("cscalp", "search_title", fallback="выбор инструмента"))
    print("--- окно поиска:", dlg.window_text() if dlg else "НЕ НАЙДЕНО", "---")
    if dlg is None:
        return
    n = 0
    for el in dlg.descendants():
        try:
            txt = (el.window_text() or "").strip()
            ct = el.element_info.control_type
            r = el.rectangle()
        except Exception:
            continue
        if txt or ct in ("DataItem", "ListItem", "Custom", "Text"):
            print(f"{ct:12} {r.left},{r.top}-{r.right},{r.bottom}  {txt!r}")
            n += 1
        if n > 150:
            print("…"); break
    print("Готово. Пришлите этот вывод.")


def _search_dialog(cfg: configparser.ConfigParser, wait: float = 2.0):
    """Окно «Выбор инструмента»: верхнеуровневое окно любого типа, а если нет —
    дочернее окно внутри главного окна CScalp (CScalp может открывать его как
    owned/child window)."""
    from pywinauto import Desktop
    title = cfg.get("cscalp", "search_title", fallback="выбор инструмента").lower()
    deadline = time.time() + wait
    while True:
        try:
            for w in Desktop(backend="uia").windows():
                try:
                    if title in (w.window_text() or "").lower():
                        return w
                except Exception:
                    continue
            # дочерние окна CScalp
            main = find_window(cfg.get("cscalp", "window_title", fallback="cscalp"))
            if main is not None:
                for w in main.children():
                    try:
                        if title in (w.window_text() or "").lower():
                            return w
                    except Exception:
                        continue
        except Exception as e:
            log.debug("поиск окна: %s", e)
        if time.time() >= deadline:
            return None
        time.sleep(0.03)


def result_point(cfg: configparser.ConfigParser):
    """Абсолютная точка первой строки результата: смещение от окна поиска,
    если оно найдено, иначе — сохранённые абсолютные координаты."""
    dx = cfg.getint("cscalp", "result_dx", fallback=0)
    dy = cfg.getint("cscalp", "result_dy", fallback=0)
    if dx or dy:
        dlg = _search_dialog(cfg)
        if dlg is not None:
            try:
                r = dlg.rectangle()
                return r.left + dx, r.top + dy
            except Exception:
                pass
        else:
            log.warning("окно поиска не найдено — кликаю по абсолютной точке")
    rx = cfg.getint("cscalp", "result_x", fallback=0)
    ry = cfg.getint("cscalp", "result_y", fallback=0)
    return (rx, ry) if (rx or ry) else None


def calibrate_result(cfg: configparser.ConfigParser) -> None:
    print("Откройте в CScalp поиск, наберите SBER.")
    x, y = _grab_point("строку «SBER TQBR Сбербанк» в окне поиска")
    cfg.set("cscalp", "result_x", str(x))
    cfg.set("cscalp", "result_y", str(y))
    dlg = _search_dialog(cfg, wait=0.5)
    if dlg is not None:
        r = dlg.rectangle()
        cfg.set("cscalp", "result_dx", str(x - r.left))
        cfg.set("cscalp", "result_dy", str(y - r.top))
        print(f"Сохранено: ({x}, {y}); окно поиска {r.left},{r.top} → смещение "
              f"({x - r.left}, {y - r.top}). Окно можно двигать — попадём.")
    else:
        cfg.set("cscalp", "result_dx", "0")
        cfg.set("cscalp", "result_dy", "0")
        print(f"Сохранено: ({x}, {y}). ВНИМАНИЕ: окно «Выбор инструмента» не найдено, "
              f"точка абсолютная — окно поиска нельзя двигать.")
    save_cfg(cfg)


def calibrate(cfg: configparser.ConfigParser) -> None:
    x, y = _grab_point("заголовок стакана CScalp (где написан тикер)")
    cfg.set("cscalp", "click_x", str(x))
    cfg.set("cscalp", "click_y", str(y))
    save_cfg(cfg)
    print(f"Сохранено: ({x}, {y}). Проверка: python cscalp_bridge.py --test SBER")


# --------------------------------------------------------------- транспорт

def _single_instance() -> bool:
    """Не даём запустить две копии мостика (иначе тикер введётся дважды)."""
    try:
        import ctypes
        h = ctypes.windll.kernel32.CreateMutexW(None, False, "Global\\cscalp_bridge_mutex")
        return ctypes.windll.kernel32.GetLastError() != 183  # ERROR_ALREADY_EXISTS
    except Exception:
        return True


def relay_loop(cfg: configparser.ConfigParser) -> None:
    """Режим relay: long-poll топика ntfy, ответ — в топик …-ack."""
    if not _single_instance():
        log.error("мостик уже запущен — вторая копия завершается")
        return
    key = cfg.get("bot", "key")
    relay = cfg.get("bot", "relay", fallback="https://ntfy.sh").rstrip("/")
    if key == "CHANGE_ME":
        log.error("впишите key в %s", INI)
        return
    topic = "cscalp-" + key
    state: dict = {}
    sess = requests.Session()
    log.info("мостик %s запущен (relay %s)", VERSION, relay)
    since = "5s"
    backoff = 1.0
    while True:
        try:
            # держим соединение до 60 с, ntfy отдаёт сообщения по мере поступления
            r = sess.get(f"{relay}/{topic}/json", params={"since": since}, stream=True, timeout=(10, 70))
            if r.status_code != 200:
                log.warning("relay HTTP %s", r.status_code); time.sleep(5); continue
            backoff = 1.0
            for line in r.iter_lines():
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                if msg.get("event") != "message":
                    continue
                since = str(msg.get("id") or since)
                try:
                    cmd = json.loads(msg.get("message", "{}"))
                except Exception:
                    continue
                t = str(cmd.get("ticker", "")).upper()
                if not t or time.time() - float(cmd.get("ts", 0)) > 60:
                    continue
                t0 = time.time()
                res = switch_instrument(cfg, t, state)
                log.info("→ %s: %s (%.2f с)", t, res, time.time() - t0)
                try:
                    sess.post(f"{relay}/{topic}-ack", data=json.dumps(
                        {"id": cmd.get("id"), "ticker": t, "result": res}), timeout=10)
                except Exception:
                    pass
        except requests.exceptions.ReadTimeout:
            continue
        except Exception as e:
            log.warning("связь: %s", e)
            time.sleep(backoff); backoff = min(backoff * 2, 30)


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
    if args and args[0] == "--version":
        print(VERSION); return
    if args and args[0] == "--calibrate":
        calibrate(cfg)
    elif args and args[0] == "--calibrate2":
        _calibrate_point(cfg, "search_x", "search_y",
                         "поле ввода в открывшемся окне поиска инструмента (окно должно быть открыто)")
    elif args and args[0] == "--calibrate3":
        calibrate_result(cfg)
    elif args and args[0] == "--calibrate4":
        _calibrate_point(cfg, "park_x", "park_y",
                         "место, куда ставить курсор после смены тикера (например, середину 9-го стакана)")
    elif args and args[0] == "--timing":
        t = args[1].upper() if len(args) > 1 else "OZON"
        st: dict = {}
        t0 = time.time()
        res = switch_instrument(cfg, t, st)
        print(f"{t}: {res}, всего {time.time() - t0:.2f} с; окно поиска через "
              f"{st.get('t_dialog', -1):.2f} с; фильтрация {st.get('t_filter', -1):.2f} с")
    elif args and args[0] == "--tune":
        # подбор after_type: пробуем 0.6 → 0.2, после каждого спрашиваем, верно ли выбралось
        import pyautogui
        tickers = ["OZON", "SBER", "GAZP", "LKOH", "YDEX"]
        best = None
        for i, at in enumerate([0.6, 0.45, 0.35, 0.25, 0.2]):
            cfg.set("cscalp", "after_type", str(at))
            t = tickers[i % len(tickers)]
            t0 = time.time()
            res = switch_instrument(cfg, t, {})
            ans = input(f"after_type={at}: {t} за {time.time() - t0:.2f} с, результат '{res}'. "
                        f"В стакане именно {t}? (y/n): ").strip().lower()
            if ans.startswith("y") or ans.startswith("д"):
                best = at
            else:
                break
        if best is None:
            best = 0.8
        cfg.set("cscalp", "after_type", str(best))
        save_cfg(cfg)
        print(f"Сохранено after_type = {best}")
    elif args and args[0] == "--show":
        import pyautogui
        x, y = cfg.getint("cscalp", "click_x"), cfg.getint("cscalp", "click_y")
        print(f"Точка заголовка стакана: ({x}, {y}). Веду туда мышь на 3 с — смотрите, куда она встала.")
        pyautogui.moveTo(x, y, duration=0.5); time.sleep(3)
        print("Теперь откройте поиск руками, наберите SBER — покажу точку строки результата.")
        time.sleep(6)
        pt = result_point(cfg)
        print(f"Точка строки результата: {pt}")
        if pt:
            pyautogui.moveTo(pt[0], pt[1], duration=0.5); time.sleep(3)
        print("Если обе точки верные — python cscalp_bridge.py --test SBER")
    elif args and args[0] == "--inspect":
        inspect_search(cfg, args[1].upper() if len(args) > 1 else "SBER")
    elif args and args[0] == "--test":
        t = args[1].upper() if len(args) > 1 else "SBER"
        print(switch_instrument(cfg, t, {}))
    elif cfg.get("bot", "mode", fallback="relay").lower() == "http":
        poll_loop(cfg)
    else:
        relay_loop(cfg)


if __name__ == "__main__":
    main()
