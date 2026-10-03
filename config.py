"""Конфигурация. Значения можно переопределить переменными окружения или файлом .env."""
import os


def _load_dotenv(path: str = ".env") -> None:
    """Минимальный .env-загрузчик без внешних зависимостей."""
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_load_dotenv()

# --- Telegram ---
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

# --- MOEX ISS API (бесплатный, без ключа) ---
MOEX_BASE: str = "https://iss.moex.com/iss"
# Основной торговый режим для акций (T+). Fallback'и для редких тикеров.
MOEX_BOARDS: tuple[str, ...] = ("TQBR", "TQDR", "SPEQ")

# --- Мониторинг ---
# Как часто (сек) опрашивать цены
CHECK_INTERVAL_SEC: int = int(os.getenv("CHECK_INTERVAL_SEC", "60"))
# Данные старше N минут считаем «рынок закрыт / нет торгов»
FRESHNESS_MIN: int = 10
# Кэш справочника ценных бумаг (сек)
SEC_INFO_TTL_SEC: int = 3600

# --- Настройки по умолчанию для нового пользователя ---
DEFAULT_THRESHOLD_PCT: float = 5.0     # алерт при |изменении за день| >= 5%
DEFAULT_COOLDOWN_MIN: float = 30.0     # пауза между повторными алертами, мин
DEFAULT_REPORT_MIN: float = 60.0       # период сводки, мин (0 = выключено)

# --- Хранилище настроек пользователей ---
DATA_FILE: str = os.getenv("DATA_FILE", "data/users.json")
