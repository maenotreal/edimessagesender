"""
config.py – загрузка и сохранение конфигурации из JSON-файла.

Файл конфигурации: edi_config.json (рядом с config.py).
Создаётся автоматически при первом запуске через setup.py.

Структура файла:
{
  "auth_mode": "oidc",          // "oidc" | "legacy"
  "api_base_url": "https://...",

  "oidc": {
    "client_id": "...",
    "client_secret": "...",
    "scope": "edi-public-api-staging"
  },

  "legacy": {
    "api_client_id": "...",
    "login": "...",
    "password": "..."
  },

  "edi": {
    "party_id": "...",
    "buyer_gln": "...",
    "seller_gln": "..."
  },

  "line_item_defaults": {
    "gtin": "...",
    "internal_buyer_code": "...",
    "description": "...",
    "requested_quantity": "...",
    "unit_of_measure": "PCE",
    "net_price": "...",
    "vat_rate": "22"
  }
}
"""

import json
import sys
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Путь к конфигу — всегда рядом со скриптом
_BASE_DIR   = Path(__file__).parent
CONFIG_FILE = _BASE_DIR / "edi_config.json"
TOKEN_CACHE = _BASE_DIR / ".token_cache.json"

# Kontur OIDC endpoints (не меняются)
OIDC_ISSUER     = "https://identity.kontur.ru"
OIDC_DEVICE_URL = f"{OIDC_ISSUER}/connect/deviceauthorization"
OIDC_TOKEN_URL  = f"{OIDC_ISSUER}/connect/token"

# Тестовая организация (режим 3)
TEST_PARTY_ID   = "a7c51e44-9bb3-4420-8842-0e266ae50a2f"
TEST_SENDER_GLN = "2000002005070"

# ─────────────────────────────────────────────────────────────────────────────
# Значения по умолчанию
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG: dict = {
    "auth_mode":   "oidc",
    "api_base_url": "https://test-edi-api.kontur.ru",
    "oidc": {
        "client_id":     "",
        "client_secret": "",
        "scope":         "edi-public-api-staging",
    },
    "legacy": {
        "api_client_id": "",
        "login":         "",
        "password":      "",
    },
    "edi": {
        "party_id":   "",
        "buyer_gln":  "",
        "seller_gln": "",
    },
    "line_item_defaults": {
        "gtin":               "",
        "internal_buyer_code": "",
        "description":        "",
        "requested_quantity": "",
        "unit_of_measure":    "PCE",
        "net_price":          "",
        "vat_rate":           "22",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def load() -> dict:
    """Загрузить конфиг из файла. Возвращает словарь."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        with CONFIG_FILE.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.error("Ошибка чтения %s: %s", CONFIG_FILE, exc)
        return {}


def save(cfg: dict) -> None:
    """Сохранить конфиг в файл."""
    try:
        with CONFIG_FILE.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.error("Ошибка записи %s: %s", CONFIG_FILE, exc)
        raise


def create_default() -> dict:
    """Создать файл конфига со значениями по умолчанию и вернуть его."""
    import copy
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    save(cfg)
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Удобные геттеры (читают конфиг один раз при первом обращении)
# ─────────────────────────────────────────────────────────────────────────────

def _deep_get(d: dict, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, {})
    return d if d != {} else default


class AppConfig:
    """
    Обёртка над словарём конфига.
    Предоставляет атрибуты, аналогичные старому config.py.
    Вызов AppConfig() загружает актуальный файл с диска.
    """

    def __init__(self):
        self._cfg = load()
        if not self._cfg:
            logger.warning(
                "Файл конфигурации не найден (%s). "
                "Запустите setup.py для настройки.",
                CONFIG_FILE,
            )

    # ── Общее ────────────────────────────────────────────────────────────────
    @property
    def auth_mode(self) -> str:
        return self._cfg.get("auth_mode", "oidc").lower()

    @property
    def api_base_url(self) -> str:
        return self._cfg.get("api_base_url", "").rstrip("/")

    # ── OIDC ─────────────────────────────────────────────────────────────────
    @property
    def oidc_client_id(self) -> str:
        return _deep_get(self._cfg, "oidc", "client_id", default="")

    @property
    def oidc_client_secret(self) -> str:
        return _deep_get(self._cfg, "oidc", "client_secret", default="")

    @property
    def oidc_scope(self) -> str:
        return _deep_get(self._cfg, "oidc", "scope", default="edi-public-api-staging")

    # ── Legacy ───────────────────────────────────────────────────────────────
    @property
    def api_client_id(self) -> str:
        return _deep_get(self._cfg, "legacy", "api_client_id", default="")

    @property
    def login(self) -> str:
        return _deep_get(self._cfg, "legacy", "login", default="")

    @property
    def password(self) -> str:
        return _deep_get(self._cfg, "legacy", "password", default="")

    # ── EDI ──────────────────────────────────────────────────────────────────
    @property
    def party_id(self) -> str:
        return _deep_get(self._cfg, "edi", "party_id", default="")

    @property
    def buyer_gln(self) -> str:
        return _deep_get(self._cfg, "edi", "buyer_gln", default="")

    @property
    def seller_gln(self) -> str:
        return _deep_get(self._cfg, "edi", "seller_gln", default="")

    @property
    def line_item_defaults(self) -> dict:
        return self._cfg.get("line_item_defaults", {})

    # ── Валидация ────────────────────────────────────────────────────────────
    def validate_auth(self) -> list[str]:
        """Вернуть список незаполненных обязательных полей авторизации."""
        if self.auth_mode == "oidc":
            return [name for name, val in [
                ("oidc.client_id",     self.oidc_client_id),
                ("oidc.client_secret", self.oidc_client_secret),
            ] if not val]
        else:
            return [name for name, val in [
                ("legacy.api_client_id", self.api_client_id),
                ("legacy.login",         self.login),
                ("legacy.password",      self.password),
            ] if not val]

    def validate_edi(self) -> list[str]:
        """Вернуть список незаполненных полей EDI."""
        return [name for name, val in [
            ("edi.party_id",   self.party_id),
            ("edi.buyer_gln",  self.buyer_gln),
            ("edi.seller_gln", self.seller_gln),
        ] if not val]
