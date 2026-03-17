#!/usr/bin/env python3
"""
setup.py – интерактивная утилита настройки EDI Message Sender.

Запускайте отдельно для создания или редактирования конфигурации:
    python setup.py

Конфиг сохраняется в edi_config.json рядом со скриптом.
"""

import copy
import json
import os
import sys
from pathlib import Path

# Добавляем папку проекта в путь (на случай запуска из другого места)
sys.path.insert(0, str(Path(__file__).parent))

import config as cfg_module
from config import CONFIG_FILE, DEFAULT_CONFIG


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции ввода
# ─────────────────────────────────────────────────────────────────────────────

def clr():
    os.system("cls" if os.name == "nt" else "clear")


def _prompt(label: str, current: str = "", secret: bool = False) -> str:
    """
    Показывает текущее значение поля и предлагает ввести новое.
    Если пользователь нажимает Enter — оставляет текущее.
    """
    if secret and current:
        display = "*" * min(len(current), 8) + "…"
    elif current:
        display = current
    else:
        display = "<не задано>"

    hint = f" [{display}]" if current else " [Enter — пропустить]"
    new_val = input(f"  {label}{hint}: ").strip()
    return new_val if new_val else current


def _choose(label: str, options: list[str], current: str = "") -> str:
    """Выбор одного из вариантов."""
    opts_str = " / ".join(
        f"[{o}]" if o == current else o for o in options
    )
    while True:
        val = input(f"  {label} ({opts_str}): ").strip().lower()
        if not val and current:
            return current
        if val in options:
            return val
        print(f"  Введите одно из: {', '.join(options)}")


def _section(title: str):
    print(f"\n{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}")


# ─────────────────────────────────────────────────────────────────────────────
# Разделы настройки
# ─────────────────────────────────────────────────────────────────────────────

def setup_auth(cfg: dict) -> dict:
    _section("Авторизация")
    print("  oidc   — OpenID Connect, Device Flow (рекомендуется)")
    print("  legacy — старый метод login/password (устарел)")
    cfg["auth_mode"] = _choose("Режим", ["oidc", "legacy"], cfg.get("auth_mode", "oidc"))

    if cfg["auth_mode"] == "oidc":
        _section("OIDC — Данные приложения (Кабинет интегратора)")
        print("  Получить: https://integrations.kontur.ru/")
        oidc = cfg.setdefault("oidc", copy.deepcopy(DEFAULT_CONFIG["oidc"]))
        oidc["client_id"]     = _prompt("client_id",     oidc.get("client_id", ""))
        oidc["client_secret"] = _prompt("client_secret", oidc.get("client_secret", ""), secret=True)
        print("\n  Scope зависит от площадки:")
        print("    edi-public-api-staging    — тестовая")
        print("    edi-public-api-production — продуктовая")
        oidc["scope"] = _prompt("scope", oidc.get("scope", "edi-public-api-staging"))
    else:
        _section("Legacy — Учётные данные API")
        leg = cfg.setdefault("legacy", copy.deepcopy(DEFAULT_CONFIG["legacy"]))
        leg["api_client_id"] = _prompt("api_client_id", leg.get("api_client_id", ""))
        leg["login"]         = _prompt("login",         leg.get("login", ""))
        leg["password"]      = _prompt("password",      leg.get("password", ""), secret=True)

    return cfg


def setup_api(cfg: dict) -> dict:
    _section("API endpoint")
    print("  Тестовая площадка:    https://test-edi-api.kontur.ru")
    print("  Продуктовая площадка: https://edi-api.kontur.ru")
    cfg["api_base_url"] = _prompt(
        "api_base_url",
        cfg.get("api_base_url", DEFAULT_CONFIG["api_base_url"])
    )
    return cfg


def setup_edi(cfg: dict) -> dict:
    _section("Организация (EDI)")
    edi = cfg.setdefault("edi", copy.deepcopy(DEFAULT_CONFIG["edi"]))
    edi["party_id"]   = _prompt("party_id  (GUID организации)", edi.get("party_id", ""))
    edi["buyer_gln"]  = _prompt("buyer_gln  (GLN покупателя)",   edi.get("buyer_gln", ""))
    edi["seller_gln"] = _prompt("seller_gln (GLN поставщика)",   edi.get("seller_gln", ""))
    return cfg


def setup_line_item(cfg: dict) -> dict:
    _section("Позиция ORDERS по умолчанию (режим 1)")
    print("  Используется при генерации тестового заказа из конфига.")
    li = cfg.setdefault("line_item_defaults", copy.deepcopy(DEFAULT_CONFIG["line_item_defaults"]))
    li["gtin"]               = _prompt("GTIN",                   li.get("gtin", ""))
    li["internal_buyer_code"]= _prompt("internal_buyer_code",    li.get("internal_buyer_code", ""))
    li["description"]        = _prompt("description",            li.get("description", ""))
    li["requested_quantity"] = _prompt("requested_quantity",     li.get("requested_quantity", ""))
    li["unit_of_measure"]    = _prompt("unit_of_measure",        li.get("unit_of_measure", "PCE"))
    li["net_price"]          = _prompt("net_price",              li.get("net_price", ""))
    li["vat_rate"]           = _prompt("vat_rate",               li.get("vat_rate", "22"))
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Главное меню setup
# ─────────────────────────────────────────────────────────────────────────────

def show_current(cfg: dict):
    """Показать краткую сводку текущего конфига."""
    _section("Текущая конфигурация")
    mode = cfg.get("auth_mode", "—")
    print(f"  Режим авторизации : {mode}")
    print(f"  API URL           : {cfg.get('api_base_url', '—')}")

    if mode == "oidc":
        oidc = cfg.get("oidc", {})
        cid = oidc.get("client_id", "")
        print(f"  OIDC client_id    : {cid or '—'}")
        print(f"  OIDC scope        : {oidc.get('scope', '—')}")
    else:
        leg = cfg.get("legacy", {})
        print(f"  api_client_id     : {leg.get('api_client_id', '—')}")
        print(f"  login             : {leg.get('login', '—')}")

    edi = cfg.get("edi", {})
    print(f"  party_id          : {edi.get('party_id', '—')}")
    print(f"  buyer_gln         : {edi.get('buyer_gln', '—')}")
    print(f"  seller_gln        : {edi.get('seller_gln', '—')}")

    li = cfg.get("line_item_defaults", {})
    if any(li.values()):
        print(f"  GTIN              : {li.get('gtin', '—')}")
        print(f"  Описание          : {li.get('description', '—')}")


def main():
    clr()
    print("╔══════════════════════════════════════════╗")
    print("║   EDI Message Sender — Настройка         ║")
    print("╚══════════════════════════════════════════╝")

    # Загружаем существующий конфиг или создаём пустой
    cfg = cfg_module.load()
    if not cfg:
        print(f"\n  Файл {CONFIG_FILE.name} не найден — будет создан новый.")
        cfg = copy.deepcopy(DEFAULT_CONFIG)
    else:
        print(f"\n  Конфиг загружен: {CONFIG_FILE}")

    while True:
        show_current(cfg)
        print("\n  Что настроить?")
        print("  1. Авторизация (auth_mode, oidc / legacy)")
        print("  2. API endpoint (URL площадки)")
        print("  3. Организация (party_id, GLN)")
        print("  4. Позиция ORDERS по умолчанию")
        print("  5. Сохранить и выйти")
        print("  6. Выйти без сохранения")
        print("  7. Сбросить конфиг до значений по умолчанию")

        choice = input("\n  Выбор: ").strip()

        if choice == "1":
            cfg = setup_auth(cfg)
        elif choice == "2":
            cfg = setup_api(cfg)
        elif choice == "3":
            cfg = setup_edi(cfg)
        elif choice == "4":
            cfg = setup_line_item(cfg)
        elif choice == "5":
            cfg_module.save(cfg)
            print(f"\n  ✓ Конфиг сохранён: {CONFIG_FILE}\n")
            break
        elif choice == "6":
            print("\n  Изменения не сохранены.\n")
            break
        elif choice == "7":
            confirm = input("  Сбросить всё? (yes/no): ").strip().lower()
            if confirm == "yes":
                cfg = copy.deepcopy(DEFAULT_CONFIG)
                print("  Конфиг сброшен (ещё не сохранён).")
        else:
            print("  Неверный выбор.")


if __name__ == "__main__":
    main()
