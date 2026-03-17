"""
auth.py – авторизация (OIDC Device Flow или legacy login/password).

Принимает AppConfig вместо глобальных переменных.
"""

import json
import logging
import time
import webbrowser
from datetime import datetime, timedelta

import requests

from config import AppConfig, TOKEN_CACHE, OIDC_DEVICE_URL, OIDC_TOKEN_URL

logger = logging.getLogger(__name__)

_REFRESH_BUFFER = 300   # секунд до истечения — обновляем заранее
_LEGACY_HOURS   = 12    # срок жизни legacy-токена


# ─────────────────────────────────────────────────────────────────────────────
# Кэш токена
# ─────────────────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if not TOKEN_CACHE.exists():
        return {}
    try:
        return json.loads(TOKEN_CACHE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Не удалось прочитать кэш токена: %s", exc)
        return {}


def _save_cache(data: dict) -> None:
    try:
        TOKEN_CACHE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Не удалось сохранить кэш токена: %s", exc)


def _clear_cache() -> None:
    if TOKEN_CACHE.exists():
        TOKEN_CACHE.unlink()


def _is_fresh(cache: dict, key: str = "expiry", buffer: int = 0) -> bool:
    try:
        expiry = datetime.fromisoformat(cache[key])
        return datetime.now() < expiry - timedelta(seconds=buffer)
    except (KeyError, ValueError):
        return False


# ─────────────────────────────────────────────────────────────────────────────
# OIDC — Device Authorization Flow
# ─────────────────────────────────────────────────────────────────────────────

def _device_flow(cfg: AppConfig, dl) -> dict:
    payload = {
        "client_id":     cfg.oidc_client_id,
        "client_secret": cfg.oidc_client_secret,
        "scope":         cfg.oidc_scope,
    }
    dl.info("OIDC Device Auth → POST %s", OIDC_DEVICE_URL)
    resp = requests.post(OIDC_DEVICE_URL, data=payload, timeout=30)
    dl.info("Status %s: %s", resp.status_code, resp.text)

    if resp.status_code != 200:
        logger.error("Device auth failed (%d): %s", resp.status_code, resp.text)
        raise RuntimeError("OIDC device auth failed")

    dev          = resp.json()
    device_code  = dev["device_code"]
    user_code    = dev["user_code"]
    verify_uri   = dev.get("verification_uri_complete") or dev["verification_uri"]
    interval     = dev.get("interval", 5)
    expires_in   = dev.get("expires_in", 300)

    print("\n" + "─" * 54)
    print("  Откройте ссылку для авторизации:")
    print(f"  {verify_uri}")
    if "verification_uri_complete" not in dev:
        print(f"  Код подтверждения: {user_code}")
    print("─" * 54)

    try:
        webbrowser.open(verify_uri)
    except Exception:
        pass

    token_req = {
        "grant_type":    "urn:ietf:params:oauth:grant-type:device_code",
        "device_code":   device_code,
        "client_id":     cfg.oidc_client_id,
        "client_secret": cfg.oidc_client_secret,
    }

    logger.info("Ожидаем подтверждения в браузере...")
    deadline = time.monotonic() + expires_in
    while time.monotonic() < deadline:
        time.sleep(interval)
        r = requests.post(OIDC_TOKEN_URL, data=token_req, timeout=30)
        dl.info("Poll → %s %s", r.status_code, r.text)

        if r.status_code == 200:
            logger.info("OIDC авторизация успешна.")
            return r.json()

        err = r.json().get("error", "")
        if err == "authorization_pending":
            continue
        if err == "slow_down":
            interval += 5
            continue
        if err == "expired_token":
            raise RuntimeError("Время авторизации истекло.")
        raise RuntimeError(f"Ошибка токена: {err} — {r.text}")

    raise RuntimeError("Время авторизации истекло.")


def _oidc_refresh(refresh_token: str, cfg: AppConfig, dl) -> dict:
    payload = {
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
        "client_id":     cfg.oidc_client_id,
        "client_secret": cfg.oidc_client_secret,
    }
    r = requests.post(OIDC_TOKEN_URL, data=payload, timeout=30)
    dl.info("Refresh → %s %s", r.status_code, r.text)
    if r.status_code == 200:
        logger.info("OIDC токен обновлён через refresh_token.")
        return r.json()
    logger.warning("refresh_token недействителен (%d) — повторная авторизация.", r.status_code)
    return _device_flow(cfg, dl)


def _get_oidc_token(cfg: AppConfig, dl) -> str:
    cache = _load_cache()

    # Живой access_token
    if cache.get("access_token") and _is_fresh(cache, buffer=_REFRESH_BUFFER):
        logger.info("Используется кэшированный OIDC токен (до %s).", cache["expiry"])
        return cache["access_token"]

    # Обновляем через refresh_token
    if cache.get("refresh_token"):
        logger.info("Обновляем OIDC токен...")
        token_data = _oidc_refresh(cache["refresh_token"], cfg, dl)
    else:
        logger.info("Кэш пуст — запускаем Device Flow...")
        token_data = _device_flow(cfg, dl)

    expiry = datetime.now() + timedelta(seconds=token_data.get("expires_in", 3600))
    _save_cache({
        "auth_mode":     "oidc",
        "access_token":  token_data["access_token"],
        "refresh_token": token_data.get("refresh_token", ""),
        "expiry":        expiry.isoformat(),
    })
    return token_data["access_token"]


# ─────────────────────────────────────────────────────────────────────────────
# Legacy
# ─────────────────────────────────────────────────────────────────────────────

def _legacy_auth(cfg: AppConfig, dl) -> str:
    url = f"{cfg.api_base_url}/V1/Authenticate"
    headers = {
        "Authorization": (
            f"KonturEdiAuth konturediauth_api_client_id={cfg.api_client_id}, "
            f"konturediauth_login={cfg.login}, "
            f"konturediauth_password={cfg.password}"
        )
    }
    dl.info("Legacy Auth → POST %s", url)
    resp = requests.post(url, headers=headers, timeout=30)
    dl.info("Status %s: %s", resp.status_code, resp.text)

    if resp.status_code == 200:
        logger.info("Legacy авторизация успешна.")
        return resp.text.strip()

    raise RuntimeError(f"Legacy auth failed ({resp.status_code}): {resp.text}")


def _get_legacy_token(cfg: AppConfig, dl) -> str:
    cache = _load_cache()
    if (cache.get("auth_mode") == "legacy"
            and cache.get("token")
            and _is_fresh(cache)):
        logger.info("Используется кэшированный legacy-токен (до %s).", cache["expiry"])
        return cache["token"]

    token  = _legacy_auth(cfg, dl)
    expiry = datetime.now() + timedelta(hours=_LEGACY_HOURS)
    _save_cache({
        "auth_mode": "legacy",
        "token":     token,
        "expiry":    expiry.isoformat(),
    })
    return token


# ─────────────────────────────────────────────────────────────────────────────
# Публичный API
# ─────────────────────────────────────────────────────────────────────────────

def get_token(cfg: AppConfig, dl) -> str:
    """Вернуть действующий токен (из кэша или после авторизации)."""
    if cfg.auth_mode == "oidc":
        return _get_oidc_token(cfg, dl)
    return _get_legacy_token(cfg, dl)


def build_auth_header(token: str, cfg: AppConfig) -> str:
    """Собрать значение заголовка Authorization."""
    if cfg.auth_mode == "oidc":
        return f"Bearer {token}"
    return (
        f"KonturEdiAuth konturediauth_api_client_id={cfg.api_client_id}, "
        f"konturediauth_token={token}"
    )


def invalidate_token() -> None:
    """Удалить кэш токена (при 401)."""
    _clear_cache()
