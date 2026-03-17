"""
api.py – HTTP-обёртки над Kontur EDI API.

Все вызовы проходят через _request():
  - правильный заголовок Authorization (OIDC Bearer или legacy)
  - логирование запроса/ответа в файл
  - retry один раз при 401
  - бросает RuntimeError при ошибках
"""

import base64
import logging
import time
from typing import Any

import requests

_RETRY_ON = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0   # секунды
_REQUEST_TIMEOUT = 60  # секунды
_REQUEST_TIMEOUT = 60  # секунды

from auth import build_auth_header, get_token, invalidate_token
from config import AppConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Логирование
# ─────────────────────────────────────────────────────────────────────────────

def _is_xml(body: Any) -> bool:
    sample = (body[:100].decode("utf-8", errors="ignore")
              if isinstance(body, bytes) else str(body)[:100])
    s = sample.lstrip()
    return s.startswith("<?xml") or s.startswith("<")


def _log_req(dl, method, url, headers, body=None):
    dl.info("→ %s %s", method.upper(), url)
    dl.info("  headers: %s", {k: v for k, v in headers.items()
                               if "authorization" not in k.lower()})
    if body is not None:
        dl.info("  body: %s", "<XML>" if _is_xml(body) else str(body)[:500])


def _log_resp(dl, resp):
    dl.info("← %s", resp.status_code)
    ct = resp.headers.get("Content-Type", "")
    if "xml" in ct.lower() or _is_xml(resp.text):
        dl.info("  body: <XML>")
    else:
        dl.info("  body: %s", resp.text[:1000])


# ─────────────────────────────────────────────────────────────────────────────
# Ядро HTTP
# ─────────────────────────────────────────────────────────────────────────────

def _request(method: str, path: str, cfg: AppConfig, dl,
             token: str, params: dict = None,
             data: Any = None, extra_headers: dict = None) -> requests.Response:
    url = f"{cfg.api_base_url}{path}"

    def _do(tok: str) -> requests.Response:
        hdrs = {"Authorization": build_auth_header(tok, cfg)}
        if extra_headers:
            hdrs.update(extra_headers)
        full = requests.Request(method.upper(), url, params=params).prepare().url
        _log_req(dl, method, full, hdrs, data)
        for attempt in range(_MAX_RETRIES):
            try:
                r = requests.request(method, url, params=params, headers=hdrs,
                                     data=data, timeout=_REQUEST_TIMEOUT)
                _log_resp(dl, r)
                return r
            except _RETRY_ON as exc:
                if attempt == _MAX_RETRIES - 1:
                    raise
                wait = _BACKOFF_BASE * (2 ** attempt)
                logger.warning("Сетевая ошибка (%s), повтор через %.0f с...", exc, wait)
                time.sleep(wait)

    resp = _do(token)

    if resp.status_code == 401:
        logger.warning("401 — обновляем токен и повторяем...")
        invalidate_token()
        new_tok = get_token(cfg, dl)
        resp = _do(new_tok)

    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Публичные методы
# ─────────────────────────────────────────────────────────────────────────────

def get_box_id(party_id: str, cfg: AppConfig, token: str, dl) -> str:
    """GET /V1/Boxes/GetMainApiBox → boxId."""
    logger.info("Получаем boxId для party %s", party_id)
    resp = _request("GET", "/V1/Boxes/GetMainApiBox", cfg, dl, token,
                    params={"partyId": party_id})

    if resp.status_code == 200:
        box_id = resp.json().get("Id")
        if not box_id:
            raise RuntimeError("API вернул BoxInfo без поля Id")
        logger.info("boxId: %s", box_id)
        return box_id
    if resp.status_code == 404:
        raise RuntimeError(
            f"Ящик API не найден для party {party_id} "
            "(транспорт организации — не API)"
        )
    raise RuntimeError(f"Ошибка получения boxId ({resp.status_code}): {resp.text}")


def send_message(box_id: str, cfg: AppConfig, token: str, dl,
                 content: Any, filename: str) -> dict:
    """POST /V1/Messages/SendMessage → dict ответа."""
    if isinstance(content, str):
        content = content.encode("utf-8")

    logger.info("Отправляем %s → ящик %s", filename, box_id)
    resp = _request(
        "POST", "/V1/Messages/SendMessage", cfg, dl, token,
        params={"boxId": box_id, "messageFileName": filename},
        data=content,
        extra_headers={"Content-Type": "application/octet-stream"},
    )

    if resp.status_code == 200:
        logger.info("Сообщение отправлено.")
        return resp.json()

    raise RuntimeError(f"Ошибка отправки ({resp.status_code}): {resp.text}")


def get_events(box_id: str, cfg: AppConfig, token: str, dl,
               exclusive_event_id: str = "",
               count: int = 1000) -> dict:
    """
    GET /V1/Messages/GetEvents → {Events: [...], LastEventId: str}

    Events[i] структура:
      EventId, EventPointer, EventDateTime, EventType, EventContent, BoxId, PartyId

    Для типа NewInboxMessage:
      EventContent.InboxMessageMeta.MessageId   — id для GetInboxMessage
      EventContent.InboxMessageMeta.DocumentDetails.DocumentType  — тип документа

    Для типов MessageDelivered / MessageCheckingOk / MessageCheckingFail:
      EventContent.OutboxMessageMeta.MessageId  — id исходящего сообщения
    """
    params: dict = {"boxId": box_id, "count": count}
    if exclusive_event_id:
        params["exclusiveEventId"] = exclusive_event_id

    resp = _request("GET", "/V1/Messages/GetEvents", cfg, dl, token, params=params)
    if resp.status_code == 200:
        return resp.json()
    raise RuntimeError(f"GetEvents failed ({resp.status_code}): {resp.text}")


def get_events_from(box_id: str, cfg: AppConfig, token: str, dl,
                    from_date: str, count: int = 1000) -> dict:
    """
    GET /V1/Messages/GetEventsFrom → {Events: [...], LastEventId: str}

    from_date: дата в формате YYYY-MM-DD (API принимает этот формат без проблем
               с кодированием знака '+' в timezone-смещении).
    Используется для первого опроса ящика начиная с даты отправки ORDERS.
    """
    # Используем формат YYYY-MM-DD — он явно поддерживается API и не требует
    # percent-encoding знака '+' (в отличие от формата с временем и timezone).
    date_only = from_date[:10]  # берём только дату из ISO-строки
    params = {"boxId": box_id, "fromDateTime": date_only, "count": count}
    resp = _request("GET", "/V1/Messages/GetEventsFrom", cfg, dl, token, params=params)
    if resp.status_code == 200:
        return resp.json()
    raise RuntimeError(f"GetEventsFrom failed ({resp.status_code}): {resp.text}")


def _decode_message_body(body: dict, context: str) -> str:
    msg_body = body.get("Data", {}).get("MessageBody")
    if msg_body:
        try:
            return base64.b64decode(msg_body).decode("utf-8")
        except Exception as exc:
            raise RuntimeError(f"Ошибка декодирования MessageBody: {exc}") from exc
    raise RuntimeError(f"MessageBody отсутствует в ответе {context}: {body}")


def get_inbox_message_xml(box_id: str, message_id: str,
                          cfg: AppConfig, token: str, dl) -> str:
    """
    GET /V1/Messages/GetInboxMessage → XML-строка тела сообщения.

    API возвращает JSON: {Meta: {...}, Data: {MessageBody: "<base64>", ...}}
    MessageBody — base64-кодированный XML.
    """
    resp = _request("GET", "/V1/Messages/GetInboxMessage", cfg, dl, token,
                    params={"boxId": box_id, "messageId": message_id})

    if resp.status_code == 404:
        raise RuntimeError(f"Сообщение {message_id} не найдено в ящике {box_id}")
    if resp.status_code != 200:
        raise RuntimeError(
            f"GetInboxMessage failed ({resp.status_code}): {resp.text}"
        )

    return _decode_message_body(resp.json(), "GetInboxMessage")


def get_outbox_message_xml(box_id: str, message_id: str,
                           cfg: AppConfig, token: str, dl) -> str:
    """
    GET /V1/Messages/GetOutboxMessage → XML-строка.
    Аналогично GetInboxMessage — JSON с Data.MessageBody в base64.
    """
    resp = _request("GET", "/V1/Messages/GetOutboxMessage", cfg, dl, token,
                    params={"boxId": box_id, "messageId": message_id})

    if resp.status_code == 404:
        raise RuntimeError(f"Исходящее сообщение {message_id} не найдено")
    if resp.status_code != 200:
        raise RuntimeError(
            f"GetOutboxMessage failed ({resp.status_code}): {resp.text}"
        )

    return _decode_message_body(resp.json(), "GetOutboxMessage")


def get_outbox_message_meta(box_id: str, message_id: str,
                            cfg: AppConfig, token: str, dl) -> dict:
    """GET /V1/Messages/GetOutboxMessageMeta → {BoxId, MessageId, DocumentCirculationId}"""
    resp = _request("GET", "/V1/Messages/GetOutboxMessageMeta", cfg, dl, token,
                    params={"boxId": box_id, "messageId": message_id})
    if resp.status_code == 200:
        return resp.json()
    raise RuntimeError(
        f"GetOutboxMessageMeta failed ({resp.status_code}): {resp.text}"
    )
