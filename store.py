"""
store.py – локальное хранилище документов EDI.

Хранит метаданные в edi_store.json, XML-тела в edi_documents/.

Структура записи ORDERS:
  id, order_number, order_date, buyer_gln, seller_gln,
  sent_at (ISO), box_id, doc_circ_id, message_id,
  xml_file, orders_status,   # "pending" | "delivered" | "checking_ok" | "checking_fail"
  desadv: [
    { id, desadv_number, desadv_date, received_at, xml_file, recadv_sent }
  ]
"""

import json
import logging
import sys
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_store_lock = threading.Lock()

_BASE_DIR  = (Path(sys.executable).parent
              if getattr(sys, "frozen", False)
              else Path(__file__).parent)
STORE_FILE = _BASE_DIR / "edi_store.json"
DOCS_DIR   = _BASE_DIR / "edi_documents"

# Статусы ORDERS
STATUS_PENDING       = "pending"         # отправлен, ответа ещё нет
STATUS_DELIVERED     = "delivered"       # MessageDelivered — системная доставка
STATUS_CHECKING_OK   = "checking_ok"     # MessageCheckingOk — системная проверка ОК
STATUS_CHECKING_FAIL = "checking_fail"   # MessageCheckingFail — системная ошибка
STATUS_ACCEPTED      = "ordrsp_accepted" # ORDRSP status=Accepted
STATUS_REJECTED      = "ordrsp_rejected" # ORDRSP status=Rejected
STATUS_CHANGED       = "ordrsp_changed"  # ORDRSP status=Changed


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def _load() -> dict:
    if not STORE_FILE.exists():
        return {"orders": []}
    try:
        return json.loads(STORE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("Хранилище повреждено, данные недоступны: %s", exc)
        return {"orders": []}


def _save(data: dict) -> None:
    DOCS_DIR.mkdir(exist_ok=True)
    try:
        STORE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception as exc:
        logger.error("Не удалось сохранить хранилище: %s", exc)


def _xml_path(doc_type: str, doc_id: str) -> Path:
    DOCS_DIR.mkdir(exist_ok=True)
    return DOCS_DIR / f"{doc_type}_{doc_id}.xml"


# ─────────────────────────────────────────────────────────────────────────────
# ORDERS
# ─────────────────────────────────────────────────────────────────────────────

def save_orders(
    order_number: str,
    order_date: str,
    buyer_gln: str,
    seller_gln: str,
    box_id: str,
    xml_content: str,
    doc_circ_id: str = "",
    message_id: str = "",
) -> str:
    """Сохранить отправленный ORDERS. Возвращает внутренний id."""
    rec_id   = str(uuid.uuid4())
    xml_file = _xml_path("ORDERS", rec_id)
    xml_file.write_text(xml_content, encoding="utf-8")

    record = {
        "id":             rec_id,
        "order_number":   order_number,
        "order_date":     order_date,
        "buyer_gln":      buyer_gln,
        "seller_gln":     seller_gln,
        "sent_at":        datetime.now().isoformat(timespec="seconds"),
        "box_id":         box_id,
        "doc_circ_id":    doc_circ_id,
        "message_id":     message_id,   # MessageId из ответа SendMessage
        "xml_file":       str(xml_file.relative_to(_BASE_DIR)),
        "orders_status":  STATUS_PENDING,
        "desadv":         [],
    }
    with _store_lock:
        data = _load()
        data["orders"].append(record)
        _save(data)
    logger.info("ORDERS сохранён: %s / %s", order_number, rec_id)
    return rec_id


def get_all_orders() -> list[dict]:
    return _load().get("orders", [])


def get_order_by_id(order_id: str) -> dict | None:
    for o in get_all_orders():
        if o["id"] == order_id:
            return o
    return None


def update_orders_status(order_id: str, status: str) -> None:
    with _store_lock:
        data = _load()
        for o in data["orders"]:
            if o["id"] == order_id:
                o["orders_status"] = status
                break
        _save(data)


def update_order_fields(order_id: str, **fields) -> None:
    """Обновить произвольные поля записи ORDERS."""
    with _store_lock:
        data = _load()
        for o in data["orders"]:
            if o["id"] == order_id:
                o.update(fields)
                break
        _save(data)


# ─────────────────────────────────────────────────────────────────────────────
# DESADV
# ─────────────────────────────────────────────────────────────────────────────

def attach_desadv(
    order_id: str,
    desadv_number: str,
    desadv_date: str,
    xml_content: str,
) -> str | None:
    """Прикрепить DESADV к ORDERS. Возвращает id DESADV или None."""
    with _store_lock:
        data = _load()
        for o in data["orders"]:
            if o["id"] != order_id:
                continue
            # Дедупликация по номеру
            for d in o.get("desadv", []):
                if d["desadv_number"] == desadv_number:
                    logger.info("DESADV %s уже сохранён.", desadv_number)
                    return d["id"]

            desadv_id = str(uuid.uuid4())
            xml_file  = _xml_path("DESADV", desadv_id)
            xml_file.write_text(xml_content, encoding="utf-8")

            o.setdefault("desadv", []).append({
                "id":            desadv_id,
                "desadv_number": desadv_number,
                "desadv_date":   desadv_date,
                "received_at":   datetime.now().isoformat(timespec="seconds"),
                "xml_file":      str(xml_file.relative_to(_BASE_DIR)),
                "recadv_sent":   False,
            })
            _save(data)
            logger.info("DESADV %s прикреплён к ORDERS %s", desadv_number, order_id)
            return desadv_id

    logger.warning("ORDERS %s не найден", order_id)
    return None


def mark_recadv_sent(order_id: str, desadv_id: str) -> None:
    with _store_lock:
        data = _load()
        for o in data["orders"]:
            if o["id"] != order_id:
                continue
            for d in o.get("desadv", []):
                if d["id"] == desadv_id:
                    d["recadv_sent"] = True
                    break
            break
        _save(data)


def read_xml(relative_path: str) -> str | None:
    p = _BASE_DIR / relative_path
    if not p.exists():
        logger.error("XML-файл не найден: %s", p)
        return None
    return p.read_text(encoding="utf-8")


def _delete_xml_file(relative_path: str) -> None:
    """Удалить XML-файл документа (молча игнорирует отсутствие файла)."""
    try:
        (_BASE_DIR / relative_path).unlink(missing_ok=True)
    except Exception as exc:
        logger.warning("Не удалось удалить файл %s: %s", relative_path, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Удаление записей
# ─────────────────────────────────────────────────────────────────────────────

def delete_order(order_id: str) -> bool:
    """
    Удалить ORDERS и все его DESADV из хранилища вместе с XML-файлами.
    Возвращает True если запись найдена и удалена.
    """
    with _store_lock:
        data = _load()
        for i, o in enumerate(data["orders"]):
            if o["id"] != order_id:
                continue
            if o.get("xml_file"):
                _delete_xml_file(o["xml_file"])
            for d in o.get("desadv", []):
                if d.get("xml_file"):
                    _delete_xml_file(d["xml_file"])
            data["orders"].pop(i)
            _save(data)
            logger.info("ORDERS %s удалён из хранилища.", o.get("order_number", order_id))
            return True
    return False


def purge_old_orders(days: int) -> int:
    """
    Удалить все ORDERS старше указанного количества дней.
    Критерий: поле sent_at.
    Возвращает количество удалённых записей.
    """
    cutoff = datetime.now() - timedelta(days=days)
    with _store_lock:
        data      = _load()
        to_delete = []

        for o in data["orders"]:
            try:
                sent = datetime.fromisoformat(o.get("sent_at", ""))
                if sent < cutoff:
                    to_delete.append(o)
            except ValueError:
                pass

        for o in to_delete:
            if o.get("xml_file"):
                _delete_xml_file(o["xml_file"])
            for d in o.get("desadv", []):
                if d.get("xml_file"):
                    _delete_xml_file(d["xml_file"])

        ids = {o["id"] for o in to_delete}
        data["orders"] = [o for o in data["orders"] if o["id"] not in ids]
        _save(data)

    logger.info("Удалено устаревших ORDERS: %d (старше %d дней).", len(to_delete), days)
    return len(to_delete)


def purge_completed_orders() -> int:
    """
    Удалить ORDERS у которых все DESADV уже имеют отправленный RECADV
    И статус ORDRSP финальный (Accepted / Rejected).
    Возвращает количество удалённых записей.
    """
    final_statuses = {STATUS_ACCEPTED, STATUS_REJECTED}
    with _store_lock:
        data      = _load()
        to_delete = []

        for o in data["orders"]:
            if o.get("orders_status") not in final_statuses:
                continue
            desadvs = o.get("desadv", [])
            # Считаем завершённым если: нет DESADV (заказ отклонён/не отгружался)
            # ИЛИ все DESADV имеют recadv_sent=True
            all_recadv_sent = all(d.get("recadv_sent") for d in desadvs) if desadvs else True
            if all_recadv_sent:
                to_delete.append(o)

        for o in to_delete:
            if o.get("xml_file"):
                _delete_xml_file(o["xml_file"])
            for d in o.get("desadv", []):
                if d.get("xml_file"):
                    _delete_xml_file(d["xml_file"])

        ids = {o["id"] for o in to_delete}
        data["orders"] = [o for o in data["orders"] if o["id"] not in ids]
        _save(data)

    logger.info("Удалено завершённых ORDERS: %d.", len(to_delete))
    return len(to_delete)


# ─────────────────────────────────────────────────────────────────────────────
# Отображение
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_LABEL = {
    STATUS_PENDING:       "⏳ Ожидание",
    STATUS_DELIVERED:     "📬 Доставлен",
    STATUS_CHECKING_OK:   "📬 Проверен",
    STATUS_CHECKING_FAIL: "❌ Сист.ошибка",
    STATUS_ACCEPTED:      "✅ Принят (ORDRSP)",
    STATUS_REJECTED:      "❌ Отклонён (ORDRSP)",
    STATUS_CHANGED:       "🔄 Изменён (ORDRSP)",
}


def _strikethrough(text: str) -> str:
    """Обернуть каждый символ строки в Unicode-зачёркивание (U+0336)."""
    return "".join(c + "\u0336" for c in text)


def print_orders_table(orders: list[dict]) -> None:
    if not orders:
        print("  Хранилище пусто.")
        return
    print(f"\n  {'#':<4} {'Номер заказа':<26} {'Дата':<12} "
          f"{'Статус ORDERS':<16} {'DESADV':<9} {'Отправлен'}")
    print("  " + "─" * 95)
    for i, o in enumerate(orders, 1):
        desadvs      = o.get("desadv", [])
        desadv_count = len(desadvs)
        unsent       = sum(1 for d in desadvs if not d.get("recadv_sent"))
        # ORDERS считается полностью завершённым если есть хотя бы один DESADV
        # и все они имеют recadv_sent=True
        all_done     = desadv_count > 0 and unsent == 0
        desadv_mark  = f"{desadv_count}" + (f" (!{unsent})" if unsent else "")
        status_label = _STATUS_LABEL.get(o.get("orders_status", ""), "—")
        number  = o['order_number']
        date    = o['order_date']
        sent_at = o['sent_at'][:16]
        if all_done:
            number       = _strikethrough(number)
            date         = _strikethrough(date)
            status_label = _strikethrough(status_label)
            desadv_mark  = _strikethrough(desadv_mark)
            sent_at      = _strikethrough(sent_at)
        print(f"  {i:<4} {number:<26} {date:<12} "
              f"{status_label:<16} {desadv_mark:<9} {sent_at}")


def print_desadv_table(order: dict) -> None:
    desadvs = order.get("desadv", [])
    if not desadvs:
        print("  Нет DESADV для этого заказа.")
        return
    print(f"\n  {'#':<4} {'Номер DESADV':<24} {'Дата':<12} "
          f"{'Получен':<20} {'RECADV'}")
    print("  " + "─" * 75)
    for i, d in enumerate(desadvs, 1):
        sent = "✓ Отправлен" if d.get("recadv_sent") else "— Нет"
        print(f"  {i:<4} {d['desadv_number']:<24} {d['desadv_date']:<12} "
              f"{d['received_at'][:16]:<20} {sent}")
