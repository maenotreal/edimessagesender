#!/usr/bin/env python3
"""
EDI Message Sender — Kontur.EDI API
Главный файл с основным меню и обработчиками режимов.

Запуск:  python main.py
Настройка: python setup.py
"""

import subprocess
import sys
from pathlib import Path


def _ensure_dependencies() -> None:
    """
    Проверить и при необходимости установить зависимости из requirements.txt.
    Запускается один раз при старте до импорта сторонних библиотек.
    """
    req_file = Path(__file__).parent / "requirements.txt"
    if not req_file.exists():
        return

    packages = [
        line.strip()
        for line in req_file.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if not packages:
        return

    # Проверяем наличие каждого пакета без импорта
    import importlib.util
    # Маппинг: имя в requirements → имя модуля для проверки
    _mod_map = {
        "pillow":   "PIL",
        "openpyxl": "openpyxl",
        "requests": "requests",
        "pandas":   "pandas",
    }

    missing = []
    for pkg in packages:
        mod = _mod_map.get(pkg.lower(), pkg.lower())
        if importlib.util.find_spec(mod) is None:
            missing.append(pkg)

    if not missing:
        return

    print(f"  Устанавливаю зависимости: {', '.join(missing)} ...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        print("  Зависимости установлены.\n")
    except subprocess.CalledProcessError as exc:
        print(f"  Ошибка установки: {exc.stderr.decode(errors='ignore')}")
        print("  Установите вручную: pip install -r requirements.txt")
        sys.exit(1)


_ensure_dependencies()

# Проверка обновлений — после установки зависимостей, до тяжёлых импортов
import updater as _updater
_updater.check_and_update(silent=True)

import json
import logging
import os
import webbrowser
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import pandas as pd

from api import (
    get_box_id, send_message,
    get_events, get_events_from,
    get_inbox_message_xml,
)
from auth import get_token
from config import AppConfig, CONFIG_FILE, TEST_PARTY_ID, TEST_SENDER_GLN, validate_gln
from logger import setup_logging
import store
import recadv_builder
import xml_builder

logger = logging.getLogger(__name__)
dl: logging.Logger = None   # detailed logger


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def clr():
    os.system("cls" if os.name == "nt" else "clear")


def monitoring_url(api_base: str, doc_id: str) -> str:
    p = urlparse(api_base)
    return f"{p.scheme}://{p.netloc.replace('-api', '')}/Monitoring/TaskChainList/Document/{doc_id}"


def open_monitoring(resp: dict, cfg: AppConfig) -> None:
    doc_id = resp.get("DocumentCirculationId")
    if doc_id:
        url = monitoring_url(cfg.api_base_url, doc_id)
        logger.info("Мониторинг: %s", url)
        webbrowser.open(url)
    else:
        logger.warning("DocumentCirculationId отсутствует — мониторинг недоступен.")


def pick_file(title: str, filetypes: list) -> str | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk(); root.withdraw(); root.attributes("-topmost", True)
        path = filedialog.askopenfilename(title=title, filetypes=filetypes)
        root.destroy()
        return path or None
    except Exception:
        return None


def pause():
    input("\n  Нажмите Enter для возврата в меню...")


def _extract_order_meta(xml_content: str) -> tuple[str, str]:
    try:
        root = ET.fromstring(xml_content)
        order = root.find("order")
        if order is not None:
            return order.get("number", ""), order.get("date", "")
    except Exception:
        pass
    return "", ""


# ─────────────────────────────────────────────────────────────────────────────
# Проверка статуса ORDERS и поиск входящих DESADV
# ─────────────────────────────────────────────────────────────────────────────

_DELIVERY_EVENT_STATUSES = {
    "MessageDelivered":    store.STATUS_DELIVERED,
    "MessageCheckingOk":   store.STATUS_CHECKING_OK,
    "MessageCheckingFail": store.STATUS_CHECKING_FAIL,
    "MessageUndelivered":  store.STATUS_CHECKING_FAIL,
}
_ORDRSP_STATUS_MAP = {
    "Accepted": store.STATUS_ACCEPTED,
    "Rejected": store.STATUS_REJECTED,
    "Changed":  store.STATUS_CHANGED,
}
_ORDRSP_STATUSES = {store.STATUS_ACCEPTED, store.STATUS_REJECTED, store.STATUS_CHANGED}


def _handle_delivery_event(etype: str, content: dict, order: dict,
                           current_status: str) -> tuple[str, int]:
    """Системное событие доставки. Возвращает (новый_статус, кол-во_изменений)."""
    out_meta     = content.get("OutboxMessageMeta") or {}
    event_msg_id = out_meta.get("MessageId", "")
    orders_msg_id = order.get("message_id", "")
    if not orders_msg_id or event_msg_id != orders_msg_id:
        return current_status, 0

    new_status = _DELIVERY_EVENT_STATUSES.get(etype, current_status)
    if new_status != current_status and current_status not in _ORDRSP_STATUSES:
        store.update_orders_status(order["id"], new_status)
        logger.info("Системный статус ORDERS %s → %s", order["order_number"], new_status)
        return new_status, 1
    return current_status, 0


def _handle_ordrsp(or_elem: ET.Element, order: dict,
                   current_status: str) -> tuple[str, int]:
    """Обработка ORDRSP. Возвращает (новый_статус, кол-во_изменений)."""
    origin = or_elem.find("originOrder")
    if origin is None or (origin.get("number") or "").strip() != order["order_number"]:
        return current_status, 0

    ordrsp_status = (or_elem.get("status") or "").strip()
    new_status = _ORDRSP_STATUS_MAP.get(ordrsp_status, store.STATUS_ACCEPTED)

    if new_status != current_status:
        store.update_orders_status(order["id"], new_status)
        logger.info("ORDRSP для %s: status=%s → %s",
                    order["order_number"], ordrsp_status, new_status)
        return new_status, 1
    return current_status, 0


def _handle_desadv(da: ET.Element, order: dict, xml_str: str, msg_id: str) -> int:
    """Обработка DESADV. Возвращает 1 если сохранён новый DESADV, иначе 0."""
    origin = da.find("originOrder")
    if origin is None or (origin.get("number") or "").strip() != order["order_number"]:
        return 0

    result = store.attach_desadv(
        order_id=order["id"],
        desadv_number=da.get("number", msg_id),
        desadv_date=da.get("date", ""),
        xml_content=xml_str,
    )
    return 1 if result else 0


def _handle_inbox_message(event: dict, order: dict, current_status: str,
                           cfg: AppConfig, token: str) -> tuple[str, int, int]:
    """Обработка NewInboxMessage. Возвращает (новый_статус, new_desadv, status_changes)."""
    content     = event.get("EventContent") or {}
    inbox_meta  = content.get("InboxMessageMeta") or {}
    msg_id      = inbox_meta.get("MessageId", "")
    doc_type    = ((inbox_meta.get("DocumentDetails") or {}).get("DocumentType") or "").upper()

    if doc_type and doc_type not in ("ORDRSP", "DESADV", "UNKNOWN", ""):
        return current_status, 0, 0
    if not msg_id:
        return current_status, 0, 0

    box_id = order.get("box_id", "")
    try:
        xml_str = get_inbox_message_xml(box_id, msg_id, cfg, token, dl)
    except RuntimeError as exc:
        logger.debug("Пропускаем сообщение %s: %s", msg_id, exc)
        return current_status, 0, 0

    try:
        root_elem = ET.fromstring(xml_str)
    except ET.ParseError:
        return current_status, 0, 0

    ih = root_elem.find("interchangeHeader")
    if ih is None:
        return current_status, 0, 0
    actual_type = (ih.findtext("documentType") or "").strip().upper()

    if actual_type == "ORDRSP":
        or_elem = root_elem.find("orderResponse")
        if or_elem is None:
            return current_status, 0, 0
        new_status, changes = _handle_ordrsp(or_elem, order, current_status)
        return new_status, 0, changes

    if actual_type == "DESADV":
        da = root_elem.find("despatchAdvice")
        if da is None:
            return current_status, 0, 0
        return current_status, _handle_desadv(da, order, xml_str, msg_id), 0

    return current_status, 0, 0


def _poll_inbox(order: dict, cfg: AppConfig, token: str) -> tuple[int, int]:
    """
    Опрашивает ящик начиная с даты отправки ORDERS.
    Возвращает (новых_desadv, изменений_статуса).
    """
    box_id  = order.get("box_id", "")
    sent_at = order.get("sent_at", "")

    if not box_id:
        logger.warning("box_id не задан для ORDERS %s", order["order_number"])
        return 0, 0

    from_date = sent_at[:10] if sent_at else "2020-01-01"
    logger.info("Опрашиваем ящик %s с %s...", box_id, from_date)

    all_events: list[dict] = []
    try:
        batch = get_events_from(box_id, cfg, token, dl, from_date=from_date)
        all_events.extend(batch.get("Events") or [])
        last_id = batch.get("LastEventId", "")
        while last_id and len(all_events) < 5000:
            batch  = get_events(box_id, cfg, token, dl, exclusive_event_id=last_id)
            events = batch.get("Events") or []
            all_events.extend(events)
            last_id = batch.get("LastEventId", "")
            if not events:
                break
        if len(all_events) >= 5000:
            logger.warning("Достигнут лимит 5000 событий при опросе ящика")
    except RuntimeError as exc:
        logger.error("Ошибка опроса ящика: %s", exc)
        return 0, 0

    logger.info("Получено событий: %d", len(all_events))

    new_desadv     = 0
    status_changes = 0
    current_status = order.get("orders_status", store.STATUS_PENDING)

    for event in all_events:
        etype   = event.get("EventType", "")
        content = event.get("EventContent") or {}

        if etype in _DELIVERY_EVENT_STATUSES:
            current_status, changes = _handle_delivery_event(
                etype, content, order, current_status)
            status_changes += changes
        elif etype == "NewInboxMessage":
            current_status, nd, sc = _handle_inbox_message(
                event, order, current_status, cfg, token)
            new_desadv     += nd
            status_changes += sc

    return new_desadv, status_changes


# ─────────────────────────────────────────────────────────────────────────────
# Режим 1 – ORDERS из конфига
# ─────────────────────────────────────────────────────────────────────────────

def mode_generate_and_send(cfg: AppConfig, token: str) -> None:
    missing_edi = cfg.validate_edi()
    if missing_edi:
        logger.error("Не заданы поля: %s. Запустите python setup.py.", ", ".join(missing_edi))
        pause(); return

    li = {k: v for k, v in cfg.line_item_defaults.items() if v}
    required = ["gtin", "internal_buyer_code", "description",
                "requested_quantity", "unit_of_measure", "net_price", "vat_rate"]
    missing_li = [f for f in required if not li.get(f)]
    if missing_li:
        logger.error("Не заданы позиции (line_item_defaults): %s", ", ".join(missing_li))
        pause(); return

    try:
        box_id = get_box_id(cfg.party_id, cfg, token, dl)
        xml_content, guid = xml_builder.generate_orders_xml(
            buyer_gln=cfg.buyer_gln, seller_gln=cfg.seller_gln, line_items=[li]
        )
        order_number, order_date = _extract_order_meta(xml_content)
        resp = send_message(box_id, cfg, token, dl, xml_content, f"ORDERS_{guid}.xml")
        doc_circ_id = resp.get("DocumentCirculationId", "")
        message_id  = resp.get("MessageId", "")
        logger.info("Ответ:\n%s", json.dumps(resp, indent=2, ensure_ascii=False))

        store.save_orders(
            order_number=order_number, order_date=order_date,
            buyer_gln=cfg.buyer_gln, seller_gln=cfg.seller_gln,
            box_id=box_id, xml_content=xml_content,
            doc_circ_id=doc_circ_id, message_id=message_id,
        )
        open_monitoring(resp, cfg)
    except RuntimeError as exc:
        logger.error("Ошибка: %s", exc)

    pause()


# ─────────────────────────────────────────────────────────────────────────────
# Режим 2 – отправить существующий XML
# ─────────────────────────────────────────────────────────────────────────────

def mode_send_existing(cfg: AppConfig, token: str) -> None:
    if not cfg.party_id:
        logger.error("party_id не задан. Запустите python setup.py.")
        pause(); return

    try:
        box_id = get_box_id(cfg.party_id, cfg, token, dl)
    except RuntimeError as exc:
        logger.error("Ошибка получения boxId: %s", exc)
        pause(); return

    file_path = pick_file("Выберите XML-файл", [("XML", "*.xml"), ("Все", "*.*")])
    if not file_path:
        file_path = input("  Путь к XML-файлу: ").strip()
    if not file_path:
        pause(); return

    path = Path(file_path)
    if not path.exists():
        logger.error("Файл не найден: %s", file_path)
        pause(); return

    xml_bytes   = path.read_bytes()
    xml_content = xml_bytes.decode("utf-8", errors="replace")

    try:
        resp = send_message(box_id, cfg, token, dl, xml_bytes, path.name)
        doc_circ_id = resp.get("DocumentCirculationId", "")
        message_id  = resp.get("MessageId", "")
        logger.info("Ответ:\n%s", json.dumps(resp, indent=2, ensure_ascii=False))

        if "ORDERS" in path.name.upper() or "<order " in xml_content.lower():
            order_number, order_date = _extract_order_meta(xml_content)
            ans = input("  Сохранить этот ORDERS в хранилище? (y/n): ").strip().lower()
            if ans == "y":
                store.save_orders(
                    order_number=order_number or path.stem, order_date=order_date,
                    buyer_gln=cfg.buyer_gln or "", seller_gln=cfg.seller_gln or "",
                    box_id=box_id, xml_content=xml_content,
                    doc_circ_id=doc_circ_id, message_id=message_id,
                )
        open_monitoring(resp, cfg)
    except RuntimeError as exc:
        logger.error("Ошибка отправки: %s", exc)

    pause()


# ─────────────────────────────────────────────────────────────────────────────
# Режим 3 – тестовый ORDERS
# ─────────────────────────────────────────────────────────────────────────────

def mode_test_orders(cfg: AppConfig, token: str) -> None:
    try:
        box_id = get_box_id(TEST_PARTY_ID, cfg, token, dl)
    except RuntimeError as exc:
        logger.error("Ошибка boxId: %s", exc)
        pause(); return

    recipient_gln = input("  GLN получателя (seller/customer): ").strip()
    shipfrom_gln  = input("  GLN грузоотправителя (shipFrom):  ").strip()
    if not recipient_gln or not shipfrom_gln:
        logger.error("Оба GLN обязательны.")
        pause(); return
    if not validate_gln(recipient_gln):
        logger.error("Некорректный GLN получателя: %s", recipient_gln)
        pause(); return
    if not validate_gln(shipfrom_gln):
        logger.error("Некорректный GLN грузоотправителя: %s", shipfrom_gln)
        pause(); return

    PREDEFINED = {
        "gtin": "000001", "internal_buyer_code": "10001",
        "description": "Тест", "requested_quantity": "100.000",
        "unit_of_measure": "PCE", "net_price": "100.0000", "vat_rate": "22",
    }

    line_items = []
    print("\n  Позиции заказа:")
    print("  1. Предустановленная  2. Ввести вручную")
    while True:
        ch = input("  Выбор (1/2, Enter – завершить): ").strip()
        if not ch:
            break
        if ch == "1":
            line_items.append(PREDEFINED.copy())
            print("  ✓ Добавлена предустановленная позиция.")
        elif ch == "2":
            li = xml_builder.input_full_line_item_manually(len(line_items) + 1)
            if li:
                line_items.append(li)
        else:
            print("  Неверный выбор."); continue
        if input("  Добавить ещё? (y/n): ").strip().lower() != "y":
            break

    if not line_items:
        logger.warning("Нет позиций — отмена.")
        pause(); return

    xml_content, msg_id = xml_builder.generate_orders_xml(
        buyer_gln=TEST_SENDER_GLN, seller_gln=recipient_gln, line_items=line_items
    )
    root = ET.fromstring(xml_content)
    di = root.find(".//deliveryInfo")
    if di is not None:
        sf = di.find("shipFrom/gln")
        if sf is not None: sf.text = shipfrom_gln
        st = di.find("shipTo/gln")
        if st is not None: st.text = TEST_SENDER_GLN
    ET.indent(root, space="  ")
    xml_content = '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(root, encoding="unicode")
    order_number, order_date = _extract_order_meta(xml_content)

    try:
        resp = send_message(box_id, cfg, token, dl, xml_content, f"ORDERS_{msg_id}.xml")
        doc_circ_id = resp.get("DocumentCirculationId", "")
        message_id  = resp.get("MessageId", "")
        logger.info("Ответ:\n%s", json.dumps(resp, indent=2, ensure_ascii=False))
        store.save_orders(
            order_number=order_number, order_date=order_date,
            buyer_gln=TEST_SENDER_GLN, seller_gln=recipient_gln,
            box_id=box_id, xml_content=xml_content,
            doc_circ_id=doc_circ_id, message_id=message_id,
        )
        open_monitoring(resp, cfg)
    except RuntimeError as exc:
        logger.error("Ошибка отправки: %s", exc)

    pause()


# ─────────────────────────────────────────────────────────────────────────────
# Режим 4 – PRICAT из Excel
# ─────────────────────────────────────────────────────────────────────────────

def mode_pricat(cfg: AppConfig, token: str) -> None:
    """Каталоги TradeItemTableLayout — добавление или удаление позиций."""

    print("\n  ══════════════════════════════════════════")
    print("  Каталоги TradeItemTableLayout (PRICAT)")
    print("  ══════════════════════════════════════════")
    print("  1. Добавление позиций  (01.xml → 03.xml, без status)")
    print("  2. Удаление позиций    (03.xml → 01.xml, status=Deleted)")

    op = input("\n  Режим (1/2): ").strip()
    while op not in ("1", "2"):
        op = input("  Введите 1 или 2: ").strip()
    is_delete = (op == "2")

    # ── partyId и boxId ───────────────────────────────────────────────────────
    party_id = input("  partyId: ").strip()
    if not party_id:
        logger.error("partyId обязателен.")
        pause(); return
    try:
        box_id = get_box_id(party_id, cfg, token, dl)
    except RuntimeError as exc:
        logger.error("Ошибка boxId: %s", exc)
        pause(); return

    # ── Загрузка Excel ────────────────────────────────────────────────────────
    excel_path = pick_file("Excel-файл", [("Excel", "*.xlsx *.xls"), ("Все", "*.*")])
    if not excel_path:
        excel_path = input("  Путь к Excel: ").strip()
    if not excel_path or not os.path.exists(excel_path):
        logger.error("Файл не найден.")
        pause(); return

    try:
        # dtype=str — читаем все ячейки как строки, сохраняя ведущие нули
        # и не допуская преобразования числовых значений в float
        df_raw = pd.read_excel(excel_path, header=None, dtype=str)

        # Убираем суффикс ".0", который pandas добавляет к целым числам
        # при чтении с dtype=str (например "123.0" → "123").
        # Но сохраняем реальные дробные значения ("1.5" остаётся "1.5").
        def _clean(val):
            if isinstance(val, str) and val.endswith(".0"):
                candidate = val[:-2]
                if candidate.lstrip("-").isdigit():
                    return candidate
            return val

        df_raw = df_raw.apply(lambda col: col.map(_clean))

        # Определяем наличие строки-заголовка: если первая ячейка не цифровая
        first = (df_raw.iloc[0, 0] or "").strip()
        df    = df_raw.iloc[1:].copy() if not first.isdigit() else df_raw.copy()

        if df.shape[1] < 5:
            logger.error("Excel должен содержать минимум 5 колонок.")
            pause(); return

        df.columns = ["gtin", "internal_buyer_code", "internal_supplier_code",
                      "supplier_name", "vat_rate"]

        # Убираем полностью пустые строки, остальные пустые ячейки → ""
        df = df.replace({"nan": "", "None": "", None: ""})
        df = df[~(df == "").all(axis=1)].fillna("")

    except Exception as exc:
        logger.error("Ошибка чтения Excel: %s", exc)
        pause(); return

    logger.info("Загружено позиций: %d", len(df))

    # ── GLN ───────────────────────────────────────────────────────────────────
    supplier_gln = input("  GLN поставщика: ").strip()
    if not supplier_gln:
        logger.error("GLN поставщика обязателен.")
        pause(); return
    if not validate_gln(supplier_gln):
        logger.error("Некорректный GLN поставщика: %s", supplier_gln)
        pause(); return
    buyer_gln = input("  GLN покупателя (для 03.xml): ").strip() or None
    if buyer_gln and not validate_gln(buyer_gln):
        logger.error("Некорректный GLN покупателя: %s", buyer_gln)
        pause(); return

    # ── Генерация XML ─────────────────────────────────────────────────────────
    line_items   = df.to_dict("records")
    xml_03, f_03 = xml_builder.generate_pricat_xml(3, supplier_gln, buyer_gln,
                                                    line_items, delete=is_delete)
    xml_01, f_01 = xml_builder.generate_pricat_xml(1, supplier_gln, None,
                                                    line_items, delete=is_delete)

    mode_label = "Удаление" if is_delete else "Добавление"
    print(f"\n  Режим: {mode_label}  |  Позиций: {len(line_items)}")
    print(f"\n  Превью {f_01}:\n{xml_01[:400]}...\n")
    print(f"  Превью {f_03}:\n{xml_03[:400]}...\n")

    if input("  Отправить? (y/n): ").strip().lower() != "y":
        pause(); return

    # ── Отправка: порядок зависит от режима ───────────────────────────────────
    # Добавление: 01 → 03  |  Удаление: 03 → 01
    send_order = [(xml_01, f_01), (xml_03, f_03)] if not is_delete             else [(xml_03, f_03), (xml_01, f_01)]

    for xml_data, fname in send_order:
        try:
            logger.info("Отправляем %s...", fname)
            resp = send_message(box_id, cfg, token, dl, xml_data, fname)
            logger.info("Ответ %s:\n%s", fname, json.dumps(resp, indent=2, ensure_ascii=False))
            open_monitoring(resp, cfg)
        except RuntimeError as exc:
            logger.error("Ошибка %s: %s", fname, exc)

    pause()

# ─────────────────────────────────────────────────────────────────────────────
# Режим 5 – RECADV
# ─────────────────────────────────────────────────────────────────────────────

def mode_recadv(cfg: AppConfig, token: str) -> None:
    print("\n  ══════════════════════════════════════════")
    print("  RECADV — Уведомление о приёмке товара")
    print("  ══════════════════════════════════════════")

    orders = store.get_all_orders()
    if not orders:
        print("\n  Хранилище пусто. Сначала отправьте ORDERS (режим 1, 2 или 3).")
        pause(); return

    store.print_orders_table(orders)

    while True:
        raw = input(f"\n  Выберите ORDERS (1–{len(orders)}) или Enter для отмены: ").strip()
        if not raw:
            return
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(orders):
                break
        except ValueError:
            pass
        print("  Неверный выбор.")

    order = orders[idx]
    print(f"\n  Выбран: {order['order_number']} [{store._STATUS_LABEL.get(order.get('orders_status',''), '—')}]")

    # ── Опрос ящика ──────────────────────────────────────────────────────────
    ans = input("  Обновить статус и загрузить новые DESADV из ящика? (y/n) [y]: ").strip().lower()
    if ans != "n":
        new_d, new_s = _poll_inbox(order, cfg, token)
        if new_s:
            print(f"  Статус ORDERS обновлён.")
        if new_d:
            print(f"  Найдено новых DESADV: {new_d}")
        else:
            print("  Новых DESADV не найдено.")
        # Перечитываем из хранилища после обновления
        order = store.get_order_by_id(order["id"])

    # ── Проверка статуса ORDERS ───────────────────────────────────────────────
    status       = order.get("orders_status", store.STATUS_PENDING)
    status_label = store._STATUS_LABEL.get(status, status)

    if status == store.STATUS_ACCEPTED:
        print("\n  ✅ ORDERS подтверждён поставщиком (ORDRSP Accepted).")
    elif status == store.STATUS_CHANGED:
        print("\n  🔄 Поставщик изменил условия заказа (ORDRSP Changed).")
        print("     Проверьте позиции DESADV перед формированием RECADV.")
        ans = input("  Продолжить? (y/n): ").strip().lower()
        if ans != "y":
            return
    elif status == store.STATUS_REJECTED:
        print("\n  ❌ ORDERS отклонён поставщиком (ORDRSP Rejected).")
        print("     Отправка RECADV нецелесообразна.")
        ans = input("  Продолжить всё равно? (y/n): ").strip().lower()
        if ans != "y":
            return
    elif status in (store.STATUS_PENDING, store.STATUS_DELIVERED,
                    store.STATUS_CHECKING_OK):
        print(f"\n  ⚠  ORDRSP от поставщика ещё не получен (статус: {status_label}).")
        print("     Поставщик мог ещё не обработать заказ.")
        ans = input("  Продолжить всё равно? (y/n): ").strip().lower()
        if ans != "y":
            return
    elif status == store.STATUS_CHECKING_FAIL:
        print(f"\n  ❌ ORDERS не прошёл системную проверку ({status_label}).")
        ans = input("  Продолжить всё равно? (y/n): ").strip().lower()
        if ans != "y":
            return
    desadvs = order.get("desadv", [])
    if not desadvs:
        print("\n  Нет DESADV для этого ORDERS.")
        print("  Убедитесь, что поставщик уже отправил DESADV на ваш ящик.")
        pause(); return

    # ── Выбор DESADV ─────────────────────────────────────────────────────────
    store.print_desadv_table(order)

    while True:
        raw = input(f"\n  Выберите DESADV (1–{len(desadvs)}) или Enter для отмены: ").strip()
        if not raw:
            return
        try:
            didx = int(raw) - 1
            if 0 <= didx < len(desadvs):
                break
        except ValueError:
            pass
        print("  Неверный выбор.")

    desadv_rec = desadvs[didx]
    if desadv_rec.get("recadv_sent"):
        ans = input("  RECADV уже отправлялся. Отправить повторно? (y/n): ").strip().lower()
        if ans != "y":
            return

    # ── Парсинг DESADV ───────────────────────────────────────────────────────
    xml_str = store.read_xml(desadv_rec["xml_file"])
    if not xml_str:
        logger.error("XML DESADV не найден.")
        pause(); return

    try:
        desadv_data = recadv_builder.DesadvData(xml_str)
    except ValueError as exc:
        logger.error("Ошибка разбора DESADV: %s", exc)
        pause(); return

    print(f"\n  DESADV № {desadv_data.desadv_number} от {desadv_data.desadv_date}")
    print(f"  Поставщик: {desadv_data.seller_gln}  →  Покупатель: {desadv_data.buyer_gln}")

    if not desadv_data.line_items:
        print("  В DESADV нет позиций товаров.")
        pause(); return

    accepted = recadv_builder.collect_accepted_quantities(desadv_data)

    # ── Получаем boxId для отправки RECADV ───────────────────────────────────
    box_id = order.get("box_id", "")
    if not box_id:
        box_id = input("  boxId для отправки RECADV: ").strip()
    if not box_id:
        logger.error("box_id не определён.")
        pause(); return

    # ── Генерация XML ─────────────────────────────────────────────────────────
    try:
        recadv_xml, recadv_number = recadv_builder.build_recadv_xml(
            desadv=desadv_data, line_items=accepted
        )
    except Exception as exc:
        logger.error("Ошибка генерации RECADV: %s", exc)
        pause(); return

    print(f"\n  Сгенерирован RECADV № {recadv_number}")
    print(f"  Превью:\n{recadv_xml[:600]}...\n")

    if input("  Отправить RECADV? (y/n): ").strip().lower() != "y":
        return

    # ── Отправка ─────────────────────────────────────────────────────────────
    try:
        resp = send_message(box_id, cfg, token, dl, recadv_xml, f"RECADV_{recadv_number}.xml")
        logger.info("RECADV отправлен:\n%s", json.dumps(resp, indent=2, ensure_ascii=False))
        store.mark_recadv_sent(order["id"], desadv_rec["id"])
        open_monitoring(resp, cfg)
    except RuntimeError as exc:
        logger.error("Ошибка отправки RECADV: %s", exc)

    pause()


# ─────────────────────────────────────────────────────────────────────────────
# Режим 6 – Хранилище
# ─────────────────────────────────────────────────────────────────────────────

def mode_storage(cfg: AppConfig, token: str) -> None:
    while True:
        clr()
        print("\n  ══════════════════════════════════════════")
        print("  Хранилище документов")
        print("  ══════════════════════════════════════════")
        orders = store.get_all_orders()
        store.print_orders_table(orders)
        print("\n  u. Обновить статус / загрузить DESADV")
        print("  v. Просмотреть DESADV выбранного ORDERS")
        print("  d. Удалить выбранный ORDERS")
        print("  c. Очистить завершённые ORDERS (RECADV отправлен)")
        print("  p. Очистить устаревшие ORDERS (указать количество дней)")
        print("  q. Вернуться в главное меню")

        choice = input("\n  Выбор: ").strip().lower()

        if choice == "q":
            break

        # ── Действия требующие выбора ORDERS ─────────────────────────────
        if choice in ("u", "v", "d"):
            if not orders:
                print("  Хранилище пусто."); input("  Enter..."); continue
            raw = input(f"  Номер ORDERS (1–{len(orders)}): ").strip()
            try:
                idx = int(raw) - 1
                if not (0 <= idx < len(orders)):
                    raise ValueError
            except ValueError:
                print("  Неверный выбор."); input("  Enter..."); continue

            order = orders[idx]

            if choice == "u":
                new_d, new_s = _poll_inbox(order, cfg, token)
                status_label = store._STATUS_LABEL.get(
                    store.get_order_by_id(order["id"]).get("orders_status", ""), "—"
                )
                print(f"  Статус ORDERS: {status_label}")
                print(f"  Новых DESADV: {new_d}")
                input("  Enter...")

            elif choice == "v":
                order = store.get_order_by_id(order["id"])
                store.print_desadv_table(order)
                input("\n  Enter...")

            elif choice == "d":
                print(f"  ORDERS: {order['order_number']} от {order['order_date']}")
                desadv_count = len(order.get("desadv", []))
                if desadv_count:
                    print(f"  Будет также удалено DESADV: {desadv_count}")
                ans = input("  Удалить? (yes/no): ").strip().lower()
                if ans == "yes":
                    store.delete_order(order["id"])
                    print("  Удалено.")
                else:
                    print("  Отменено.")
                input("  Enter...")

        # ── Массовая очистка ──────────────────────────────────────────────
        elif choice == "c":
            n = store.purge_completed_orders()
            if n:
                print(f"  Удалено завершённых ORDERS: {n}")
            else:
                print("  Завершённых ORDERS не найдено.")
                print("  (Завершённым считается ORDERS со статусом Accepted/Rejected,")
                print("   у которого все DESADV имеют отправленный RECADV.)")
            input("  Enter...")

        elif choice == "p":
            raw = input("  Удалить ORDERS старше скольки дней? (Enter — отмена): ").strip()
            if not raw:
                continue
            try:
                days = int(raw)
                if days <= 0:
                    raise ValueError
            except ValueError:
                print("  Введите целое положительное число."); input("  Enter..."); continue
            # Предварительный подсчёт
            from datetime import datetime, timedelta
            cutoff = datetime.now() - timedelta(days=days)
            preview = [o for o in store.get_all_orders()
                       if o.get("sent_at", "") and
                       datetime.fromisoformat(o["sent_at"][:19]) < cutoff]
            if not preview:
                print(f"  ORDERS старше {days} дней не найдено."); input("  Enter..."); continue
            print(f"  Будет удалено: {len(preview)} ORDERS")
            for o in preview:
                print(f"    - {o['order_number']} от {o['order_date']} (отправлен {o['sent_at'][:10]})")
            ans = input("  Подтвердить удаление? (yes/no): ").strip().lower()
            if ans == "yes":
                n = store.purge_old_orders(days)
                print(f"  Удалено: {n}")
            else:
                print("  Отменено.")
            input("  Enter...")

        else:
            print("  Неверный выбор."); input("  Enter...")


# ─────────────────────────────────────────────────────────────────────────────
# Главное меню
# ─────────────────────────────────────────────────────────────────────────────

MENU = """
╔══════════════════════════════════════════════════╗
║           EDI Message Sender                     ║
╠══════════════════════════════════════════════════╣
║  1. Сгенерировать ORDERS из конфига и отправить  ║
║  2. Отправить существующий XML-файл              ║
║  3. Тестовый ORDERS (произвольные GLN)           ║
║  4. Работа с каталогами в TradeItemTableLayout   ║
║  ────────────────────────────────────────────────║
║  5. RECADV — отправить уведомление о приёмке     ║
║  6. Хранилище — просмотр ORDERS / DESADV         ║
║  ────────────────────────────────────────────────║
║  s. Настройка (setup)                            ║
║  u. Проверить обновления                         ║
║  q. Выход                                        ║
╚══════════════════════════════════════════════════╝"""

HANDLERS = {
    "1": mode_generate_and_send,
    "2": mode_send_existing,
    "3": mode_test_orders,
    "4": mode_pricat,
    "5": mode_recadv,
    "6": mode_storage,
}

# ── Пасхалка ─────────────────────────────────────────────────────────────────

def _generate_art(img_path: str, target_cols: int = 55,
                  threshold: int = 140, aspect: float = 0.58) -> str:
    """Конвертировать изображение в Braille Unicode-арт."""
    from PIL import Image
    DOT_MAP = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]
    DOT_POS = [(0,0),(1,0),(2,0),(3,0),(0,1),(1,1),(2,1),(3,1)]
    img = Image.open(img_path).convert('L')
    tw  = (target_cols * 2 // 2) * 2
    th  = (int(img.size[1] * tw / img.size[0] * aspect) // 4) * 4
    img = img.resize((tw, th), Image.LANCZOS)
    px  = list(img.getdata())
    rows = []
    for br in range(0, th, 4):
        row = []
        for bc in range(0, tw, 2):
            bits = 0
            for di, (dr, dc) in enumerate(DOT_POS):
                py, bx = br + dr, bc + dc
                if py < th and bx < tw and px[py * tw + bx] < threshold:
                    bits |= DOT_MAP[di]
            row.append(chr(0x2800 + bits))
        rows.append(''.join(row))
    return '\n'.join(rows)


def _easter_enabled() -> bool:
    return store._load().get("_easter", False)


def _easter_activate() -> None:
    """Активировать пасхалку: сгенерировать арт из изображения и сохранить."""
    data = store._load()
    if data.get("_easter"):
        return
    img_path = Path(__file__).parent / "joker.png"
    if img_path.exists():
        try:
            data["_easter"]     = True
            data["_easter_art"] = _generate_art(str(img_path))
            store._save(data)
            return
        except Exception:
            pass
    data["_easter"] = True
    store._save(data)


def _print_easter() -> None:
    art = store._load().get("_easter_art", "")
    if art:
        print(art)
        print()



def main() -> None:
    global dl
    dl = setup_logging()

    if not CONFIG_FILE.exists():
        print(f"\n  Конфиг не найден ({CONFIG_FILE.name}).")
        ans = input("  Запустить настройку сейчас? (y/n): ").strip().lower()
        if ans == "y":
            import setup as _setup; _setup.main()
        else:
            sys.exit(0)

    cfg = AppConfig()
    missing_auth = cfg.validate_auth()
    if missing_auth:
        logger.error("Не заполнены поля авторизации: %s\n  python setup.py",
                     ", ".join(missing_auth))
        sys.exit(1)

    logger.info("Режим авторизации: %s", cfg.auth_mode.upper())
    try:
        token = get_token(cfg, dl)
    except RuntimeError as exc:
        logger.error("Авторизация не удалась: %s", exc)
        sys.exit(1)

    while True:
        clr()
        if _easter_enabled():
            _print_easter()
        print(MENU)
        orders_count = len(store.get_all_orders())
        print(f"  Конфиг: {CONFIG_FILE}  |  Режим: {cfg.auth_mode.upper()}")
        print(f"  API:    {cfg.api_base_url}  |  Хранилище: {orders_count} ORDERS\n")

        choice = input("  Выбор: ").strip().lower()

        if choice == "q":
            print("\n  До свидания!\n"); break

        if choice == "s":
            import setup as _setup; _setup.main()
            cfg = AppConfig()
            missing_auth = cfg.validate_auth()
            if missing_auth:
                logger.error("Не заполнены поля авторизации: %s", ", ".join(missing_auth))
                continue
            try:
                token = get_token(cfg, dl)
            except RuntimeError as exc:
                logger.error("Авторизация не удалась: %s", exc)
            continue

        if choice == "u":
            _updater.check_and_update(silent=False)
            input("  Enter...")
            continue

        if choice == "увы":
            _easter_activate()
        elif choice in HANDLERS:
            clr()
            try:
                HANDLERS[choice](cfg, token)
            except Exception as exc:
                logger.error("Неожиданная ошибка: %s", exc, exc_info=True)
                pause()
        else:
            print("  Неверный выбор."); input("  Enter...")


if __name__ == "__main__":
    main()
