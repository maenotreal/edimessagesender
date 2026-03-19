"""
recadv_builder.py – построитель XML RECADV на основе DESADV.

Логика:
  1. Парсим DESADV XML → извлекаем заголовок, стороны, позиции.
  2. Для каждой позиции пользователь может скорректировать
     принятое количество (acceptedQuantity).
  3. Формируем минимально валидный RECADV согласно спецификации.

Ключевые поля RECADV (из спецификации):
  receivingAdvice @number @date @status
    originOrder @number @date        ← из DESADV/originOrder
    despatchIdentificator @number @date ← из DESADV/despatchAdvice @number @date
    seller/gln                       ← из DESADV/seller/gln
    buyer/gln                        ← из DESADV/buyer/gln
    deliveryInfo/shipFrom/gln        ← из DESADV
    deliveryInfo/shipTo/gln          ← из DESADV
    lineItems/
      lineItem/
        gtin
        internalBuyerCode
        description
        despatchedQuantity           ← из DESADV
        acceptedQuantity             ← задаёт пользователь (по умолчанию = despatched)
"""

import logging
import random
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Парсинг DESADV
# ─────────────────────────────────────────────────────────────────────────────

def _txt(elem, path: str, default: str = "") -> str:
    """Безопасный xpath-геттер текста."""
    node = elem.find(path)
    return (node.text or "").strip() if node is not None else default


def _attr(elem, path: str, attr: str, default: str = "") -> str:
    node = elem.find(path)
    return (node.get(attr) or "").strip() if node is not None else default


class DesadvData:
    """Извлечённые из DESADV данные, необходимые для RECADV."""

    def __init__(self, xml_str: str):
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError as exc:
            raise ValueError(f"Невалидный XML DESADV: {exc}") from exc

        da = root.find("despatchAdvice")
        if da is None:
            raise ValueError("Элемент <despatchAdvice> не найден в DESADV")

        # Номер/дата DESADV
        self.desadv_number = da.get("number", "")
        self.desadv_date   = da.get("date", "")

        # Исходный заказ
        oo = da.find("originOrder")
        self.order_number = oo.get("number", "") if oo is not None else ""
        self.order_date   = oo.get("date",   "") if oo is not None else ""

        # GLN сторон
        self.seller_gln = _txt(da, "seller/gln")
        self.buyer_gln  = _txt(da, "buyer/gln")
        self.ship_from  = _txt(da, "deliveryInfo/shipFrom/gln")
        self.ship_to    = _txt(da, "deliveryInfo/shipTo/gln")

        # sender/recipient из interchangeHeader
        ih = root.find("interchangeHeader")
        self.sender    = _txt(ih, "sender")    if ih is not None else ""
        self.recipient = _txt(ih, "recipient") if ih is not None else ""

        # Позиции
        self.line_items: list[dict] = []
        for li in da.findall(".//lineItems/lineItem"):
            dq_elem = li.find("despatchedQuantity")
            dq_uom  = dq_elem.get("unitOfMeasure", "PCE") if dq_elem is not None else "PCE"
            dq_val  = (dq_elem.text or "0").strip()        if dq_elem is not None else "0"

            self.line_items.append({
                "gtin":                _txt(li, "gtin"),
                "internal_buyer_code": _txt(li, "internalBuyerCode"),
                "description":         _txt(li, "description"),
                "despatched_qty":      dq_val,
                "despatched_uom":      dq_uom,
                "net_price":           _txt(li, "netPrice"),
                "vat_rate":            _txt(li, "vATRate"),
            })


# ─────────────────────────────────────────────────────────────────────────────
# Интерактивный ввод корректировок
# ─────────────────────────────────────────────────────────────────────────────

def collect_accepted_quantities(desadv: DesadvData) -> list[dict]:
    """
    Показывает позиции из DESADV и предлагает пользователю указать
    принятое количество для каждой.
    Возвращает список dict с ключами despatched_qty / accepted_qty / uom.
    """
    print("\n  ┌─── Позиции DESADV ─────────────────────────────────────────────────┐")
    print(f"  │  {'#':<4} {'GTIN':<16} {'Описание':<30} {'Отгружено':<12} {'UOM'}")
    print("  │  " + "─" * 70)

    for i, li in enumerate(desadv.line_items, 1):
        desc = li["description"][:28] if li["description"] else "(без названия)"
        print(f"  │  {i:<4} {li['gtin']:<16} {desc:<30} {li['despatched_qty']:<12} {li['despatched_uom']}")
    print("  └────────────────────────────────────────────────────────────────────┘")

    print("\n  Укажите принятое количество для каждой позиции.")
    print("  (Enter = принять отгруженное количество полностью)\n")

    result = []
    for i, li in enumerate(desadv.line_items, 1):
        while True:
            raw = input(f"  [{i}] {li['description'] or li['gtin']} "
                        f"(отгружено {li['despatched_qty']} {li['despatched_uom']}): ").strip()
            if not raw:
                accepted = li["despatched_qty"]
                break
            try:
                accepted = f"{float(raw):g}"
                break
            except ValueError:
                print("    Введите число или нажмите Enter.")

        result.append({
            "gtin":                li["gtin"],
            "internal_buyer_code": li["internal_buyer_code"],
            "description":         li["description"],
            "despatched_qty":      li["despatched_qty"],
            "accepted_qty":        accepted,
            "uom":                 li["despatched_uom"],
            "net_price":           li["net_price"],
            "vat_rate":            li["vat_rate"],
        })

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Автоматическое принятие без расхождений (для режима прослушивания)
# ─────────────────────────────────────────────────────────────────────────────

def collect_accepted_quantities_auto(desadv: DesadvData) -> list[dict]:
    """
    Автоматически принять все позиции без расхождений
    (acceptedQuantity == despatchedQuantity для каждой позиции).
    Используется в режиме автоматического прослушивания.
    """
    return [
        {
            "gtin":                li["gtin"],
            "internal_buyer_code": li["internal_buyer_code"],
            "description":         li["description"],
            "despatched_qty":      li["despatched_qty"],
            "accepted_qty":        li["despatched_qty"],   # нет расхождений
            "uom":                 li["despatched_uom"],
            "net_price":           li["net_price"],
            "vat_rate":            li["vat_rate"],
        }
        for li in desadv.line_items
    ]


def build_recadv_from_desadv_xml(xml_str: str) -> tuple[str, str]:
    """
    Удобная обёртка: разобрать DESADV XML и сформировать RECADV без расхождений.
    Возвращает (recadv_xml_string, recadv_number).
    """
    desadv_data = DesadvData(xml_str)
    line_items  = collect_accepted_quantities_auto(desadv_data)
    return build_recadv_xml(desadv=desadv_data, line_items=line_items)


# ─────────────────────────────────────────────────────────────────────────────
# Построитель XML RECADV
# ─────────────────────────────────────────────────────────────────────────────

def build_recadv_xml(
    desadv: DesadvData,
    line_items: list[dict],
    recadv_number: str | None = None,
) -> tuple[str, str]:
    """
    Построить XML RECADV.
    Возвращает (xml_string, recadv_number).
    """
    now_utc  = datetime.now(timezone.utc)
    dt_str   = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    today    = now_utc.strftime("%Y-%m-%d")
    msg_id   = str(uuid.uuid4())

    if not recadv_number:
        suffix       = random.randint(1000, 9999)
        recadv_number = f"RECV-{now_utc.strftime('%Y%m%d-%H%M%S')}-{suffix}"

    # ── Корень ────────────────────────────────────────────────────────────────
    root = ET.Element("eDIMessage", id=msg_id, creationDateTime=dt_str)

    # ── interchangeHeader ─────────────────────────────────────────────────────
    # RECADV: отправитель = buyer (получатель DESADV), получатель = seller
    ih = ET.SubElement(root, "interchangeHeader")
    ET.SubElement(ih, "sender").text    = desadv.buyer_gln or desadv.recipient
    ET.SubElement(ih, "recipient").text = desadv.seller_gln or desadv.sender
    ET.SubElement(ih, "documentType").text       = "RECADV"
    ET.SubElement(ih, "creationDateTime").text   = dt_str
    ET.SubElement(ih, "creationDateTimeBySender").text = dt_str

    # ── receivingAdvice ───────────────────────────────────────────────────────
    ra = ET.SubElement(root, "receivingAdvice",
                       number=recadv_number,
                       date=today,
                       status="Original")

    # Ссылка на исходный заказ
    ET.SubElement(ra, "originOrder",
                  number=desadv.order_number,
                  date=desadv.order_date)

    # Ссылка на DESADV (despatchIdentificator)
    ET.SubElement(ra, "despatchIdentificator",
                  number=desadv.desadv_number,
                  date=desadv.desadv_date)

    # Стороны — только GLN (минимум достаточный по спецификации)
    seller_elem = ET.SubElement(ra, "seller")
    ET.SubElement(seller_elem, "gln").text = desadv.seller_gln

    buyer_elem = ET.SubElement(ra, "buyer")
    ET.SubElement(buyer_elem, "gln").text = desadv.buyer_gln

    # deliveryInfo
    di = ET.SubElement(ra, "deliveryInfo")
    actual_dt = ET.SubElement(di, "actualDeliveryDateTime")
    actual_dt.text = dt_str

    sf = ET.SubElement(di, "shipFrom")
    ET.SubElement(sf, "gln").text = desadv.ship_from or desadv.seller_gln

    st = ET.SubElement(di, "shipTo")
    ET.SubElement(st, "gln").text = desadv.ship_to or desadv.buyer_gln

    # ── lineItems ─────────────────────────────────────────────────────────────
    li_root = ET.SubElement(ra, "lineItems")
    ET.SubElement(li_root, "currencyISOCode").text = "RUB"

    for item in line_items:
        li = ET.SubElement(li_root, "lineItem")

        if item.get("gtin"):
            ET.SubElement(li, "gtin").text = item["gtin"]
        if item.get("internal_buyer_code"):
            ET.SubElement(li, "internalBuyerCode").text = item["internal_buyer_code"]
        if item.get("description"):
            ET.SubElement(li, "description").text = item["description"]

        # despatchedQuantity
        dq = ET.SubElement(li, "despatchedQuantity",
                            unitOfMeasure=item.get("uom", "PCE"))
        dq.text = item["despatched_qty"]

        # acceptedQuantity — ключевое поле RECADV
        aq = ET.SubElement(li, "acceptedQuantity",
                            unitOfMeasure=item.get("uom", "PCE"))
        aq.text = item["accepted_qty"]

        # Если есть расхождение — notDeliveredQuantity или overshippedQuantity
        try:
            diff = float(item["accepted_qty"]) - float(item["despatched_qty"])
            if diff < 0:
                nd = ET.SubElement(li, "notDeliveredQuantity",
                                   unitOfMeasure=item.get("uom", "PCE"))
                nd.text = f"{abs(diff):g}"
            elif diff > 0:
                ov = ET.SubElement(li, "overshippedQuantity",
                                   unitOfMeasure=item.get("uom", "PCE"))
                ov.text = f"{diff:g}"
        except (ValueError, TypeError) as exc:
            logger.warning("Не удалось вычислить расхождение количеств: %s", exc)

        if item.get("net_price"):
            ET.SubElement(li, "netPrice").text = item["net_price"]
        if item.get("vat_rate"):
            ET.SubElement(li, "vATRate").text = item["vat_rate"]

    ET.indent(root, space="  ")
    xml_str = '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(root, encoding="unicode")
    return xml_str, recadv_number
