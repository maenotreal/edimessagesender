import xml.etree.ElementTree as ET
import uuid
import random
from datetime import datetime, timezone
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def generate_orders_xml(buyer_gln, seller_gln, line_items, order_number=None):
    """
    Build an ORDERS XML with the given GLNs and list of line items.
    Each line item is a dict with keys: gtin, internal_buyer_code, description,
    requested_quantity, unit_of_measure, net_price, vat_rate.
    Returns XML string and the new GUID used for message id.
    """
    msg_id = str(uuid.uuid4())
    root = ET.Element("eDIMessage", id=msg_id)

    # Interchange header
    ih = ET.SubElement(root, "interchangeHeader")
    ET.SubElement(ih, "sender").text = buyer_gln
    ET.SubElement(ih, "recipient").text = seller_gln
    ET.SubElement(ih, "documentType").text = "ORDERS"

    now_utc = datetime.now(timezone.utc)
    dt_str = now_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    ET.SubElement(ih, "creationDateTime").text = dt_str

    # Order element
    order = ET.SubElement(root, "order")
    now_local = datetime.now()
    if order_number is None:
        random_suffix = random.randint(1000, 9999)
        order_number = f"ORD-{now_local.strftime('%Y%m%d-%H%M%S')}-{random_suffix}"
    order.set("number", order_number)
    order.set("date", now_local.strftime('%Y-%m-%d'))

    # Seller (only GLN)
    seller = ET.SubElement(order, "seller")
    ET.SubElement(seller, "gln").text = seller_gln

    # Buyer (only GLN)
    buyer = ET.SubElement(order, "buyer")
    ET.SubElement(buyer, "gln").text = buyer_gln

    # Delivery info
    delivery_info = ET.SubElement(order, "deliveryInfo")
    ship_from = ET.SubElement(delivery_info, "shipFrom")
    ET.SubElement(ship_from, "gln").text = seller_gln   # shipFrom = seller
    ship_to = ET.SubElement(delivery_info, "shipTo")
    ET.SubElement(ship_to, "gln").text = buyer_gln      # shipTo = buyer

    # Line items
    line_items_elem = ET.SubElement(order, "lineItems")
    ET.SubElement(line_items_elem, "currencyISOCode").text = "RUB"

    for li in line_items:
        line_item = ET.SubElement(line_items_elem, "lineItem")
        if li.get('gtin'):
            ET.SubElement(line_item, "gtin").text = li['gtin']
        if li.get('internal_buyer_code'):
            ET.SubElement(line_item, "internalBuyerCode").text = li['internal_buyer_code']
        if li.get('description'):
            ET.SubElement(line_item, "description").text = li['description']
        if li.get('requested_quantity') and li.get('unit_of_measure'):
            req_qty = ET.SubElement(line_item, "requestedQuantity")
            req_qty.text = li['requested_quantity']
            req_qty.set("unitOfMeasure", li['unit_of_measure'])
        if li.get('net_price'):
            ET.SubElement(line_item, "netPrice").text = li['net_price']
        if li.get('vat_rate'):
            ET.SubElement(line_item, "vATRate").text = li['vat_rate']

    ET.indent(root, space="  ")
    xml_str = '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(root, encoding='unicode')
    return xml_str, msg_id

def generate_pricat_xml(version, supplier_gln, buyer_gln, line_items,
                        delete: bool = True):
    """
    Сгенерировать XML файла каталога TradeItemTableLayout (PRICAT).

    Параметры:
        version      - 1 (общий, без покупателя) или 3 (для конкретного покупателя)
        supplier_gln - GLN поставщика
        buyer_gln    - GLN покупателя (обязателен для version=3, иначе игнорируется)
        line_items   - список dict с ключами:
                         gtin, internal_buyer_code, internal_supplier_code,
                         supplier_name, vat_rate
        delete       - True  -> удаление позиций (status="Deleted")
                       False -> добавление позиций (атрибут status не добавляется)

    Возвращает (xml_string, filename).
    Имена файлов: "01.xml" для version=1, "03.xml" для version=3.

    Порядок отправки:
        Удаление:   03.xml -> 01.xml
        Добавление: 01.xml -> 03.xml
    """
    from datetime import datetime

    pricat_number = supplier_gln
    if buyer_gln and version == 3:
        pricat_number += f"_{buyer_gln}"
    pricat_number += f"_{version}"

    date_str = datetime.now().strftime("%Y-%m-%d")

    root = ET.Element("pricat")
    root.set("pricatNumber", pricat_number)
    root.set("date", date_str)
    root.set("supplier", supplier_gln)
    if version == 3 and buyer_gln:
        root.set("buyer", buyer_gln)

    for item in line_items:
        line = ET.SubElement(root, "lineItem")
        line.set("gtin",                 str(item["gtin"]))
        line.set("internalSupplierCode", str(item["internal_supplier_code"]))
        line.set("supplierName",         str(item["supplier_name"]))
        line.set("internalBuyerCode",    str(item["internal_buyer_code"]))
        line.set("vATRate",              str(item["vat_rate"]))
        if delete:
            line.set("status", "Deleted")

    ET.indent(root, space="  ")
    xml_str  = '<?xml version="1.0" encoding="utf-8"?>' + "\n" + ET.tostring(root, encoding="unicode")
    filename = "03.xml" if version == 3 else "01.xml"
    return xml_str, filename


def input_full_line_item_manually(item_number):
    """
    Ask user to input all possible fields for a line item (based on the ORDERS template).
    Empty fields are omitted.
    Returns a dict with keys corresponding to XML tags.
    """
    print(f"\n--- Line Item #{item_number} (full details) ---")
    li = {}

    gtin = input("GTIN (enter to skip): ").strip()
    if gtin:
        li['gtin'] = gtin

    internal_buyer = input("Internal buyer code (enter to skip): ").strip()
    if internal_buyer:
        li['internal_buyer_code'] = internal_buyer

    internal_supplier = input("Internal supplier code (enter to skip): ").strip()
    if internal_supplier:
        li['internal_supplier_code'] = internal_supplier

    description = input("Description (enter to skip): ").strip()
    if description:
        li['description'] = description

    # Requested quantity with unit of measure
    req_qty = input("Requested quantity (enter to skip): ").strip()
    if req_qty:
        li['requested_quantity'] = req_qty
        uom = input("Unit of measure for requested quantity (default PCE, enter to use default): ").strip()
        li['unit_of_measure'] = uom if uom else "PCE"

    # One place quantity
    one_place = input("One place quantity (enter to skip): ").strip()
    if one_place:
        li['one_place_quantity'] = one_place
        uom = input("Unit of measure for one place quantity (default PCE, enter to use default): ").strip()
        li['one_place_unit'] = uom if uom else "PCE"

    net_price = input("Net price (without VAT) (enter to skip): ").strip()
    if net_price:
        li['net_price'] = net_price

    net_price_vat = input("Net price with VAT (enter to skip): ").strip()
    if net_price_vat:
        li['net_price_with_vat'] = net_price_vat

    net_amount = input("Net amount (sum without VAT) (enter to skip): ").strip()
    if net_amount:
        li['net_amount'] = net_amount

    vat_rate = input("VAT rate (e.g., 22, 20, 10, NOT_APPLICABLE) (enter to skip): ").strip()
    if vat_rate:
        li['vat_rate'] = vat_rate

    vat_amount = input("VAT amount (sum of VAT) (enter to skip): ").strip()
    if vat_amount:
        li['vat_amount'] = vat_amount

    amount = input("Total amount with VAT (enter to skip): ").strip()
    if amount:
        li['amount'] = amount

    return li if li else None