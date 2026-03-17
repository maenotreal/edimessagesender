#!/usr/bin/env python3
"""
tests.py — unit-тесты для EDI Message Sender.

Запуск:
    python tests.py        — все тесты, кратко
    python tests.py -v     — все тесты, подробно
"""

import base64
import copy
import io
import json
import logging
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))
logging.disable(logging.CRITICAL)


# ─────────────────────────────────────────────────────────────────────────────
# Фикстуры — XML-образцы
# ─────────────────────────────────────────────────────────────────────────────

ORDERS_XML = '''<?xml version="1.0" encoding="utf-8"?>
<eDIMessage id="test-orders-001">
  <interchangeHeader>
    <sender>1111111111111</sender>
    <recipient>2222222222222</recipient>
    <documentType>ORDERS</documentType>
  </interchangeHeader>
  <order number="ORD-TEST-001" date="2025-03-16">
    <seller><gln>2222222222222</gln></seller>
    <buyer><gln>1111111111111</gln></buyer>
    <lineItems>
      <lineItem>
        <gtin>0000001</gtin>
        <internalBuyerCode>100</internalBuyerCode>
        <description>Товар тест</description>
        <requestedQuantity unitOfMeasure="PCE">10.000</requestedQuantity>
        <netPrice>50.00</netPrice>
        <vATRate>20</vATRate>
      </lineItem>
    </lineItems>
  </order>
</eDIMessage>'''

DESADV_XML = '''<?xml version="1.0" encoding="utf-8"?>
<eDIMessage id="test-desadv-001">
  <interchangeHeader>
    <sender>2222222222222</sender>
    <recipient>1111111111111</recipient>
    <documentType>DESADV</documentType>
  </interchangeHeader>
  <despatchAdvice number="DESADV-001" date="2025-03-16" status="Original">
    <originOrder number="ORD-TEST-001" date="2025-03-16"/>
    <seller><gln>2222222222222</gln></seller>
    <buyer><gln>1111111111111</gln></buyer>
    <deliveryInfo>
      <shipFrom><gln>2222222222222</gln></shipFrom>
      <shipTo><gln>1111111111111</gln></shipTo>
    </deliveryInfo>
    <lineItems>
      <lineItem>
        <gtin>0000001</gtin>
        <internalBuyerCode>100</internalBuyerCode>
        <description>Товар тест</description>
        <despatchedQuantity unitOfMeasure="PCE">10.000</despatchedQuantity>
        <netPrice>50.00</netPrice>
        <vATRate>20</vATRate>
      </lineItem>
    </lineItems>
  </despatchAdvice>
</eDIMessage>'''

ORDRSP_XML = '''<?xml version="1.0" encoding="utf-8"?>
<eDIMessage id="test-ordrsp-001">
  <interchangeHeader>
    <sender>2222222222222</sender>
    <recipient>1111111111111</recipient>
    <documentType>ORDRSP</documentType>
  </interchangeHeader>
  <orderResponse number="ORDRSP-001" date="2025-03-16" status="Accepted">
    <originOrder number="ORD-TEST-001" date="2025-03-16"/>
  </orderResponse>
</eDIMessage>'''


def _make_config(auth_mode="oidc", **overrides):
    """Создать AppConfig без чтения файла с диска."""
    import config as cfg_mod
    obj = cfg_mod.AppConfig.__new__(cfg_mod.AppConfig)
    obj._cfg = {
        "auth_mode":    auth_mode,
        "api_base_url": "https://test-edi-api.kontur.ru",
        "oidc": {
            "client_id":     "test-client",
            "client_secret": "test-secret",
            "scope":         "edi-public-api-staging",
        },
        "legacy": {
            "api_client_id": "legacy-key",
            "login":         "user",
            "password":      "pass",
        },
        "edi": {
            "party_id":   "aaa-bbb-ccc",
            "buyer_gln":  "1111111111111",
            "seller_gln": "2222222222222",
        },
        "line_item_defaults": {
            "gtin": "0000001",
            "internal_buyer_code": "100",
            "description": "Тест",
            "requested_quantity": "10.000",
            "unit_of_measure": "PCE",
            "net_price": "50.00",
            "vat_rate": "20",
        },
    }
    obj._cfg.update(overrides)
    return obj


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательный миксин для изоляции store
# ─────────────────────────────────────────────────────────────────────────────

class StoreIsolationMixin:
    """
    Перенаправляет все пути store.py в изолированную временную папку.
    Патчим _BASE_DIR, STORE_FILE, DOCS_DIR одновременно,
    чтобы relative_to() работало корректно.
    """
    def _setup_store(self):
        import store
        self.store = store
        self._tmp = tempfile.TemporaryDirectory()
        self._base = Path(self._tmp.name)
        # Патчим все три переменные модуля
        self._orig_base  = store._BASE_DIR
        self._orig_store = store.STORE_FILE
        self._orig_docs  = store.DOCS_DIR
        store._BASE_DIR  = self._base
        store.STORE_FILE = self._base / "edi_store.json"
        store.DOCS_DIR   = self._base / "edi_documents"

    def _teardown_store(self):
        import store
        store._BASE_DIR  = self._orig_base
        store.STORE_FILE = self._orig_store
        store.DOCS_DIR   = self._orig_docs
        self._tmp.cleanup()

    def _save_orders(self, number="ORD-001", date="2025-03-16",
                     buyer="1111111111111", seller="2222222222222",
                     box_id="box1", xml="<x/>", circ="", msg=""):
        return self.store.save_orders(
            number, date, buyer, seller, box_id, xml, circ, msg
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestConfig
# ─────────────────────────────────────────────────────────────────────────────

class TestConfig(unittest.TestCase):

    def setUp(self):
        import config
        self.cfg_mod = config
        self.tmp = tempfile.TemporaryDirectory()
        self._orig = config.CONFIG_FILE
        config.CONFIG_FILE = Path(self.tmp.name) / "edi_config.json"

    def tearDown(self):
        self.cfg_mod.CONFIG_FILE = self._orig
        self.tmp.cleanup()

    def test_create_default_writes_file(self):
        cfg = self.cfg_mod.create_default()
        self.assertTrue(self.cfg_mod.CONFIG_FILE.exists())
        self.assertEqual(cfg["auth_mode"], "oidc")

    def test_save_and_load_roundtrip(self):
        data = copy.deepcopy(self.cfg_mod.DEFAULT_CONFIG)
        data["auth_mode"] = "legacy"
        data["edi"]["party_id"] = "test-party"
        self.cfg_mod.save(data)
        loaded = self.cfg_mod.load()
        self.assertEqual(loaded["auth_mode"], "legacy")
        self.assertEqual(loaded["edi"]["party_id"], "test-party")

    def test_load_missing_returns_empty(self):
        self.assertEqual(self.cfg_mod.load(), {})

    def test_appconfig_auth_mode(self):
        self.assertEqual(_make_config("oidc").auth_mode, "oidc")
        self.assertEqual(_make_config("legacy").auth_mode, "legacy")

    def test_appconfig_api_base_url_strips_trailing_slash(self):
        cfg = _make_config()
        cfg._cfg["api_base_url"] = "https://test.kontur.ru/"
        self.assertEqual(cfg.api_base_url, "https://test.kontur.ru")

    def test_validate_auth_oidc_ok(self):
        self.assertEqual(_make_config("oidc").validate_auth(), [])

    def test_validate_auth_oidc_missing_client_id(self):
        cfg = _make_config("oidc")
        cfg._cfg["oidc"]["client_id"] = ""
        self.assertIn("oidc.client_id", cfg.validate_auth())

    def test_validate_auth_oidc_missing_secret(self):
        cfg = _make_config("oidc")
        cfg._cfg["oidc"]["client_secret"] = ""
        self.assertIn("oidc.client_secret", cfg.validate_auth())

    def test_validate_auth_legacy_ok(self):
        self.assertEqual(_make_config("legacy").validate_auth(), [])

    def test_validate_auth_legacy_missing_password(self):
        cfg = _make_config("legacy")
        cfg._cfg["legacy"]["password"] = ""
        self.assertIn("legacy.password", cfg.validate_auth())

    def test_validate_edi_ok(self):
        self.assertEqual(_make_config().validate_edi(), [])

    def test_validate_edi_missing_party_id(self):
        cfg = _make_config()
        cfg._cfg["edi"]["party_id"] = ""
        self.assertIn("edi.party_id", cfg.validate_edi())

    def test_validate_edi_missing_buyer_gln(self):
        cfg = _make_config()
        cfg._cfg["edi"]["buyer_gln"] = ""
        self.assertIn("edi.buyer_gln", cfg.validate_edi())

    def test_deep_get_nested_values(self):
        cfg = _make_config()
        self.assertEqual(cfg.oidc_client_id, "test-client")
        self.assertEqual(cfg.oidc_scope, "edi-public-api-staging")
        self.assertEqual(cfg.party_id, "aaa-bbb-ccc")
        self.assertEqual(cfg.buyer_gln, "1111111111111")
        self.assertEqual(cfg.seller_gln, "2222222222222")

    def test_deep_get_missing_section_returns_default(self):
        cfg = _make_config()
        cfg._cfg.pop("edi", None)
        self.assertEqual(cfg.party_id, "")


# ─────────────────────────────────────────────────────────────────────────────
# TestAuth
# ─────────────────────────────────────────────────────────────────────────────

class TestAuth(unittest.TestCase):

    def setUp(self):
        import auth
        self.auth = auth
        self.tmp = tempfile.TemporaryDirectory()
        self._orig = auth.TOKEN_CACHE
        auth.TOKEN_CACHE = Path(self.tmp.name) / ".token_cache.json"

    def tearDown(self):
        self.auth.TOKEN_CACHE = self._orig
        self.tmp.cleanup()

    def test_cache_save_and_load(self):
        self.auth._save_cache({"access_token": "tok123", "expiry": "2099-01-01"})
        loaded = self.auth._load_cache()
        self.assertEqual(loaded["access_token"], "tok123")

    def test_cache_missing_returns_empty_dict(self):
        self.assertEqual(self.auth._load_cache(), {})

    def test_clear_cache_removes_file(self):
        self.auth._save_cache({"token": "x"})
        self.auth._clear_cache()
        self.assertFalse(self.auth.TOKEN_CACHE.exists())

    def test_is_fresh_future(self):
        future = (datetime.now() + timedelta(hours=1)).isoformat()
        self.assertTrue(self.auth._is_fresh({"expiry": future}))

    def test_is_fresh_past(self):
        past = (datetime.now() - timedelta(seconds=1)).isoformat()
        self.assertFalse(self.auth._is_fresh({"expiry": past}))

    def test_is_fresh_with_buffer_expires_soon(self):
        # Истекает через 3 мин, буфер 5 мин → не свежий
        soon = (datetime.now() + timedelta(minutes=3)).isoformat()
        self.assertFalse(self.auth._is_fresh({"expiry": soon}, buffer=300))

    def test_is_fresh_with_buffer_ok(self):
        # Истекает через 10 мин, буфер 5 мин → свежий
        later = (datetime.now() + timedelta(minutes=10)).isoformat()
        self.assertTrue(self.auth._is_fresh({"expiry": later}, buffer=300))

    def test_is_fresh_missing_key(self):
        self.assertFalse(self.auth._is_fresh({}))

    def test_build_auth_header_oidc(self):
        self.assertEqual(
            self.auth.build_auth_header("mytoken", _make_config("oidc")),
            "Bearer mytoken"
        )

    def test_build_auth_header_legacy_contains_required_parts(self):
        header = self.auth.build_auth_header("mytoken", _make_config("legacy"))
        self.assertIn("KonturEdiAuth", header)
        self.assertIn("legacy-key", header)
        self.assertIn("mytoken", header)

    def test_get_oidc_token_uses_cache(self):
        future = (datetime.now() + timedelta(hours=2)).isoformat()
        self.auth._save_cache({
            "auth_mode": "oidc", "access_token": "cached",
            "refresh_token": "", "expiry": future,
        })
        self.assertEqual(
            self.auth._get_oidc_token(_make_config("oidc"), MagicMock()),
            "cached"
        )

    def test_get_legacy_token_uses_cache(self):
        future = (datetime.now() + timedelta(hours=6)).isoformat()
        self.auth._save_cache({
            "auth_mode": "legacy", "token": "legacy_cached", "expiry": future,
        })
        self.assertEqual(
            self.auth._get_legacy_token(_make_config("legacy"), MagicMock()),
            "legacy_cached"
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestApi
# ─────────────────────────────────────────────────────────────────────────────

class TestApi(unittest.TestCase):

    def setUp(self):
        import api
        self.api = api
        self.cfg = _make_config("oidc")
        self.dl  = MagicMock()

    def _mock_resp(self, status=200, body=None, text="{}"):
        r = MagicMock()
        r.status_code = status
        r.text = text
        r.headers = {"Content-Type": "application/json"}
        if body is not None:
            r.json.return_value = body
        return r

    def test_is_xml_detects_xml_bytes(self):
        self.assertTrue(self.api._is_xml(b"<?xml version"))
        self.assertTrue(self.api._is_xml(b"<root>data</root>"))

    def test_is_xml_detects_json(self):
        self.assertFalse(self.api._is_xml(b'{"key": "val"}'))
        self.assertFalse(self.api._is_xml("plain text"))

    def test_get_box_id_success(self):
        with patch("api._request", return_value=self._mock_resp(200, {"Id": "box-abc"})):
            result = self.api.get_box_id("party-1", self.cfg, "tok", self.dl)
        self.assertEqual(result, "box-abc")

    def test_get_box_id_404_raises(self):
        with patch("api._request", return_value=self._mock_resp(404, text="Not Found")):
            with self.assertRaises(RuntimeError) as ctx:
                self.api.get_box_id("party-1", self.cfg, "tok", self.dl)
        self.assertIn("не найден", str(ctx.exception))

    def test_get_box_id_missing_id_raises(self):
        with patch("api._request", return_value=self._mock_resp(200, {})):
            with self.assertRaises(RuntimeError) as ctx:
                self.api.get_box_id("party-1", self.cfg, "tok", self.dl)
        self.assertIn("без поля Id", str(ctx.exception))

    def test_send_message_success(self):
        resp_body = {"MessageId": "msg-1", "DocumentCirculationId": "circ-1"}
        with patch("api._request", return_value=self._mock_resp(200, resp_body)):
            result = self.api.send_message(
                "box-1", self.cfg, "tok", self.dl, b"<xml/>", "test.xml"
            )
        self.assertEqual(result["MessageId"], "msg-1")
        self.assertEqual(result["DocumentCirculationId"], "circ-1")

    def test_send_message_converts_str_to_bytes(self):
        captured = {}
        def fake_request(*args, **kwargs):
            captured["data"] = kwargs.get("data")
            return self._mock_resp(200, {})
        with patch("api._request", side_effect=fake_request):
            self.api.send_message("box-1", self.cfg, "tok", self.dl, "<xml/>", "f.xml")
        self.assertIsInstance(captured["data"], bytes)

    def test_send_message_non_200_raises(self):
        with patch("api._request", return_value=self._mock_resp(400, text="Bad Request")):
            with self.assertRaises(RuntimeError) as ctx:
                self.api.send_message("b", self.cfg, "t", self.dl, b"x", "f.xml")
        self.assertIn("400", str(ctx.exception))

    def test_get_inbox_message_xml_decodes_base64(self):
        content = "<eDIMessage>test</eDIMessage>"
        encoded = base64.b64encode(content.encode()).decode()
        body = {"Data": {"MessageBody": encoded}}
        with patch("api._request", return_value=self._mock_resp(200, body)):
            result = self.api.get_inbox_message_xml(
                "box-1", "msg-1", self.cfg, "tok", self.dl
            )
        self.assertEqual(result, content)

    def test_get_inbox_message_xml_404_raises(self):
        with patch("api._request", return_value=self._mock_resp(404, text="Not found")):
            with self.assertRaises(RuntimeError) as ctx:
                self.api.get_inbox_message_xml("box-1", "msg-1", self.cfg, "tok", self.dl)
        self.assertIn("не найдено", str(ctx.exception))

    def test_get_inbox_message_xml_missing_body_raises(self):
        with patch("api._request", return_value=self._mock_resp(200, {"Data": {}})):
            with self.assertRaises(RuntimeError) as ctx:
                self.api.get_inbox_message_xml("box-1", "msg-1", self.cfg, "tok", self.dl)
        self.assertIn("MessageBody", str(ctx.exception))

    def test_get_events_from_sends_date_only(self):
        """API требует YYYY-MM-DD — не должно передаваться с временем."""
        captured = {}
        def fake_req(method, path, cfg, dl, token, params=None, **kw):
            captured["params"] = params
            return self._mock_resp(200, {"Events": [], "LastEventId": ""})
        with patch("api._request", side_effect=fake_req):
            self.api.get_events_from(
                "box-1", self.cfg, "tok", self.dl,
                from_date="2025-03-16T12:30:00"
            )
        dt = captured["params"]["fromDateTime"]
        self.assertRegex(dt, r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(dt, "2025-03-16")

    def test_get_events_returns_dict(self):
        body = {"Events": [{"EventType": "NewInboxMessage"}], "LastEventId": "ev-1"}
        with patch("api._request", return_value=self._mock_resp(200, body)):
            result = self.api.get_events("box-1", self.cfg, "tok", self.dl)
        self.assertIn("Events", result)
        self.assertEqual(len(result["Events"]), 1)

    def test_request_retries_once_on_401(self):
        """401 → сброс токена → повтор. Второй запрос должен вернуть 200."""
        calls = {"n": 0}
        r401 = self._mock_resp(401, text="Unauthorized")
        r200 = self._mock_resp(200, {"Id": "box-retry"})

        def fake_http(method, url, **kw):
            calls["n"] += 1
            return r401 if calls["n"] == 1 else r200

        with patch("requests.request", side_effect=fake_http):
            with patch("api.build_auth_header", return_value="Bearer tok"):
                with patch("api.get_token", return_value="new"):
                    with patch("api.invalidate_token"):
                        result = self.api.get_box_id("p", self.cfg, "tok", self.dl)
        self.assertEqual(result, "box-retry")
        self.assertEqual(calls["n"], 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestStore
# ─────────────────────────────────────────────────────────────────────────────

class TestStore(StoreIsolationMixin, unittest.TestCase):

    def setUp(self):
        self._setup_store()

    def tearDown(self):
        self._teardown_store()

    # ── ORDERS CRUD ───────────────────────────────────────────────────────────

    def test_save_orders_creates_record(self):
        oid = self._save_orders("ORD-001")
        orders = self.store.get_all_orders()
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["order_number"], "ORD-001")
        self.assertEqual(orders[0]["orders_status"], self.store.STATUS_PENDING)

    def test_save_orders_writes_xml_file(self):
        oid = self._save_orders(xml="<orders_content/>")
        o = self.store.get_order_by_id(oid)
        xml_path = self._base / o["xml_file"]
        self.assertTrue(xml_path.exists())
        self.assertIn("orders_content", xml_path.read_text())

    def test_save_orders_stores_message_id(self):
        oid = self._save_orders(msg="msg-xyz")
        o = self.store.get_order_by_id(oid)
        self.assertEqual(o["message_id"], "msg-xyz")

    def test_save_multiple_orders(self):
        self._save_orders("ORD-001")
        self._save_orders("ORD-002")
        self.assertEqual(len(self.store.get_all_orders()), 2)

    def test_get_order_by_id_found(self):
        oid = self._save_orders("ORD-FIND")
        o = self.store.get_order_by_id(oid)
        self.assertIsNotNone(o)
        self.assertEqual(o["id"], oid)
        self.assertEqual(o["order_number"], "ORD-FIND")

    def test_get_order_by_id_missing(self):
        self.assertIsNone(self.store.get_order_by_id("nonexistent-id"))

    def test_update_orders_status(self):
        oid = self._save_orders()
        self.store.update_orders_status(oid, self.store.STATUS_ACCEPTED)
        self.assertEqual(
            self.store.get_order_by_id(oid)["orders_status"],
            self.store.STATUS_ACCEPTED
        )

    def test_update_orders_status_all_values(self):
        statuses = [
            self.store.STATUS_DELIVERED,
            self.store.STATUS_CHECKING_OK,
            self.store.STATUS_CHECKING_FAIL,
            self.store.STATUS_ACCEPTED,
            self.store.STATUS_REJECTED,
            self.store.STATUS_CHANGED,
        ]
        oid = self._save_orders()
        for s in statuses:
            self.store.update_orders_status(oid, s)
            self.assertEqual(self.store.get_order_by_id(oid)["orders_status"], s)

    def test_update_order_fields(self):
        oid = self._save_orders()
        self.store.update_order_fields(oid, doc_circ_id="circ-new", message_id="msg-new")
        o = self.store.get_order_by_id(oid)
        self.assertEqual(o["doc_circ_id"], "circ-new")
        self.assertEqual(o["message_id"], "msg-new")

    # ── DESADV ────────────────────────────────────────────────────────────────

    def test_attach_desadv_creates_record(self):
        oid = self._save_orders()
        did = self.store.attach_desadv(oid, "DESADV-001", "2025-03-16", "<d/>")
        self.assertIsNotNone(did)
        o = self.store.get_order_by_id(oid)
        self.assertEqual(len(o["desadv"]), 1)
        self.assertEqual(o["desadv"][0]["desadv_number"], "DESADV-001")
        self.assertFalse(o["desadv"][0]["recadv_sent"])

    def test_attach_desadv_writes_xml_file(self):
        oid = self._save_orders()
        did = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<desadv_body/>")
        o   = self.store.get_order_by_id(oid)
        xml_path = self._base / o["desadv"][0]["xml_file"]
        self.assertTrue(xml_path.exists())
        self.assertIn("desadv_body", xml_path.read_text())

    def test_attach_desadv_deduplication(self):
        oid  = self._save_orders()
        did1 = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        did2 = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        self.assertEqual(did1, did2)
        self.assertEqual(len(self.store.get_order_by_id(oid)["desadv"]), 1)

    def test_attach_desadv_multiple(self):
        oid = self._save_orders()
        self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d1/>")
        self.store.attach_desadv(oid, "D-002", "2025-03-17", "<d2/>")
        self.assertEqual(len(self.store.get_order_by_id(oid)["desadv"]), 2)

    def test_attach_desadv_unknown_order_returns_none(self):
        result = self.store.attach_desadv("bad-id", "D-001", "2025-03-16", "<d/>")
        self.assertIsNone(result)

    def test_mark_recadv_sent(self):
        oid = self._save_orders()
        did = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        self.store.mark_recadv_sent(oid, did)
        o = self.store.get_order_by_id(oid)
        self.assertTrue(o["desadv"][0]["recadv_sent"])

    def test_mark_recadv_sent_only_marks_target(self):
        oid  = self._save_orders()
        did1 = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d1/>")
        did2 = self.store.attach_desadv(oid, "D-002", "2025-03-17", "<d2/>")
        self.store.mark_recadv_sent(oid, did1)
        o = self.store.get_order_by_id(oid)
        self.assertTrue(o["desadv"][0]["recadv_sent"])
        self.assertFalse(o["desadv"][1]["recadv_sent"])

    def test_read_xml(self):
        oid = self._save_orders(xml="<hello_world/>")
        o   = self.store.get_order_by_id(oid)
        content = self.store.read_xml(o["xml_file"])
        self.assertIsNotNone(content)
        self.assertIn("hello_world", content)

    def test_read_xml_missing_returns_none(self):
        self.assertIsNone(self.store.read_xml("nonexistent/path.xml"))

    # ── Удаление ──────────────────────────────────────────────────────────────

    def test_delete_order_removes_record(self):
        oid = self._save_orders()
        self.assertTrue(self.store.delete_order(oid))
        self.assertIsNone(self.store.get_order_by_id(oid))

    def test_delete_order_removes_xml_files(self):
        oid = self._save_orders()
        did = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        o   = self.store.get_order_by_id(oid)
        orders_xml = self._base / o["xml_file"]
        desadv_xml = self._base / o["desadv"][0]["xml_file"]
        self.store.delete_order(oid)
        self.assertFalse(orders_xml.exists())
        self.assertFalse(desadv_xml.exists())

    def test_delete_order_unknown_returns_false(self):
        self.assertFalse(self.store.delete_order("nonexistent"))

    def test_delete_order_does_not_affect_others(self):
        oid1 = self._save_orders("ORD-001")
        oid2 = self._save_orders("ORD-002")
        self.store.delete_order(oid1)
        self.assertIsNone(self.store.get_order_by_id(oid1))
        self.assertIsNotNone(self.store.get_order_by_id(oid2))

    def test_purge_old_orders(self):
        oid_old = self._save_orders("ORD-OLD")
        oid_new = self._save_orders("ORD-NEW")
        # Делаем oid_old старым — меняем sent_at в JSON напрямую
        data = json.loads(self.store.STORE_FILE.read_text())
        for o in data["orders"]:
            if o["id"] == oid_old:
                o["sent_at"] = (datetime.now() - timedelta(days=200)).isoformat()
        self.store.STORE_FILE.write_text(json.dumps(data))

        n = self.store.purge_old_orders(90)
        self.assertEqual(n, 1)
        self.assertIsNone(self.store.get_order_by_id(oid_old))
        self.assertIsNotNone(self.store.get_order_by_id(oid_new))

    def test_purge_old_orders_none_qualify(self):
        self._save_orders("ORD-RECENT")
        n = self.store.purge_old_orders(90)
        self.assertEqual(n, 0)
        self.assertEqual(len(self.store.get_all_orders()), 1)

    def test_purge_completed_orders(self):
        oid = self._save_orders()
        did = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        self.store.mark_recadv_sent(oid, did)
        self.store.update_orders_status(oid, self.store.STATUS_ACCEPTED)
        n = self.store.purge_completed_orders()
        self.assertEqual(n, 1)
        self.assertIsNone(self.store.get_order_by_id(oid))

    def test_purge_completed_rejected_without_desadv(self):
        """ORDERS Rejected без DESADV тоже считается завершённым."""
        oid = self._save_orders()
        self.store.update_orders_status(oid, self.store.STATUS_REJECTED)
        n = self.store.purge_completed_orders()
        self.assertEqual(n, 1)

    def test_purge_completed_skips_pending_recadv(self):
        oid = self._save_orders()
        self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        self.store.update_orders_status(oid, self.store.STATUS_ACCEPTED)
        # recadv_sent остаётся False → не удаляем
        n = self.store.purge_completed_orders()
        self.assertEqual(n, 0)

    def test_purge_completed_skips_pending_status(self):
        oid = self._save_orders()
        did = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d/>")
        self.store.mark_recadv_sent(oid, did)
        # Статус pending → не удаляем
        n = self.store.purge_completed_orders()
        self.assertEqual(n, 0)

    # ── Утилиты ───────────────────────────────────────────────────────────────

    def test_strikethrough_adds_combining_char(self):
        self.assertEqual(self.store._strikethrough("abc"), "a\u0336b\u0336c\u0336")

    def test_strikethrough_empty_string(self):
        self.assertEqual(self.store._strikethrough(""), "")

    def test_status_labels_cover_all_statuses(self):
        for s in [
            self.store.STATUS_PENDING, self.store.STATUS_DELIVERED,
            self.store.STATUS_CHECKING_OK, self.store.STATUS_CHECKING_FAIL,
            self.store.STATUS_ACCEPTED, self.store.STATUS_REJECTED,
            self.store.STATUS_CHANGED,
        ]:
            self.assertIn(s, self.store._STATUS_LABEL, f"Статус {s!r} не в _STATUS_LABEL")

    def test_store_persists_across_reloads(self):
        """Данные должны сохраняться между вызовами _load/_save."""
        oid = self._save_orders("ORD-PERSIST")
        # Перезагружаем данные
        orders = self.store.get_all_orders()
        self.assertEqual(orders[0]["order_number"], "ORD-PERSIST")


# ─────────────────────────────────────────────────────────────────────────────
# TestXmlBuilder
# ─────────────────────────────────────────────────────────────────────────────

class TestXmlBuilder(unittest.TestCase):

    def setUp(self):
        import xml_builder
        self.xb = xml_builder
        self.line_item = {
            "gtin": "0000001",
            "internal_buyer_code": "100",
            "description": "Тест",
            "requested_quantity": "10.000",
            "unit_of_measure": "PCE",
            "net_price": "50.00",
            "vat_rate": "20",
        }

    # ── generate_orders_xml ───────────────────────────────────────────────────

    def test_orders_produces_valid_xml(self):
        xml, _ = self.xb.generate_orders_xml("111", "222", [self.line_item])
        root = ET.fromstring(xml)
        self.assertEqual(root.tag, "eDIMessage")

    def test_orders_document_type_is_orders(self):
        xml, _ = self.xb.generate_orders_xml("111", "222", [self.line_item])
        root = ET.fromstring(xml)
        self.assertEqual(root.findtext("interchangeHeader/documentType"), "ORDERS")

    def test_orders_sender_recipient(self):
        xml, _ = self.xb.generate_orders_xml("1111111111111", "2222222222222",
                                               [self.line_item])
        root = ET.fromstring(xml)
        self.assertEqual(root.findtext("interchangeHeader/sender"), "1111111111111")
        self.assertEqual(root.findtext("interchangeHeader/recipient"), "2222222222222")

    def test_orders_returns_uuid(self):
        import re
        _, guid = self.xb.generate_orders_xml("111", "222", [self.line_item])
        self.assertRegex(guid,
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

    def test_orders_custom_number(self):
        xml, _ = self.xb.generate_orders_xml("111", "222", [self.line_item],
                                               order_number="MY-ORDER-42")
        root = ET.fromstring(xml)
        self.assertEqual(root.find("order").get("number"), "MY-ORDER-42")

    def test_orders_date_format(self):
        xml, _ = self.xb.generate_orders_xml("111", "222", [self.line_item])
        root = ET.fromstring(xml)
        date = root.find("order").get("date")
        self.assertRegex(date, r"^\d{4}-\d{2}-\d{2}$")

    def test_orders_multiple_line_items(self):
        items = [self.line_item, {**self.line_item, "gtin": "0000002"}]
        xml, _ = self.xb.generate_orders_xml("111", "222", items)
        root = ET.fromstring(xml)
        self.assertEqual(len(root.findall(".//lineItem")), 2)

    def test_orders_line_item_fields(self):
        xml, _ = self.xb.generate_orders_xml("111", "222", [self.line_item])
        root = ET.fromstring(xml)
        li = root.find(".//lineItem")
        self.assertEqual(li.findtext("gtin"), "0000001")
        self.assertEqual(li.findtext("internalBuyerCode"), "100")
        self.assertEqual(li.findtext("description"), "Тест")

    def test_orders_optional_fields_skipped_when_empty(self):
        item = {**self.line_item, "description": "", "net_price": ""}
        xml, _ = self.xb.generate_orders_xml("111", "222", [item])
        self.assertNotIn("<description>", xml)
        self.assertNotIn("<netPrice>", xml)

    def test_orders_each_call_unique_guid(self):
        _, g1 = self.xb.generate_orders_xml("111", "222", [self.line_item])
        _, g2 = self.xb.generate_orders_xml("111", "222", [self.line_item])
        self.assertNotEqual(g1, g2)

    # ── generate_pricat_xml ───────────────────────────────────────────────────

    def _pricat_item(self):
        return {
            "gtin": "0000001",
            "internal_buyer_code": "00100",
            "internal_supplier_code": "S001",
            "supplier_name": "ООО Тест",
            "vat_rate": "20",
        }

    def test_pricat_delete_has_status_deleted(self):
        xml, _ = self.xb.generate_pricat_xml(3, "111", "222", [self._pricat_item()],
                                               delete=True)
        self.assertIn('status="Deleted"', xml)

    def test_pricat_add_has_no_status(self):
        xml, _ = self.xb.generate_pricat_xml(1, "111", None, [self._pricat_item()],
                                               delete=False)
        self.assertNotIn("status", xml)

    def test_pricat_v1_filename_is_01(self):
        _, fn = self.xb.generate_pricat_xml(1, "111", None, [self._pricat_item()])
        self.assertEqual(fn, "01.xml")

    def test_pricat_v3_filename_is_03(self):
        _, fn = self.xb.generate_pricat_xml(3, "111", "222", [self._pricat_item()])
        self.assertEqual(fn, "03.xml")

    def test_pricat_v1_has_no_buyer_attribute(self):
        xml, _ = self.xb.generate_pricat_xml(1, "111", "222", [self._pricat_item()])
        root = ET.fromstring(xml)
        self.assertNotIn("buyer", root.attrib)

    def test_pricat_v3_has_buyer_attribute(self):
        xml, _ = self.xb.generate_pricat_xml(3, "111", "222", [self._pricat_item()])
        root = ET.fromstring(xml)
        self.assertEqual(root.attrib.get("buyer"), "222")

    def test_pricat_date_is_iso_format(self):
        xml, _ = self.xb.generate_pricat_xml(1, "111", None, [self._pricat_item()])
        root = ET.fromstring(xml)
        self.assertRegex(root.attrib["date"], r"^\d{4}-\d{2}-\d{2}$")

    def test_pricat_leading_zeros_in_gtin_preserved(self):
        item = {**self._pricat_item(), "gtin": "0123456789012"}
        xml, _ = self.xb.generate_pricat_xml(1, "111", None, [item])
        self.assertIn('gtin="0123456789012"', xml)

    def test_pricat_leading_zeros_in_buyer_code_preserved(self):
        item = {**self._pricat_item(), "internal_buyer_code": "00100"}
        xml, _ = self.xb.generate_pricat_xml(1, "111", None, [item])
        self.assertIn('internalBuyerCode="00100"', xml)

    def test_pricat_pricat_number_format_v1(self):
        xml, _ = self.xb.generate_pricat_xml(1, "SUPPLIER_GLN", None,
                                               [self._pricat_item()])
        root = ET.fromstring(xml)
        self.assertEqual(root.attrib["pricatNumber"], "SUPPLIER_GLN_1")

    def test_pricat_pricat_number_format_v3(self):
        xml, _ = self.xb.generate_pricat_xml(3, "SUPP", "BUYER",
                                               [self._pricat_item()])
        root = ET.fromstring(xml)
        self.assertEqual(root.attrib["pricatNumber"], "SUPP_BUYER_3")

    def test_pricat_multiple_items(self):
        items = [self._pricat_item(), {**self._pricat_item(), "gtin": "9999999"}]
        xml, _ = self.xb.generate_pricat_xml(1, "111", None, items)
        root = ET.fromstring(xml)
        self.assertEqual(len(root.findall("lineItem")), 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestRecadvBuilder
# ─────────────────────────────────────────────────────────────────────────────

class TestRecadvBuilder(unittest.TestCase):

    def setUp(self):
        import recadv_builder
        self.rb = recadv_builder

    def _desadv(self):
        return self.rb.DesadvData(DESADV_XML)

    def _accepted(self, qty="10.000"):
        return [{
            "gtin": "0000001",
            "internal_buyer_code": "100",
            "description": "Товар тест",
            "despatched_qty": "10.000",
            "accepted_qty": qty,
            "uom": "PCE",
            "net_price": "50.00",
            "vat_rate": "20",
        }]

    # ── DesadvData ────────────────────────────────────────────────────────────

    def test_parse_order_number(self):
        self.assertEqual(self._desadv().order_number, "ORD-TEST-001")

    def test_parse_desadv_number(self):
        self.assertEqual(self._desadv().desadv_number, "DESADV-001")

    def test_parse_seller_gln(self):
        self.assertEqual(self._desadv().seller_gln, "2222222222222")

    def test_parse_buyer_gln(self):
        self.assertEqual(self._desadv().buyer_gln, "1111111111111")

    def test_parse_ship_from(self):
        self.assertEqual(self._desadv().ship_from, "2222222222222")

    def test_parse_ship_to(self):
        self.assertEqual(self._desadv().ship_to, "1111111111111")

    def test_parse_line_items_count(self):
        self.assertEqual(len(self._desadv().line_items), 1)

    def test_parse_line_item_gtin(self):
        self.assertEqual(self._desadv().line_items[0]["gtin"], "0000001")

    def test_parse_line_item_qty(self):
        li = self._desadv().line_items[0]
        self.assertEqual(li["despatched_qty"], "10.000")
        self.assertEqual(li["despatched_uom"], "PCE")

    def test_parse_line_item_price(self):
        li = self._desadv().line_items[0]
        self.assertEqual(li["net_price"], "50.00")
        self.assertEqual(li["vat_rate"], "20")

    def test_parse_invalid_xml_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self.rb.DesadvData("<wrong_root/>")
        self.assertIn("despatchAdvice", str(ctx.exception))

    def test_parse_malformed_xml_raises(self):
        with self.assertRaises(ValueError):
            self.rb.DesadvData("not xml at all {{}")

    # ── build_recadv_xml ──────────────────────────────────────────────────────

    def test_produces_valid_xml(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        self.assertEqual(root.tag, "eDIMessage")

    def test_document_type_is_recadv(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        self.assertEqual(root.findtext("interchangeHeader/documentType"), "RECADV")

    def test_sender_is_buyer(self):
        """RECADV отправляет покупатель."""
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        self.assertEqual(root.findtext("interchangeHeader/sender"), "1111111111111")
        self.assertEqual(root.findtext("interchangeHeader/recipient"), "2222222222222")

    def test_references_original_order(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        ra = root.find("receivingAdvice")
        self.assertEqual(ra.find("originOrder").get("number"), "ORD-TEST-001")

    def test_references_desadv(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        ra = root.find("receivingAdvice")
        self.assertEqual(ra.find("despatchIdentificator").get("number"), "DESADV-001")

    def test_status_is_original(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        self.assertEqual(root.find("receivingAdvice").get("status"), "Original")

    def test_custom_number_used(self):
        xml, num = self.rb.build_recadv_xml(self._desadv(), self._accepted(),
                                              recadv_number="RECV-CUSTOM-99")
        self.assertEqual(num, "RECV-CUSTOM-99")
        root = ET.fromstring(xml)
        self.assertEqual(root.find("receivingAdvice").get("number"), "RECV-CUSTOM-99")

    def test_auto_number_generated(self):
        _, num = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        self.assertTrue(num.startswith("RECV-"))

    def test_exact_qty_no_discrepancy_fields(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted("10.000"))
        self.assertNotIn("notDeliveredQuantity", xml)
        self.assertNotIn("overshippedQuantity", xml)

    def test_less_than_despatched_adds_not_delivered(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted("7.000"))
        self.assertIn("notDeliveredQuantity", xml)
        self.assertNotIn("overshippedQuantity", xml)

    def test_more_than_despatched_adds_overshipped(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted("13.000"))
        self.assertIn("overshippedQuantity", xml)
        self.assertNotIn("notDeliveredQuantity", xml)

    def test_not_delivered_value_is_correct(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted("7.000"))
        root = ET.fromstring(xml)
        nd = root.find(".//notDeliveredQuantity")
        self.assertIsNotNone(nd)
        self.assertAlmostEqual(float(nd.text), 3.0)

    def test_overshipped_value_is_correct(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted("12.500"))
        root = ET.fromstring(xml)
        ov = root.find(".//overshippedQuantity")
        self.assertIsNotNone(ov)
        self.assertAlmostEqual(float(ov.text), 2.5)

    def test_accepted_quantity_in_xml(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted("9.000"))
        root = ET.fromstring(xml)
        aq = root.find(".//acceptedQuantity")
        self.assertIsNotNone(aq)
        self.assertEqual(aq.text, "9.000")

    def test_uom_preserved_in_quantities(self):
        xml, _ = self.rb.build_recadv_xml(self._desadv(), self._accepted())
        root = ET.fromstring(xml)
        aq = root.find(".//acceptedQuantity")
        self.assertEqual(aq.get("unitOfMeasure"), "PCE")


# ─────────────────────────────────────────────────────────────────────────────
# TestUpdater
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdater(unittest.TestCase):

    def setUp(self):
        import updater
        self.up = updater
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self._orig_vf = updater.VERSION_FILE
        self._stdout_patch = patch("sys.stdout", new_callable=io.StringIO)
        self._stdout_patch.start()

    def tearDown(self):
        self._stdout_patch.stop()
        self.up.VERSION_FILE = self._orig_vf
        self.tmp.cleanup()

    def _set_version(self, v):
        vf = self.base / "version.json"
        vf.write_text(json.dumps({
            "version":     v,
            "zip_url":     "https://example.com/update.zip",
            "version_url": "https://example.com/version.json",
        }), encoding="utf-8")
        self.up.VERSION_FILE = vf
        return vf

    def _make_zip(self, files, prefix=""):
        zp = self.base / "update.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            for name, content in files.items():
                zf.writestr(prefix + name, content)
        return zp

    # ── _parse_version ────────────────────────────────────────────────────────

    def test_parse_simple(self):
        self.assertEqual(self.up._parse_version("1.2.3"), (1, 2, 3))

    def test_parse_single_component(self):
        self.assertEqual(self.up._parse_version("5"), (5,))

    def test_parse_comparison_patch(self):
        self.assertLess(self.up._parse_version("1.0.0"),
                        self.up._parse_version("1.0.1"))

    def test_parse_comparison_minor(self):
        self.assertLess(self.up._parse_version("1.2.0"),
                        self.up._parse_version("1.3.0"))

    def test_parse_comparison_major(self):
        self.assertLess(self.up._parse_version("1.9.9"),
                        self.up._parse_version("2.0.0"))

    def test_parse_equality(self):
        self.assertEqual(self.up._parse_version("2.5.1"),
                         self.up._parse_version("2.5.1"))

    def test_parse_invalid_returns_zero(self):
        self.assertEqual(self.up._parse_version("bad"), (0,))

    def test_parse_empty_returns_zero(self):
        self.assertEqual(self.up._parse_version(""), (0,))

    # ── _local_version ────────────────────────────────────────────────────────

    def test_local_version_reads_correctly(self):
        self._set_version("1.5.0")
        self.assertEqual(self.up._local_version(), "1.5.0")

    def test_local_version_missing_file_returns_none(self):
        self.up.VERSION_FILE = self.base / "no_version.json"
        self.assertIsNone(self.up._local_version())

    def test_local_version_corrupted_file_returns_none(self):
        vf = self.base / "version.json"
        vf.write_text("not json !!!!")
        self.up.VERSION_FILE = vf
        self.assertIsNone(self.up._local_version())

    # ── _install_zip ──────────────────────────────────────────────────────────

    def test_install_updates_files(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        zp = self._make_zip({"main.py": "# updated", "new_module.py": "# new"})
        self.assertTrue(self.up._install_zip(zp, install_dir))
        self.assertEqual((install_dir / "main.py").read_text(), "# updated")
        self.assertTrue((install_dir / "new_module.py").exists())

    def test_install_strips_top_level_prefix(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        zp = self._make_zip({"main.py": "# new"}, prefix="edi_final/")
        self.up._install_zip(zp, install_dir)
        self.assertTrue((install_dir / "main.py").exists())
        self.assertFalse((install_dir / "edi_final").exists())

    def test_install_preserves_edi_config(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        (install_dir / "edi_config.json").write_text('{"original": true}')
        zp = self._make_zip({
            "main.py": "# new",
            "edi_config.json": '{"overwrite": true}',
        })
        self.up._install_zip(zp, install_dir)
        self.assertIn("original", (install_dir / "edi_config.json").read_text())

    def test_install_preserves_edi_store(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        (install_dir / "edi_store.json").write_text('{"orders":[]}')
        zp = self._make_zip({"edi_store.json": '{"orders":["hacked"]}'})
        self.up._install_zip(zp, install_dir)
        self.assertEqual((install_dir / "edi_store.json").read_text(), '{"orders":[]}')

    def test_install_preserves_token_cache(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        (install_dir / ".token_cache.json").write_text('{"token":"secret"}')
        zp = self._make_zip({".token_cache.json": '{"token":"stolen"}'})
        self.up._install_zip(zp, install_dir)
        self.assertIn("secret", (install_dir / ".token_cache.json").read_text())

    def test_install_preserves_joker_png(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        (install_dir / "joker.png").write_bytes(b"\x89PNG original")
        zp = self._make_zip({"joker.png": "overwrite"})
        self.up._install_zip(zp, install_dir)
        self.assertEqual((install_dir / "joker.png").read_bytes(), b"\x89PNG original")

    def test_install_creates_subdirs(self):
        # Используем два разных верхних уровня — тогда prefix не определяется
        # и структура папок сохраняется как есть.
        install_dir = self.base / "install"
        install_dir.mkdir()
        zp = self.base / "sub.zip"
        with __import__('zipfile').ZipFile(zp, "w") as zf:
            zf.writestr("subdir/module.py", "# module")
            zf.writestr("main.py",          "# main")   # второй top-level → prefix не применяется
        self.up._install_zip(zp, install_dir)
        self.assertTrue((install_dir / "subdir" / "module.py").exists())

    def test_install_invalid_zip_returns_false(self):
        install_dir = self.base / "install"
        install_dir.mkdir()
        bad = self.base / "bad.zip"
        bad.write_bytes(b"not a zip file at all")
        self.assertFalse(self.up._install_zip(bad, install_dir))

    # ── check_and_update ─────────────────────────────────────────────────────

    def test_no_update_when_versions_equal(self):
        self._set_version("1.0.0")
        remote = {"version": "1.0.0", "zip_url": "", "version_url": ""}
        with patch.object(self.up, "_remote_info", return_value=remote):
            self.up.check_and_update(silent=True)  # не должно падать или вызывать input

    def test_no_crash_when_no_network(self):
        self._set_version("1.0.0")
        with patch.object(self.up, "_remote_info", return_value=None):
            self.up.check_and_update(silent=True)

    def test_no_crash_without_version_file(self):
        self.up.VERSION_FILE = self.base / "no_version.json"
        self.up.check_and_update(silent=True)

    def test_user_declines_update(self):
        self._set_version("1.0.0")
        remote = {"version": "9.9.9", "zip_url": "https://x.com/z.zip",
                  "version_url": "https://x.com/v.json"}
        with patch.object(self.up, "_remote_info", return_value=remote):
            with patch("builtins.input", return_value="n"):
                # sys.exit НЕ должен вызываться
                self.up.check_and_update(silent=False)

    def test_update_installs_and_restarts(self):
        """Пользователь согласился — должен скачать, установить и перезапустить."""
        self._set_version("1.0.0")
        install_dir = self.base / "install"
        install_dir.mkdir()

        # Создаём ZIP-обновление
        zp = self._make_zip({"main.py": "# v2.0.0"})
        remote = {"version": "2.0.0", "zip_url": "https://x.com/z.zip",
                  "version_url": ""}

        with patch.object(self.up, "_remote_info", return_value=remote):
            with patch("builtins.input", return_value="y"):
                with patch.object(self.up, "_download",
                                  side_effect=lambda url, dest: dest.write_bytes(
                                      zp.read_bytes()) or True):
                    with patch.object(self.up, "_install_zip", return_value=True):
                        with patch("subprocess.Popen"):
                            with self.assertRaises(SystemExit):
                                self.up.check_and_update(silent=False)


# ─────────────────────────────────────────────────────────────────────────────
# Интеграционные тесты
# ─────────────────────────────────────────────────────────────────────────────

class TestIntegration(StoreIsolationMixin, unittest.TestCase):

    def setUp(self):
        self._setup_store()
        import recadv_builder, xml_builder
        self.rb = recadv_builder
        self.xb = xml_builder

    def tearDown(self):
        self._teardown_store()

    def test_full_document_cycle(self):
        """ORDERS → ORDRSP Accepted → DESADV → RECADV → purge."""
        # 1. Отправляем ORDERS
        oid = self._save_orders("ORD-CYCLE", xml=ORDERS_XML, msg="msg-1")
        o = self.store.get_order_by_id(oid)
        self.assertEqual(o["orders_status"], self.store.STATUS_PENDING)

        # 2. Приходит ORDRSP Accepted
        self.store.update_orders_status(oid, self.store.STATUS_ACCEPTED)
        self.assertEqual(
            self.store.get_order_by_id(oid)["orders_status"],
            self.store.STATUS_ACCEPTED
        )

        # 3. Прикрепляем DESADV
        did = self.store.attach_desadv(oid, "DESADV-001", "2025-03-16", DESADV_XML)
        self.assertIsNotNone(did)

        # 4. Парсим DESADV, генерируем RECADV
        desadv = self.rb.DesadvData(DESADV_XML)
        self.assertEqual(desadv.order_number, "ORD-TEST-001")  # номер из DESADV_XML
        accepted = [{
            "gtin": "0000001", "internal_buyer_code": "100",
            "description": "Товар тест", "despatched_qty": "10.000",
            "accepted_qty": "10.000", "uom": "PCE",
            "net_price": "50.00", "vat_rate": "20",
        }]
        recadv_xml, num = self.rb.build_recadv_xml(desadv, accepted, "RECV-CYCLE-01")

        # 5. Проверяем RECADV
        root = ET.fromstring(recadv_xml)
        ra = root.find("receivingAdvice")
        self.assertEqual(ra.find("originOrder").get("number"), "ORD-TEST-001")  # из DESADV_XML
        self.assertEqual(ra.find("despatchIdentificator").get("number"), "DESADV-001")

        # 6. Отмечаем RECADV отправленным
        self.store.mark_recadv_sent(oid, did)
        o = self.store.get_order_by_id(oid)
        self.assertTrue(o["desadv"][0]["recadv_sent"])

        # 7. Очищаем завершённые
        n = self.store.purge_completed_orders()
        self.assertEqual(n, 1)
        self.assertIsNone(self.store.get_order_by_id(oid))

    def test_strikethrough_only_when_all_recadv_sent(self):
        oid = self._save_orders("ORD-STRIKE")
        did1 = self.store.attach_desadv(oid, "D-001", "2025-03-16", "<d1/>")
        did2 = self.store.attach_desadv(oid, "D-002", "2025-03-17", "<d2/>")

        def _all_done():
            o = self.store.get_order_by_id(oid)
            desadvs = o.get("desadv", [])
            return len(desadvs) > 0 and all(d.get("recadv_sent") for d in desadvs)

        self.assertFalse(_all_done())
        self.store.mark_recadv_sent(oid, did1)
        self.assertFalse(_all_done())   # только один из двух
        self.store.mark_recadv_sent(oid, did2)
        self.assertTrue(_all_done())    # оба отправлены → зачёркивать

    def test_orders_and_pricat_xml_are_independent(self):
        """Генераторы ORDERS и PRICAT не влияют друг на друга."""
        item_o = {
            "gtin": "0000001", "internal_buyer_code": "100",
            "description": "Тест", "requested_quantity": "5.000",
            "unit_of_measure": "PCE", "net_price": "10.00", "vat_rate": "20",
        }
        item_p = {
            "gtin": "0000001", "internal_buyer_code": "100",
            "internal_supplier_code": "S001", "supplier_name": "ООО",
            "vat_rate": "20",
        }
        orders_xml, guid = self.xb.generate_orders_xml("111", "222", [item_o])
        pricat_xml, fn   = self.xb.generate_pricat_xml(1, "111", None, [item_p])

        root_o = ET.fromstring(orders_xml)
        root_p = ET.fromstring(pricat_xml)
        self.assertEqual(root_o.tag, "eDIMessage")
        self.assertEqual(root_p.tag, "pricat")

    def test_ordrsp_rejected_no_desadv_purge(self):
        """ORDERS Rejected без DESADV → purge_completed удаляет его."""
        oid = self._save_orders("ORD-REJECTED")
        self.store.update_orders_status(oid, self.store.STATUS_REJECTED)
        n = self.store.purge_completed_orders()
        self.assertEqual(n, 1)
        self.assertIsNone(self.store.get_order_by_id(oid))

    def test_multiple_orders_isolation(self):
        """Операции над одним ORDERS не затрагивают другие."""
        oid1 = self._save_orders("ORD-A")
        oid2 = self._save_orders("ORD-B")
        oid3 = self._save_orders("ORD-C")

        self.store.update_orders_status(oid1, self.store.STATUS_ACCEPTED)
        self.store.attach_desadv(oid2, "D-001", "2025-03-16", "<d/>")

        self.assertEqual(
            self.store.get_order_by_id(oid1)["orders_status"],
            self.store.STATUS_ACCEPTED
        )
        self.assertEqual(
            self.store.get_order_by_id(oid3)["orders_status"],
            self.store.STATUS_PENDING
        )
        self.assertEqual(len(self.store.get_order_by_id(oid2)["desadv"]), 1)
        self.assertEqual(len(self.store.get_order_by_id(oid1)["desadv"]), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Запуск
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    order = [
        TestConfig, TestAuth, TestXmlBuilder, TestRecadvBuilder,
        TestStore, TestApi, TestUpdater, TestIntegration,
    ]
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls in order:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    verbosity = 2 if "-v" in sys.argv else 1
    result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
