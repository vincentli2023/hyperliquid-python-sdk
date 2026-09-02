import json
import logging
import threading
import time

import eth_account
import pytest

from hyperliquid.exchange import Exchange
from hyperliquid.utils.error import ClientError, WebsocketPostError
from hyperliquid.utils.types import Meta, SpotMeta
from hyperliquid.websocket_manager import ActiveSubscription, WebsocketManager

TEST_META: Meta = {"universe": [{"name": "BTC", "szDecimals": 5}]}
TEST_SPOT_META: SpotMeta = {"universe": [], "tokens": []}
WALLET = eth_account.Account.from_key("0x0123456789012345678901234567890123456789012345678901234567890123")


class FakeWs:
    def __init__(self, fail_send=False):
        self.sent = []
        self.fail_send = fail_send

    def send(self, message):
        if self.fail_send:
            raise OSError("socket gone")
        self.sent.append(json.loads(message))

    def close(self):
        pass


def _ready_manager(fail_send=False):
    manager = WebsocketManager("http://localhost")
    manager.ws = FakeWs(fail_send)
    manager.ws_ready = True
    return manager


def _post_in_thread(manager, request, timeout=1.0):
    box = {}

    def run():
        try:
            box["result"] = manager.post(request, timeout=timeout)
        except Exception as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=run)
    t.start()
    deadline = time.time() + 1.0
    while not manager.ws.sent and time.time() < deadline:
        time.sleep(0.005)
    return t, box


def test_post_roundtrip_routes_reply_by_id():
    manager = _ready_manager()
    t, box = _post_in_thread(manager, {"type": "action", "payload": {"x": 1}})
    sent = manager.ws.sent[0]
    assert sent == {"method": "post", "id": 1, "request": {"type": "action", "payload": {"x": 1}}}
    manager.on_message(None, json.dumps({"channel": "post", "data": {"id": 1, "response": {"type": "action", "payload": {"status": "ok"}}}}))
    t.join(1.0)
    assert box["result"] == {"type": "action", "payload": {"status": "ok"}}
    assert manager._pending_posts == {}


def test_post_ids_increment_and_do_not_cross_talk():
    manager = _ready_manager()
    t1, box1 = _post_in_thread(manager, {"type": "info", "payload": {"n": 1}})
    t2, box2 = _post_in_thread(manager, {"type": "info", "payload": {"n": 2}})
    while len(manager.ws.sent) < 2:
        time.sleep(0.005)
    ids = sorted(m["id"] for m in manager.ws.sent)
    assert ids == [1, 2]
    manager.on_message(None, json.dumps({"channel": "post", "data": {"id": 2, "response": {"type": "info", "payload": "two"}}}))
    manager.on_message(None, json.dumps({"channel": "post", "data": {"id": 1, "response": {"type": "info", "payload": "one"}}}))
    t1.join(1.0)
    t2.join(1.0)
    assert box1["result"] == {"type": "info", "payload": "one"}
    assert box2["result"] == {"type": "info", "payload": "two"}


def test_post_timeout_raises_and_cleans_up():
    manager = _ready_manager()
    with pytest.raises(WebsocketPostError, match="timed out"):
        manager.post({"type": "info", "payload": {}}, timeout=0.05)
    assert manager._pending_posts == {}


def test_post_fails_fast_on_close_without_resend():
    manager = _ready_manager()
    t, box = _post_in_thread(manager, {"type": "action", "payload": {}}, timeout=5.0)
    manager.on_close(None, 1006, "gone")
    t.join(1.0)
    assert isinstance(box["error"], WebsocketPostError)
    assert "websocket closed" in str(box["error"])
    assert len(manager.ws.sent) == 1  # exactly one send, no retry
    assert manager._pending_posts == {}


def test_post_requires_ready_socket():
    manager = WebsocketManager("http://localhost")
    manager.ws = FakeWs()
    manager.ws_ready = False
    with pytest.raises(WebsocketPostError, match="not ready"):
        manager.post({"type": "info", "payload": {}})
    assert manager.ws.sent == []


def test_post_send_failure_raises():
    manager = _ready_manager(fail_send=True)
    with pytest.raises(WebsocketPostError, match="send failed"):
        manager.post({"type": "info", "payload": {}})
    assert manager._pending_posts == {}


def test_unknown_post_reply_is_logged_not_raised(caplog):
    manager = _ready_manager()
    with caplog.at_level(logging.WARNING):
        manager.on_message(None, json.dumps({"channel": "post", "data": {"id": 99, "response": {"type": "error", "payload": "x"}}}))
    assert "unknown id=99" in caplog.text


def test_post_frames_do_not_reach_subscriptions():
    manager = _ready_manager()
    got = []
    manager.active_subscriptions["allMids"].append(ActiveSubscription(lambda m: got.append(m), 1))
    manager.on_message(None, json.dumps({"channel": "post", "data": {"id": 5, "response": {}}}))
    manager.on_message(None, json.dumps({"channel": "allMids", "data": {"mids": {}}}))
    assert len(got) == 1 and got[0]["channel"] == "allMids"


# ---------- Exchange routing ----------


class FakeManager:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def post(self, request, timeout=5.0):
        self.requests.append(request)
        return self.response


def _exchange(**kwargs):
    ex = Exchange(WALLET, meta=TEST_META, spot_meta=TEST_SPOT_META, **kwargs)
    return ex


def test_exchange_default_still_posts_http(monkeypatch):
    ex = _exchange()
    assert ex.ws_manager is None
    seen = {}
    monkeypatch.setattr(Exchange, "post", lambda self, path, payload=None: seen.update(path=path, payload=payload) or {"status": "ok"})
    assert ex.noop(1700000000000) == {"status": "ok"}
    assert seen["path"] == "/exchange"
    assert set(seen["payload"]) == {"action", "nonce", "signature", "vaultAddress", "expiresAfter"}


def test_exchange_ws_manager_sends_same_payload_over_websocket(monkeypatch):
    http_seen = {}
    monkeypatch.setattr(Exchange, "post", lambda self, path, payload=None: http_seen.update(path=path, payload=payload))
    manager = FakeManager({"type": "action", "payload": {"status": "ok", "response": {"type": "default"}}})
    ex = _exchange(ws_manager=manager)
    result = ex.noop(1700000000000)
    assert result == {"status": "ok", "response": {"type": "default"}}
    assert http_seen == {}  # nothing went over HTTP
    request = manager.requests[0]
    assert request["type"] == "action"
    assert set(request["payload"]) == {"action", "nonce", "signature", "vaultAddress", "expiresAfter"}
    assert request["payload"]["action"] == {"type": "noop"}
    assert request["payload"]["nonce"] == 1700000000000


def test_exchange_ws_error_reply_maps_to_client_error():
    ex = _exchange(ws_manager=FakeManager({"type": "error", "payload": "Invalid nonce"}))
    with pytest.raises(ClientError) as info:
        ex.noop(1700000000000)
    assert info.value.error_message == "Invalid nonce"


def test_exchange_ws_transport_error_propagates_unchanged():
    class Broken:
        def post(self, request, timeout=5.0):
            raise WebsocketPostError("post id=1 timed out after 5.0s")

    ex = _exchange(ws_manager=Broken())
    with pytest.raises(WebsocketPostError):
        ex.noop(1700000000000)
