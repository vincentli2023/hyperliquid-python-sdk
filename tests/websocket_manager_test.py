import logging

from hyperliquid.websocket_manager import ActiveSubscription, WebsocketManager


def test_on_message_logs_pong_latency(monkeypatch, caplog):
    manager = WebsocketManager("http://localhost")
    manager.last_ping_sent_ts = 100.0
    monkeypatch.setattr("hyperliquid.websocket_manager.time.monotonic", lambda: 105.0)

    with caplog.at_level(logging.DEBUG):
        manager.on_message(None, '{"channel":"pong"}')

    assert manager.last_message_ts == 105.0
    assert manager.last_pong_ts == 105.0
    assert "Websocket received pong (after_ping=5.0s)" in caplog.text


def test_on_close_logs_connection_diagnostics(monkeypatch, caplog):
    manager = WebsocketManager("http://localhost")
    manager.connection_attempt = 3
    manager.last_open_ts = 100.0
    manager.last_message_ts = 145.0
    manager.last_ping_sent_ts = 148.0
    manager.last_pong_ts = 149.0
    manager.active_subscriptions["allMids"].append(ActiveSubscription(lambda _: None, 1))
    manager.queued_subscriptions.append(
        ({"type": "allMids"}, ActiveSubscription(lambda _: None, 2))
    )
    monkeypatch.setattr("hyperliquid.websocket_manager.time.monotonic", lambda: 150.0)

    with caplog.at_level(logging.WARNING):
        manager.on_close(None, 1000, "Expired")

    assert manager.last_close_status_code == 1000
    assert manager.last_close_msg == "Expired"
    assert "Websocket closed: code=1000, msg=Expired" in caplog.text
    assert "uptime=50.0s" in caplog.text
    assert "since_last_message=5.0s" in caplog.text
    assert "since_last_ping=2.0s" in caplog.text
    assert "since_last_pong=1.0s" in caplog.text
    assert "active_subscriptions=1" in caplog.text
    assert "queued_subscriptions=1" in caplog.text
    assert "stop_requested=False" in caplog.text


def test_disconnect_reason_summarizes_last_connection(monkeypatch):
    manager = WebsocketManager("http://localhost")
    manager.last_close_status_code = 1000
    manager.last_close_msg = "Expired"
    manager.last_error = "temporary network error"
    manager.last_run_exception = "socket closed"
    manager.last_message_ts = 140.0
    manager.last_pong_ts = 145.0
    monkeypatch.setattr("hyperliquid.websocket_manager.time.monotonic", lambda: 150.0)

    reason = manager._disconnect_reason()

    assert "close_code=1000, close_msg=Expired" in reason
    assert "last_error=temporary network error" in reason
    assert "run_exception=socket closed" in reason
    assert "since_last_message=10.0s" in reason
    assert "since_last_pong=5.0s" in reason
