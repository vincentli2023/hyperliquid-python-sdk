import json
import logging
import threading
import time
from collections import defaultdict

import websocket

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

ActiveSubscription = NamedTuple(
    "ActiveSubscription",
    [("callback", Callable[[Any], None]), ("subscription_id", int)],
)


def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "userFills":
        return f'userFills:{subscription["user"].lower()}'
    elif subscription["type"] == "candle":
        return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFundings":
        return f'userFundings:{subscription["user"].lower()}'
    elif subscription["type"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    elif subscription["type"] == "webData2":
        return f'webData2:{subscription["user"].lower()}'
    elif subscription["type"] == "bbo":
        return f'bbo:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetCtx":
        return f'activeAssetCtx:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetData":
        return f'activeAssetData:{subscription["coin"].lower()},{subscription["user"].lower()}'
    else:
        raise ValueError(f"Unknown subscription type: {subscription}")


def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    ch = ws_msg.get("channel")
    if ch == "pong":
        return "pong"
    elif ch == "allMids":
        return "allMids"
    elif ch == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ch == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ch == "user":
        return "userEvents"
    elif ch == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ch == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ch == "orderUpdates":
        return "orderUpdates"
    elif ch == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ch == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ch == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'
    elif ch == "bbo":
        return f'bbo:{ws_msg["data"]["coin"].lower()}'
    elif ch == "activeAssetCtx" or ch == "activeSpotAssetCtx":
        return f'activeAssetCtx:{ws_msg["data"]["coin"].lower()}'
    elif ch == "activeAssetData":
        return f'activeAssetData:{ws_msg["data"]["coin"].lower()},{ws_msg["data"]["user"].lower()}'
    else:
        return None


class WebsocketManager(threading.Thread):
    def __init__(self, base_url: str):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.connection_attempt = 0

        # 连接建立前排队的订阅
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []

        # identifier -> [ActiveSubscription, ...]
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)

        # subscription_id -> 原始 subscription（用于重连后 resubscribe）
        self.subscription_by_id: Dict[int, Subscription] = {}

        self.ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ws: websocket.WebSocketApp | None = None

        self.stop_event = threading.Event()
        self.daemon = True

        self.last_open_ts: float | None = None
        self.last_message_ts: float | None = None
        self.last_ping_sent_ts: float | None = None
        self.last_pong_ts: float | None = None
        self.last_close_ts: float | None = None
        self.last_close_status_code: int | None = None
        self.last_close_msg: str | None = None
        self.last_error: str | None = None
        self.last_run_exception: str | None = None

        self._create_ws()

        # ping / watchdog 配置
        self.ping_interval_sec = 20
        self.pong_timeout_sec = 10
        self.message_staleness_sec = 60
        self._ping_thread = threading.Thread(target=self.send_ping_loop, daemon=True)

    def _create_ws(self) -> None:
        logging.debug("Creating websocket client for %s", self.ws_url)
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
        )

    def _active_subscription_count(self) -> int:
        return sum(len(active_list) for active_list in self.active_subscriptions.values())

    def _format_elapsed(self, ts: float | None, now: float | None = None) -> str:
        if ts is None:
            return "n/a"
        if now is None:
            now = time.monotonic()
        return f"{max(now - ts, 0.0):.1f}s"

    def _reset_connection_diagnostics(self) -> None:
        self.last_open_ts = None
        self.last_message_ts = None
        self.last_ping_sent_ts = None
        self.last_pong_ts = None
        self.last_close_ts = None
        self.last_close_status_code = None
        self.last_close_msg = None
        self.last_error = None
        self.last_run_exception = None

    def _disconnect_reason(self) -> str:
        now = time.monotonic()
        details = []
        if self.last_close_status_code is not None or self.last_close_msg is not None:
            details.append(
                f"close_code={self.last_close_status_code}, close_msg={self.last_close_msg or ''}"
            )
        if self.last_error is not None:
            details.append(f"last_error={self.last_error}")
        if self.last_run_exception is not None:
            details.append(f"run_exception={self.last_run_exception}")
        details.append(f"since_last_message={self._format_elapsed(self.last_message_ts, now)}")
        details.append(f"since_last_pong={self._format_elapsed(self.last_pong_ts, now)}")
        return ", ".join(details)

    def run(self) -> None:
        """主线程：自动重连 + 开 ping 线程"""
        if not self._ping_thread.is_alive():
            self._ping_thread.start()

        reconnect_delay = 5
        while not self.stop_event.is_set():
            self.connection_attempt += 1
            self._reset_connection_diagnostics()
            self.ws_ready = False
            logging.info(
                "Websocket connecting to %s (attempt=%d, queued_subscriptions=%d, active_subscriptions=%d)",
                self.ws_url,
                self.connection_attempt,
                len(self.queued_subscriptions),
                self._active_subscription_count(),
            )

            self._create_ws()

            try:
                # 不用 websocket-client 的 ping_interval，避免和 HL 协议冲突
                self.ws.run_forever()
            except Exception as e:
                self.last_run_exception = str(e)
                logging.warning("Websocket run_forever raised exception: %s", e)

            if self.stop_event.is_set():
                break

            logging.warning(
                "Websocket connection lost (%s), retrying in %d seconds...",
                self._disconnect_reason(),
                reconnect_delay,
            )
            time.sleep(reconnect_delay)

        logging.info("Websocket thread exiting.")

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass

    # ---------- ping loop：按 HL 协议发 {"method":"ping"} ----------

    def _force_close_ws(self, reason: str) -> None:
        """主动关闭当前 ws，触发 run() 里的重连逻辑"""
        logging.warning("Websocket force-closing: %s", reason)
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def send_ping_loop(self) -> None:
        """独立线程：定期发送 ping + 检查 pong 超时 + 检查消息活性"""
        while not self.stop_event.wait(self.ping_interval_sec):
            ws = self.ws
            if ws is None or not self.ws_ready:
                continue

            now = time.monotonic()

            # --- pong 超时检测 ---
            if (
                self.last_ping_sent_ts is not None
                and self.last_pong_ts is not None
                and self.last_ping_sent_ts > self.last_pong_ts
                and (now - self.last_ping_sent_ts) > self.pong_timeout_sec
            ):
                self._force_close_ws(
                    f"pong timeout: sent ping {self._format_elapsed(self.last_ping_sent_ts, now)} ago, "
                    f"last pong {self._format_elapsed(self.last_pong_ts, now)} ago"
                )
                continue

            # --- 消息活性检测（有订阅但长时间没收到任何消息）---
            if (
                self._active_subscription_count() > 0
                and self.last_message_ts is not None
                and (now - self.last_message_ts) > self.message_staleness_sec
            ):
                self._force_close_ws(
                    f"message staleness: no message for {self._format_elapsed(self.last_message_ts, now)}, "
                    f"active_subscriptions={self._active_subscription_count()}"
                )
                continue

            # --- 发送 ping ---
            try:
                if ws.sock and ws.sock.connected:
                    ping_ts = time.monotonic()
                    self.last_ping_sent_ts = ping_ts
                    logging.debug(
                        "Websocket sending HL ping (since_last_message=%s, since_last_pong=%s)",
                        self._format_elapsed(self.last_message_ts, ping_ts),
                        self._format_elapsed(self.last_pong_ts, ping_ts),
                    )
                    ws.send(json.dumps({"method": "ping"}))
                else:
                    logging.debug("Websocket ping skipped because socket is not connected")
            except Exception as e:
                logging.debug("Websocket ping failed: %s", e)

    # ---------- WebSocket callbacks ----------

    def on_message(self, _ws, message: str) -> None:
        now = time.monotonic()
        self.last_message_ts = now
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        logging.debug("on_message %s", message)
        ws_msg: WsMsg = json.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            self.last_pong_ts = now
            logging.debug(
                "Websocket received pong (after_ping=%s)",
                self._format_elapsed(self.last_ping_sent_ts, now),
            )
            return
        if identifier is None:
            logging.debug("Websocket not handling empty/unknown message")
            return
        active_subscriptions = self.active_subscriptions[identifier]
        if len(active_subscriptions) == 0:
            logging.warning(
                "Websocket message from an unexpected subscription: identifier=%s payload=%s",
                identifier,
                message,
            )
        else:
            for active_subscription in active_subscriptions:
                active_subscription.callback(ws_msg)

    def on_open(self, _ws) -> None:
        self.last_open_ts = time.monotonic()
        self.ws_ready = True
        logging.info(
            "Websocket opened (attempt=%d, ping_interval=%ss, queued_subscriptions=%d, active_subscriptions=%d)",
            self.connection_attempt,
            self.ping_interval_sec,
            len(self.queued_subscriptions),
            self._active_subscription_count(),
        )

        # 1) flush queued_subscriptions
        if self.queued_subscriptions:
            logging.debug(
                "Flushing %d queued subscriptions", len(self.queued_subscriptions)
            )
        for subscription, active_subscription in self.queued_subscriptions:
            self.subscribe(
                subscription,
                active_subscription.callback,
                active_subscription.subscription_id,
            )
        self.queued_subscriptions.clear()

        # 2) 对已有 active_subscriptions 做 resubscribe（只发请求，不改本地结构）
        for identifier, active_list in self.active_subscriptions.items():
            for active in active_list:
                sub = self.subscription_by_id.get(active.subscription_id)
                if sub is None:
                    continue
                logging.debug(
                    "Resubscribing %s (subscription_id=%d)", identifier, active.subscription_id
                )
                try:
                    self.ws.send(
                        json.dumps({"method": "subscribe", "subscription": sub})
                    )
                except Exception as e:
                    logging.warning("Failed to resubscribe %s: %s", identifier, e)

    def on_error(self, _ws, error) -> None:
        self.last_error = str(error)
        logging.warning("Websocket error: %s", error)

    def on_close(self, _ws, status_code, msg) -> None:
        now = time.monotonic()
        self.last_close_ts = now
        self.last_close_status_code = status_code
        self.last_close_msg = msg
        self.ws_ready = False
        logging.warning(
            (
                "Websocket closed: code=%s, msg=%s, uptime=%s, since_last_message=%s, "
                "since_last_ping=%s, since_last_pong=%s, active_subscriptions=%d, "
                "queued_subscriptions=%d, stop_requested=%s"
            ),
            status_code,
            msg,
            self._format_elapsed(self.last_open_ts, now),
            self._format_elapsed(self.last_message_ts, now),
            self._format_elapsed(self.last_ping_sent_ts, now),
            self._format_elapsed(self.last_pong_ts, now),
            self._active_subscription_count(),
            len(self.queued_subscriptions),
            self.stop_event.is_set(),
        )

    # ---------- Subscription management ----------

    def subscribe(
        self,
        subscription: Subscription,
        callback: Callable[[Any], None],
        subscription_id: Optional[int] = None,
    ) -> int:
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter

        # 记录 id -> subscription，为重连时 resubscribe 用
        self.subscription_by_id[subscription_id] = subscription

        if not self.ws_ready:
            logging.debug("enqueueing subscription (id=%d)", subscription_id)
            self.queued_subscriptions.append(
                (subscription, ActiveSubscription(callback, subscription_id))
            )
        else:
            logging.debug("subscribing (id=%d)", subscription_id)
            identifier = subscription_to_identifier(subscription)
            if identifier in ("userEvents", "orderUpdates"):
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError(
                        f"Cannot subscribe to {identifier} multiple times"
                    )
            self.active_subscriptions[identifier].append(
                ActiveSubscription(callback, subscription_id)
            )
            try:
                self.ws.send(
                    json.dumps({"method": "subscribe", "subscription": subscription})
                )
            except Exception as e:
                logging.warning("Failed to send subscribe for %s: %s", identifier, e)
        return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        if not self.ws_ready:
            # 如果想支持离线时本地先删，可以放宽这个限制
            raise NotImplementedError("Can't unsubscribe before websocket connected")

        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [
            x for x in active_subscriptions if x.subscription_id != subscription_id
        ]

        if len(new_active_subscriptions) == 0 and len(active_subscriptions) > 0:
            try:
                self.ws.send(
                    json.dumps({"method": "unsubscribe", "subscription": subscription})
                )
            except Exception as e:
                logging.warning("Failed to send unsubscribe for %s: %s", identifier, e)

        self.active_subscriptions[identifier] = new_active_subscriptions

        removed = len(active_subscriptions) != len(new_active_subscriptions)
        if removed:
            self.subscription_by_id.pop(subscription_id, None)

        return removed
