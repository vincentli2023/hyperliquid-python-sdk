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

        self._create_ws()

        # ping 线程
        self.ping_interval_sec = 50
        self._ping_thread = threading.Thread(target=self.send_ping_loop, daemon=True)

    def _create_ws(self) -> None:
        logging.info("this is the latest verion")
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
        )

    def run(self) -> None:
        """主线程：自动重连 + 开 ping 线程"""
        if not self._ping_thread.is_alive():
            self._ping_thread.start()

        reconnect_delay = 5
        while not self.stop_event.is_set():
            logging.info("Websocket connecting to %s ...", self.ws_url)
            self.ws_ready = False

            self._create_ws()

            try:
                # 不用 websocket-client 的 ping_interval，避免和 HL 协议冲突
                self.ws.run_forever()
            except Exception as e:
                logging.warning("Websocket run_forever raised exception: %s", e)

            if self.stop_event.is_set():
                break

            logging.warning(
                "Websocket connection lost, retrying in %d seconds...", reconnect_delay
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

    def send_ping_loop(self) -> None:
        """独立线程定期发送 application-level ping"""
        while not self.stop_event.wait(self.ping_interval_sec):
            ws = self.ws
            if ws is None:
                continue
            try:
                # sock.connected 是 websocket-client 的底层连接状态
                if ws.sock and ws.sock.connected:
                    logging.debug("Websocket sending HL ping")
                    ws.send(json.dumps({"method": "ping"}))
            except Exception as e:
                logging.debug("Websocket ping failed: %s", e)

    # ---------- WebSocket callbacks ----------

    def on_message(self, _ws, message: str) -> None:
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        logging.debug("on_message %s", message)
        ws_msg: WsMsg = json.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        if identifier is None:
            logging.debug("Websocket not handling empty/unknown message")
            return
        active_subscriptions = self.active_subscriptions[identifier]
        if len(active_subscriptions) == 0:
            print("Websocket message from an unexpected subscription:", message, identifier)
        else:
            for active_subscription in active_subscriptions:
                active_subscription.callback(ws_msg)

    def on_open(self, _ws) -> None:
        logging.debug("on_open")
        self.ws_ready = True

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
        logging.warning("Websocket error: %s", error)

    def on_close(self, _ws, status_code, msg) -> None:
        logging.warning("Websocket closed: code=%s, msg=%s", status_code, msg)
        self.ws_ready = False

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
