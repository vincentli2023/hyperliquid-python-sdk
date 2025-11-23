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
    if ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ws_msg["channel"] == "orderUpdates":
        return "orderUpdates"
    elif ws_msg["channel"] == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "bbo":
        return f'bbo:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetCtx" or ws_msg["channel"] == "activeSpotAssetCtx":
        return f'activeAssetCtx:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetData":
        return f'activeAssetData:{ws_msg["data"]["coin"].lower()},{ws_msg["data"]["user"].lower()}'
    else:
        return None


class WebsocketManager(threading.Thread):
    def __init__(self, base_url: str):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False

        # 连接建立前排队的订阅：[(subscription, ActiveSubscription), ...]
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []

        # identifier -> [ActiveSubscription, ...]
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)

        # subscription_id -> 原始 subscription（用于重连后 resubscribe）
        self.subscription_by_id: Dict[int, Subscription] = {}

        self.ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ws: websocket.WebSocketApp | None = None

        self.stop_event = threading.Event()
        # 可选：daemon 线程，主进程退出时自动结束
        self.daemon = True

        self._create_ws()

    # 创建一个新的 WebSocketApp 实例（用于首次连接 & 重连）
    def _create_ws(self) -> None:
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
        )

    def run(self) -> None:
        """主线程：自动重连 + 使用 ping_interval 保持心跳"""
        reconnect_delay = 5  # 秒
        while not self.stop_event.is_set():
            logging.info("Websocket connecting to %s ...", self.ws_url)
            self.ws_ready = False

            # 每次重连都新建一个 WebSocketApp 实例
            self._create_ws()

            try:
                # 内部会自动发 ping/pong，保持连接
                self.ws.run_forever(ping_interval=50, ping_timeout=10)
            except Exception as e:
                logging.warning("Websocket run_forever raised exception: %s", e)

            if self.stop_event.is_set():
                break

            logging.warning("Websocket connection lost, retrying in %s seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

        logging.info("Websocket thread exiting.")

    def stop(self) -> None:
        """主动关闭 websocket，并让线程退出"""
        self.stop_event.set()
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass

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

        # 1) 先处理连接建立前排队的订阅
        if self.queued_subscriptions:
            logging.debug("Flushing %d queued subscriptions", len(self.queued_subscriptions))
        for subscription, active_subscription in self.queued_subscriptions:
            # 这里调用 subscribe，会走正常逻辑：记录 active_subscriptions + 发 subscribe 请求
            self.subscribe(subscription, active_subscription.callback, active_subscription.subscription_id)
        self.queued_subscriptions.clear()

        # 2) 对已有的 active_subscriptions 做一次 resubscribe
        #    这里只重新发送 subscribe，不会重复 append active_subscriptions
        for identifier, active_list in self.active_subscriptions.items():
            for active in active_list:
                sub = self.subscription_by_id.get(active.subscription_id)
                if sub is None:
                    continue
                logging.debug(
                    "Resubscribing %s (subscription_id=%d)", identifier, active.subscription_id
                )
                try:
                    self.ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
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
        """注册订阅：
        - 如果 websocket 未 ready，则排队，等 on_open 时统一发
        - 如果已 ready，则立刻发送 subscribe 请求
        """
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter

        # 记录 subscription_id -> subscription，用于重连后 resubscribe
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
                # 这些 channel 目前协议上不能多次订阅（一个 user）
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")
            self.active_subscriptions[identifier].append(
                ActiveSubscription(callback, subscription_id)
            )
            try:
                self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
            except Exception as e:
                logging.warning("Failed to send subscribe for %s: %s", identifier, e)
        return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        """取消订阅：
        - 如果这是该 identifier 下最后一个订阅，则发 unsubscribe
        - 返回值表示是否确实删除了一个订阅
        """
        if not self.ws_ready:
            # 如果想支持在断线时本地先删，可以改掉这一行
            raise NotImplementedError("Can't unsubscribe before websocket connected")

        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [
            x for x in active_subscriptions if x.subscription_id != subscription_id
        ]

        if len(new_active_subscriptions) == 0 and len(active_subscriptions) > 0:
            # 这是最后一个订阅，通知服务器真正取消
            try:
                self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
            except Exception as e:
                logging.warning("Failed to send unsubscribe for %s: %s", identifier, e)

        self.active_subscriptions[identifier] = new_active_subscriptions

        # 如果确实删掉了订阅，则清理 mapping
        removed = len(active_subscriptions) != len(new_active_subscriptions)
        if removed:
            self.subscription_by_id.pop(subscription_id, None)

        return removed
