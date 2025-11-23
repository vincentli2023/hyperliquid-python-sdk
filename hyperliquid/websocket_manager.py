import json
import logging
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

import websocket

# 保持你原有的引用，确保你的环境中安装了 hyperliquid 包
try:
    from hyperliquid.utils.types import Subscription, WsMsg
except ImportError:
    # 如果只是为了测试运行，这里提供简单的 Mock 类型
    Subscription = Dict[str, Any]
    WsMsg = Dict[str, Any]

# 定义 ActiveSubscription 结构
ActiveSubscription = NamedTuple(
    "ActiveSubscription",
    [("callback", Callable[[Any], None]), ("subscription_id", int)],
)

# ---------------------------------------------------------
# Helper Functions (保持原有的逻辑不变)
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Enhanced Websocket Manager
# ---------------------------------------------------------

class WebsocketManager(threading.Thread):
    def __init__(self, base_url: str):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False
        
        # 线程锁：保护共享数据 (active_subscriptions, queued_subscriptions 等)
        self.lock = threading.Lock()

        # 连接建立前排队的订阅
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []

        # identifier -> [ActiveSubscription, ...]
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)

        # subscription_id -> 原始 subscription（用于重连后 resubscribe）
        self.subscription_by_id: Dict[int, Subscription] = {}

        # 构造 WS URL
        if "http" in base_url:
            self.ws_url = "ws" + base_url[len("http") :] + "/ws"
        else:
            self.ws_url = base_url # Fallback if user passes partial url

        self.ws: websocket.WebSocketApp | None = None

        # 控制变量
        self.stop_event = threading.Event()
        self.daemon = True

        # Watchdog & Ping 配置
        self.ping_interval_sec = 50
        self.watchdog_timeout_sec = 60  # 如果60秒没收到任何消息，认为断连
        self.last_msg_time = time.time()
        self._last_ping_time = 0
        
        self._create_ws()

        # 启动 Ping 线程
        self._ping_thread = threading.Thread(target=self.send_ping_loop, daemon=True)

    def _create_ws(self) -> None:
        logging.info(f"Initializing WebsocketApp for {self.ws_url}")
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
        )

    def run(self) -> None:
        """主线程：负责维持连接循环 (Reconnection Loop)"""
        if not self._ping_thread.is_alive():
            self._ping_thread.start()

        reconnect_delay = 5
        
        while not self.stop_event.is_set():
            logging.info(f"Websocket connecting to {self.ws_url} ...")
            self.ws_ready = False
            self.last_msg_time = time.time() # 重置计时器，给连接过程一些缓冲时间

            self._create_ws()

            try:
                # 关键点：禁用 websocket-client 内部 ping，完全由 send_ping_loop 控制
                self.ws.run_forever(ping_interval=0)
            except Exception as e:
                logging.warning(f"Websocket run_forever raised exception: {e}")

            if self.stop_event.is_set():
                break

            logging.warning(
                f"Websocket connection lost, retrying in {reconnect_delay} seconds..."
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

    # ---------- Ping & Watchdog Loop ----------

    def send_ping_loop(self) -> None:
        """
        独立线程：
        1. 定期发送应用层 Ping ({"method": "ping"})
        2. 监控 last_msg_time，如果超时未收到数据，强制断开连接触发重连
        """
        while not self.stop_event.wait(5):  # 每5秒唤醒一次检查状态
            if self.stop_event.is_set():
                break
            
            current_time = time.time()

            # 1. 看门狗检测 (Watchdog Check)
            # 只有当连接应该是 active 的时候才检查
            if self.ws_ready and (current_time - self.last_msg_time > self.watchdog_timeout_sec):
                logging.error(
                    f"Watchdog: No message received for {current_time - self.last_msg_time:.1f}s. "
                    "Closing connection to force reconnect."
                )
                if self.ws:
                    try:
                        self.ws.close()
                    except Exception:
                        pass
                continue

            # 2. 发送 Ping
            if current_time - self._last_ping_time > self.ping_interval_sec:
                ws = self.ws
                if ws and ws.sock and ws.sock.connected:
                    try:
                        logging.debug("Websocket sending HL ping")
                        ws.send(json.dumps({"method": "ping"}))
                        self._last_ping_time = current_time
                    except Exception as e:
                        logging.debug(f"Websocket ping failed: {e}")

    # ---------- WebSocket callbacks ----------

    def on_message(self, _ws, message: str) -> None:
        # 收到任何消息（包括 Pong），更新看门狗时间
        self.last_msg_time = time.time()

        if message == "Websocket connection established.":
            logging.debug(message)
            return

        try:
            ws_msg: WsMsg = json.loads(message)
        except json.JSONDecodeError:
            logging.warning(f"Failed to decode JSON: {message}")
            return

        identifier = ws_msg_to_identifier(ws_msg)
        
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        
        if identifier is None:
            logging.debug("Websocket ignored message with no identifier")
            return
        
        # 线程安全：获取回调列表的副本
        with self.lock:
            # 复制一份 list，防止在遍历期间被 subscribe/unsubscribe 修改
            active_subscriptions = list(self.active_subscriptions[identifier])

        if len(active_subscriptions) == 0:
            # 可能是刚刚取消订阅，或者是未知的消息
            return

        for active_subscription in active_subscriptions:
            try:
                active_subscription.callback(ws_msg)
            except Exception as e:
                # 隔离异常：不要让用户回调的错误导致 WS 断开
                logging.error(f"Callback error for {identifier}: {e}", exc_info=True)

    def on_open(self, _ws) -> None:
        logging.info("Websocket on_open: Connection established.")
        self.ws_ready = True
        
        # 立即更新一次时间，避免刚连上就被看门狗杀掉
        self.last_msg_time = time.time()

        with self.lock:
            # 1. 重新订阅已有的 (Resubscribe existing)
            for identifier, active_list in self.active_subscriptions.items():
                if not active_list:
                    continue
                
                # 取第一个有效的 subscription 配置进行重连
                first_sub_id = active_list[0].subscription_id
                sub = self.subscription_by_id.get(first_sub_id)
                
                if sub:
                    logging.info(f"Resubscribing to {identifier} (id={first_sub_id})")
                    self._safe_send({"method": "subscribe", "subscription": sub})

            # 2. 处理排队中的新订阅 (Flush queued)
            if self.queued_subscriptions:
                logging.info(f"Flushing {len(self.queued_subscriptions)} queued subscriptions")
                
            for subscription, active_subscription in self.queued_subscriptions:
                self._subscribe_internal(subscription, active_subscription)
            
            self.queued_subscriptions.clear()

    def on_error(self, _ws, error) -> None:
        logging.warning(f"Websocket error: {error}")

    def on_close(self, _ws, status_code, msg) -> None:
        logging.warning(f"Websocket closed: code={status_code}, msg={msg}")
        self.ws_ready = False

    def _safe_send(self, data: Dict[str, Any]) -> None:
        """辅助函数：安全发送数据，捕获网络异常"""
        try:
            self.ws.send(json.dumps(data))
        except Exception as e:
            logging.warning(f"Failed to send WS message: {e}")

    # ---------- Subscription management ----------

    def _subscribe_internal(self, subscription: Subscription, active_sub: ActiveSubscription):
        """内部方法：处理实际的订阅逻辑（需在锁内调用）"""
        identifier = subscription_to_identifier(subscription)
        
        # 检查独占类型
        if identifier in ("userEvents", "orderUpdates"):
            if len(self.active_subscriptions[identifier]) != 0:
                # 如果是队列flush过来的，这里打个日志忽略，避免崩溃
                logging.warning(f"Cannot subscribe to {identifier} multiple times, skipping.")
                return

        self.active_subscriptions[identifier].append(active_sub)
        self._safe_send({"method": "subscribe", "subscription": subscription})

    def subscribe(
        self,
        subscription: Subscription,
        callback: Callable[[Any], None],
        subscription_id: Optional[int] = None,
    ) -> int:
        with self.lock:
            if subscription_id is None:
                self.subscription_id_counter += 1
                subscription_id = self.subscription_id_counter

            self.subscription_by_id[subscription_id] = subscription
            active_sub = ActiveSubscription(callback, subscription_id)

            if not self.ws_ready:
                logging.debug(f"Enqueueing subscription (id={subscription_id})")
                self.queued_subscriptions.append((subscription, active_sub))
            else:
                logging.debug(f"Subscribing immediately (id={subscription_id})")
                self._subscribe_internal(subscription, active_sub)

            return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        identifier = subscription_to_identifier(subscription)
        
        with self.lock:
            if identifier not in self.active_subscriptions:
                return False
                
            active_subscriptions = self.active_subscriptions[identifier]
            new_active_subscriptions = [
                x for x in active_subscriptions if x.subscription_id != subscription_id
            ]

            removed = len(active_subscriptions) != len(new_active_subscriptions)
            self.active_subscriptions[identifier] = new_active_subscriptions

            if removed:
                self.subscription_by_id.pop(subscription_id, None)

            # 如果该 identifier 下已经没有订阅者了，尝试发送取消订阅指令
            # 即使此时 WS 断开，我们也只是清理了本地状态，下次连上不会再订
            if len(new_active_subscriptions) == 0 and self.ws_ready:
                try:
                    self._safe_send({"method": "unsubscribe", "subscription": subscription})
                except Exception as e:
                    logging.warning(f"Failed to send unsubscribe for {identifier}: {e}")

            return removed

# ---------------------------------------------------------
# 使用示例 (Main)
# ---------------------------------------------------------
if __name__ == "__main__":
    # 简单的配置日志，方便查看效果
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )

    base_url = "https://api.hyperliquid.xyz" 
    # 如果是测试网可以用: "https://api.hyperliquid-testnet.xyz"
    
    ws_manager = WebsocketManager(base_url)
    ws_manager.start() # 启动线程

    # 定义一个回调打印数据
    def my_callback(msg):
        print(f"Update received: {msg['channel']}")

    # 模拟订阅 L2 Book
    sub_l2 = {"type": "l2Book", "coin": "BTC"}
    ws_manager.subscribe(sub_l2, my_callback)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        ws_manager.stop()
