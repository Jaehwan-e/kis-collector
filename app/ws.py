import asyncio
import json
import logging

import websockets

from .auth import AuthManager
from .config import settings
from .db import Database
from .parser import parse_message
from . import stats

logger = logging.getLogger(__name__)


class WSClient:
    def __init__(self, auth: AuthManager, db: Database):
        self._auth = auth
        self._db = db
        self._symbols = auth.account.symbols
        self._name = auth.account.name
        self._stop = False

    async def run(self):
        delay = 5
        while not self._stop:
            try:
                # 재접속 시 새 approval_key 발급
                await self._auth.force_issue_approval_key()
                approval_key = self._auth.approval_key

                async with websockets.connect(
                    settings.ws_url,
                    ping_interval=60,
                    ping_timeout=30,
                    close_timeout=5,
                ) as ws:
                    await self._subscribe(ws, approval_key)
                    delay = 5
                    await self._receive_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("[%s] WS 연결 끊김: %s / %ds 후 재접속", self._name, e, delay)
                from . import notify
                st = stats.get(self._name)
                st.ws_reconnects += 1
                st.errors += 1
                if delay >= 60:
                    await notify.send_error(f"[{self._name}] WS 재접속 반복", str(e)[:200])
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

        await self._db.flush()
        logger.info("[%s] WS 클라이언트 종료", self._name)

    async def _subscribe(self, ws, approval_key: str):
        for symbol in self._symbols:
            for tr_id in ("H0STASP0", "H0STCNT0"):
                msg = {
                    "header": {
                        "approval_key": approval_key,
                        "custtype": "P",
                        "tr_type": "1",
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {"tr_id": tr_id, "tr_key": symbol}
                    },
                }
                await ws.send(json.dumps(msg))
                logger.info("[%s] 구독 요청: %s / %s", self._name, tr_id, symbol)

    async def _receive_loop(self, ws):
        async for raw in ws:
            if self._stop:
                break
            parsed = parse_message(raw)
            if parsed is None:
                continue
            msg_type = parsed["_type"]
            st = stats.get(self._name)
            if msg_type == "trade":
                await self._db.add_trade(parsed)
                st.trade_count += 1
            elif msg_type == "orderbook":
                await self._db.add_orderbook(parsed)
                st.orderbook_count += 1

    def stop(self):
        self._stop = True
