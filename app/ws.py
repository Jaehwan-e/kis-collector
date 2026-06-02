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

# WS 재접속 백오프 스케줄 (초)
# WS 태스크는 WS_START(08:30)~WS_END(15:30) 세션 내에서만 동작하고
# WS_END에 cancel되므로 긴 휴식(분/시간 단위)은 살아있는 세션 시간만 깎아먹음.
# 빠른 재시도 후 60초 plateau로 고정 — over-shoot로 인한 학습 데이터 손실 방지.
BACKOFF_SCHEDULE = [5, 10, 20, 40, 60]
# 60초 plateau 진입 후 알림 throttle: 진입 1회 + N번마다 재알림
ALERT_THROTTLE_EVERY = 10


class WSClient:
    def __init__(self, auth: AuthManager, db: Database):
        self._auth = auth
        self._db = db
        self._symbols = auth.account.symbols
        self._name = auth.account.name
        self._stop = False

    async def run(self):
        attempt = 0  # 연속 실패 횟수 (백오프 스케줄 인덱스)
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
                    attempt = 0  # 연결 성공 시 백오프 리셋
                    await self._receive_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as e:
                idx = min(attempt, len(BACKOFF_SCHEDULE) - 1)
                delay = BACKOFF_SCHEDULE[idx]
                logger.warning("[%s] WS 연결 끊김: %s / %ds 후 재접속 (시도 %d회차)",
                               self._name, e, delay, attempt + 1)
                from . import notify
                st = stats.get(self._name)
                st.ws_reconnects += 1
                st.errors += 1
                # 60초 plateau 진입 후 알림: 첫 진입 + ALERT_THROTTLE_EVERY번마다
                if delay == 60:
                    plateau_count = attempt - (len(BACKOFF_SCHEDULE) - 1)
                    if plateau_count == 0 or plateau_count % ALERT_THROTTLE_EVERY == 0:
                        await notify.send_error(
                            f"[{self._name}] WS 재접속 반복 ({attempt + 1}회)",
                            f"{delay}초 휴식 후 재시도 | {str(e)[:150]}"
                        )
                await asyncio.sleep(delay)
                attempt += 1

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
            # PINGPONG keepalive 무시 (문자열 or JSON 형태 둘 다)
            if raw == "PINGPONG" or raw.strip() == "PINGPONG":
                continue
            # JSON 응답(구독 확인/에러)은 로깅
            if raw.startswith("{"):
                try:
                    resp = json.loads(raw)
                    header = resp.get("header", {})
                    tr_id = header.get("tr_id", "")
                    # JSON 형태 PINGPONG 무시
                    if tr_id == "PINGPONG":
                        continue
                    msg_cd = resp.get("body", {}).get("rt_cd", "")
                    msg1 = resp.get("body", {}).get("msg1", "")
                    if msg_cd != "0":
                        logger.warning("[%s] 구독 응답 실패: %s %s %s", self._name, tr_id, msg_cd, msg1)
                    else:
                        logger.debug("[%s] 구독 응답: %s %s", self._name, tr_id, msg1)
                except Exception:
                    pass
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
