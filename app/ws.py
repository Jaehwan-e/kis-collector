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
# 짧은 끊김은 빠르게 재시도, 반복 실패 시 점점 길게 쉼
# KIS 서버가 IP 차단한 경우 60초 간격 재시도는 차단을 풀지 못하므로
# 10분, 30분, 1시간, 3시간으로 충분히 쉬어 차단 해제를 기다림
BACKOFF_SCHEDULE = [5, 10, 20, 40, 60, 600, 1800, 3600, 10800]


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
                # 60초 이상 휴식 단계부터 텔레그램 알림
                if delay >= 60:
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
            # PINGPONG keepalive 무시
            if raw == "PINGPONG" or raw.strip() == "PINGPONG":
                continue
            # JSON 응답(구독 확인/에러)은 로깅
            if raw.startswith("{"):
                try:
                    resp = json.loads(raw)
                    header = resp.get("header", {})
                    tr_id = header.get("tr_id", "")
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
