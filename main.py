import asyncio
import datetime
import logging
import signal

from auth import AuthManager
from config import settings
from db import Database
from rest import RESTPoller
from ws import WSClient

logger = logging.getLogger(__name__)

KST = datetime.timezone(datetime.timedelta(hours=9))

# 장 운영 시간
PRE_MARKET = datetime.time(8, 0)    # 토큰 발급 + 휴장일 조회 + daily_base 수집
WS_START = datetime.time(8, 30)     # WS + REST 시작
WS_END = datetime.time(15, 30)      # WS + REST 종료
POST_MARKET = datetime.time(15, 35) # daily_investor 수집


async def _wait_until(target: datetime.time):
    """목표 시각까지 대기 — 30초 간격으로 시각 확인 (슬립 복귀 대응)"""
    while True:
        now = datetime.datetime.now(KST).time()
        if now >= target:
            return
        await asyncio.sleep(30)


async def _run_market_session(db: Database, auth: AuthManager):
    """하루 장중 세션: 08:00 ~ 15:35"""
    rest = RESTPoller(auth, db)
    ws = WSClient(auth, db)

    # 1) 장 시작 전: 토큰 발급 + 휴장일 체크 + daily_base 수집
    await _wait_until(PRE_MARKET)

    logger.info("토큰 발급")
    await auth.ensure_tokens()

    if not await rest.is_market_open():
        logger.info("오늘 휴장 — 수집 스킵")
        await rest.close()
        return

    logger.info("일별 시세(daily_base) 수집")
    await rest.poll_daily_base()

    # 2) WS 시작 시간 대기
    await _wait_until(WS_START)

    # 3) 장중: WS + REST 폴링 + 주기적 플러시
    logger.info("장중 데이터 수집 시작")

    ws_task = asyncio.create_task(ws.run())
    flush_task = asyncio.create_task(_flush_loop(db))
    rest_task = asyncio.create_task(rest.poll_member())

    tasks = [ws_task, flush_task, rest_task]

    try:
        await _wait_until(WS_END)
    finally:
        ws.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await db.flush()
    logger.info("장중 수집 종료, 버퍼 플러시 완료")

    # 4) 장 마감 후 daily_investor 수집
    await _wait_until(POST_MARKET)

    logger.info("일별 투자자(daily_investor) 수집")
    await rest.poll_daily_investor()

    await rest.close()


async def _flush_loop(db: Database):
    while True:
        await asyncio.sleep(settings.flush_interval)
        await db.flush()


async def main():
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db = Database()
    await db.init()
    auth = AuthManager()

    # 모든 실행 중인 태스크를 추적
    _all_tasks: list[asyncio.Task] = []

    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("종료 시그널 수신")
        # 현재 이벤트 루프의 모든 태스크 취소
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    try:
        while True:
            now = datetime.datetime.now(KST)

            # 장 마감 이후라면 다음날 08:00까지 30초 간격 대기
            if now.time() >= POST_MARKET:
                logger.info("장 종료 — 다음 영업일까지 대기")
                while datetime.datetime.now(KST).time() >= POST_MARKET or \
                      datetime.datetime.now(KST).time() < PRE_MARKET:
                    await asyncio.sleep(30)
                continue

            await _run_market_session(db, auth)
    except asyncio.CancelledError:
        logger.info("종료 처리 중...")
    finally:
        await db.close()
        await auth.close()
        logger.info("프로그램 종료")


if __name__ == "__main__":
    asyncio.run(main())
