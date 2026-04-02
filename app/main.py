import asyncio
import datetime
import logging
import signal

from .auth import AuthManager
from .backup import run_backup_all
from .bot import bot
from .config import settings
from .db import Database
from . import notify
from .rest import RESTPoller
from .ws import WSClient

logger = logging.getLogger(__name__)

KST = datetime.timezone(datetime.timedelta(hours=9))

# 장 운영 시간
PRE_MARKET = datetime.time(8, 0)    # 토큰 발급 + 휴장일 조회 + daily_base 수집
WS_START = datetime.time(8, 30)     # WS + REST 시작
WS_END = datetime.time(15, 30)      # WS + REST 종료
POST_MARKET = datetime.time(15, 35) # daily_investor 수집

# 에러 카운터
_error_count = 0
_ws_reconnects = 0
_error_alert_threshold = 5


def _inc_error():
    global _error_count
    _error_count += 1


def _inc_ws_reconnect():
    global _ws_reconnects
    _ws_reconnects += 1


async def _wait_until(target: datetime.time):
    """목표 시각까지 대기 — 30초 간격으로 시각 확인 (슬립 복귀 대응)"""
    while True:
        now = datetime.datetime.now(KST).time()
        if now >= target:
            return
        await asyncio.sleep(30)


async def _run_market_session(db: Database, auth: AuthManager):
    """하루 장중 세션: 08:00 ~ 15:35"""
    global _error_count, _ws_reconnects
    _error_count = 0
    _ws_reconnects = 0

    rest = RESTPoller(auth, db)
    ws = WSClient(auth, db)

    # 1) 장 시작 전: 토큰 발급 + 휴장일 체크 + daily_base 수집
    await _wait_until(PRE_MARKET)

    logger.info("토큰 발급")
    await auth.ensure_tokens()

    if not await rest.is_market_open():
        logger.info("오늘 휴장 — 수집 스킵")
        await notify.send("📅 오늘 휴장 — 수집 스킵")
        await rest.close()
        return

    await notify.send_startup()

    logger.info("일별 시세(daily_base) 수집")
    await rest.poll_daily_base()

    # 2) WS 시작 시간 대기
    await _wait_until(WS_START)

    # 3) 장중: WS + REST 폴링 + 주기적 플러시
    logger.info("장중 데이터 수집 시작")
    start_time = datetime.datetime.now(KST).strftime("%H:%M:%S")

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
    end_time = datetime.datetime.now(KST).strftime("%H:%M:%S")
    logger.info("장중 수집 종료, 버퍼 플러시 완료")

    # 4) 장 마감 후 daily_investor 수집
    await _wait_until(POST_MARKET)

    logger.info("일별 투자자(daily_investor) 수집")
    await rest.poll_daily_investor()

    # 5) 일일 보고
    stats = await _collect_daily_stats(db, start_time, end_time)
    await notify.send_daily_report(stats)

    # 6) 자동 백업
    if settings.backup_remote_list:
        logger.info("자동 백업 시작")
        await run_backup_all()

    await rest.close()


async def _collect_daily_stats(db: Database, start_time: str, end_time: str) -> dict:
    """오늘 수집된 데이터 건수 조회"""
    today_start_ms = int(
        datetime.datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    )
    stats = {}
    try:
        async with db._pool.acquire() as conn:
            stats["trade_count"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ws_trade WHERE ts >= $1", today_start_ms)
            stats["orderbook_count"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ws_orderbook WHERE ts >= $1", today_start_ms)
            stats["member_count"] = await conn.fetchval(
                "SELECT COUNT(*) FROM rest_member WHERE ts >= $1", today_start_ms)
            stats["daily_base_count"] = await conn.fetchval(
                "SELECT COUNT(*) FROM rest_daily_base WHERE trade_date = $1",
                datetime.date.today())
            stats["investor_count"] = await conn.fetchval(
                "SELECT COUNT(*) FROM rest_daily_investor WHERE trade_date >= $1",
                datetime.date.today() - datetime.timedelta(days=1))
            stats["db_size"] = await conn.fetchval(
                "SELECT pg_size_pretty(pg_database_size('stock_data'))")
    except Exception:
        logger.exception("일일 통계 조회 실패")

    stats["ws_reconnects"] = _ws_reconnects
    stats["error_count"] = _error_count
    stats["start_time"] = start_time
    stats["end_time"] = end_time
    return stats


async def _flush_loop(db: Database):
    while True:
        await asyncio.sleep(settings.flush_interval)
        await db.flush()


class _KSTFormatter(logging.Formatter):
    """로그 시간을 KST로 표시"""
    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(record.created, tz=KST)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime("%Y-%m-%d %H:%M:%S")


async def main():
    handler = logging.StreamHandler()
    handler.setFormatter(_KSTFormatter(
        fmt="%(asctime)s KST %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        handlers=[handler],
    )

    db = Database()
    await db.init()
    auth = AuthManager()

    # 텔레그램 봇 명령어 리스너 시작
    bot_task = asyncio.create_task(bot.run())

    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("종료 시그널 수신")
        bot.stop()
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
        await notify.send_shutdown()
        await bot.close()
        await notify.close()
        await db.close()
        await auth.close()
        logger.info("프로그램 종료")


if __name__ == "__main__":
    asyncio.run(main())
