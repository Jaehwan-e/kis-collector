import asyncio
import datetime
import logging
import signal

from .auth import AuthManager
from .backup import run_backup_all
from .bot import bot
from .config import AccountConfig, settings
from .db import Database
from . import notify
from . import stats
from .rest import RESTPoller
from .ws import WSClient

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


async def _run_account_session(auth: AuthManager, db: Database):
    """계정 하나의 장중 세션: WS + REST 동시 실행"""
    name = auth.account.name
    symbols = auth.account.symbols

    rest = RESTPoller(auth, db)
    ws = WSClient(auth, db)

    try:
        # 장 시작 전 daily_base 수집
        logger.info("[%s] 일별 시세(daily_base) 수집", name)
        await rest.poll_daily_base()

        # WS 시작 시간 대기
        await _wait_until(WS_START)

        # 장중: WS + REST 폴링
        logger.info("[%s] 장중 데이터 수집 시작", name)
        ws_task = asyncio.create_task(ws.run())
        rest_task = asyncio.create_task(rest.poll_member())

        try:
            await _wait_until(WS_END)
        finally:
            ws.stop()
            ws_task.cancel()
            rest_task.cancel()
            await asyncio.gather(ws_task, rest_task, return_exceptions=True)

        # 장 마감 후 daily_investor 수집
        await _wait_until(POST_MARKET)
        logger.info("[%s] 일별 투자자(daily_investor) 수집", name)
        await rest.poll_daily_investor()

    except asyncio.CancelledError:
        ws.stop()
        raise
    finally:
        await rest.close()
        await auth.close()


async def _run_market_session(db: Database):
    """하루 장중 세션: 모든 계정 동시 실행"""
    stats.reset_all()

    accounts = settings.account_list
    total_symbols = sum(len(a.symbols) for a in accounts)

    # 1) 08:00까지 대기
    await _wait_until(PRE_MARKET)

    # 2) 첫 번째 계정만 발급 → 휴장일 체크
    first_auth = AuthManager(accounts[0])
    await first_auth.ensure_tokens()
    logger.info("[%s] 토큰 발급 완료", accounts[0].name)

    first_rest = RESTPoller(first_auth, db)
    if not await first_rest.is_market_open():
        logger.info("오늘 휴장 — 수집 스킵")
        await notify.send("📅 오늘 휴장 — 수집 스킵")
        await first_rest.close()
        await first_auth.close()
        await _wait_until(POST_MARKET)
        return
    await first_rest.close()

    # 3) 개장일 — 나머지 계정 토큰 발급
    auths = [first_auth]
    for acc in accounts[1:]:
        auth = AuthManager(acc)
        await auth.ensure_tokens()
        logger.info("[%s] 토큰 발급 완료", acc.name)
        auths.append(auth)

    if settings.is_multi_account:
        await notify.send_startup_multi(accounts)
    else:
        await notify.send_startup()

    start_time = datetime.datetime.now(KST).strftime("%H:%M:%S")

    # 2) flush 루프 (공유 DB)
    flush_task = asyncio.create_task(_flush_loop(db))

    # 3) 계정별 세션 동시 실행 (발급된 auth 전달)
    account_tasks = [
        asyncio.create_task(_run_account_session(auth, db))
        for auth in auths
    ]

    try:
        results = await asyncio.gather(*account_tasks, return_exceptions=True)
        for acc, result in zip(accounts, results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            elif isinstance(result, Exception):
                logger.error("[%s] 세션 실패: %s", acc.name, result)
                await notify.send_error(f"[{acc.name}] 세션 실패", str(result)[:200])
    except asyncio.CancelledError:
        for t in account_tasks:
            t.cancel()
        await asyncio.gather(*account_tasks, return_exceptions=True)
        raise
    finally:
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        await db.flush()

    end_time = datetime.datetime.now(KST).strftime("%H:%M:%S")
    logger.info("장중 수집 종료, 버퍼 플러시 완료")

    # 4) 일일 보고
    daily_stats = await _collect_daily_stats(db, start_time, end_time)
    daily_stats["account_count"] = len(accounts)
    daily_stats["total_symbols"] = total_symbols
    await notify.send_daily_report(daily_stats)

    # 5) 자동 백업 — 예외가 상위로 전파되어 프로세스가 죽지 않도록 방어
    if settings.backup_remote_list:
        logger.info("자동 백업 시작")
        try:
            await run_backup_all()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("자동 백업 중 예외 발생 — 세션은 정상 종료")
            try:
                await notify.send_error("자동 백업 예외", "로그 참고")
            except Exception:
                pass


async def _collect_daily_stats(db: Database, start_time: str, end_time: str) -> dict:
    """오늘 수집된 데이터 건수 조회 (인메모리 카운터 기반)"""
    t = stats.totals()
    result = {
        "trade_count": t.trade_count,
        "orderbook_count": t.orderbook_count,
        "member_count": t.member_count,
        "daily_base_count": t.daily_base_count,
        "investor_count": t.investor_count,
        "ws_reconnects": t.ws_reconnects,
        "error_count": t.errors,
        "start_time": start_time,
        "end_time": end_time,
    }
    try:
        async with db._pool.acquire() as conn:
            result["db_size"] = await conn.fetchval(
                "SELECT pg_size_pretty(pg_database_size('stock_data'))")
    except Exception:
        logger.exception("DB 용량 조회 실패")
        result["db_size"] = "?"
    return result


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

    accounts = settings.account_list
    total_symbols = sum(len(a.symbols) for a in accounts)
    logger.info("시작: %d계정, %d종목", len(accounts), total_symbols)
    for acc in accounts:
        logger.info("  [%s] %d종목: %s", acc.name, len(acc.symbols),
                     ",".join(acc.symbols[:5]) + ("..." if len(acc.symbols) > 5 else ""))

    db = Database()
    await db.init()

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

            # 세션 내부 예외로 프로세스가 죽지 않도록 방어
            # CancelledError(정상 종료 신호)만 상위로 재전파
            try:
                await _run_market_session(db)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("장중 세션 예외 발생 — 다음 영업일로 대기")
                try:
                    await notify.send_error("세션 예외", "로그 참고")
                except Exception:
                    pass
                # 장 마감 시간까지 대기하여 외부 루프의 야간 대기 분기로 전환
                while datetime.datetime.now(KST).time() < POST_MARKET:
                    await asyncio.sleep(30)
    except asyncio.CancelledError:
        logger.info("종료 처리 중...")
    finally:
        await notify.send_shutdown()
        await bot.close()
        await notify.close()
        await db.close()
        logger.info("프로그램 종료")


if __name__ == "__main__":
    asyncio.run(main())
