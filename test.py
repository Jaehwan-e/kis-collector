"""기능별 개별 테스트 스크립트

실행: source .venv/bin/activate && python test.py [테스트명]

테스트 목록:
  parser      — 파서 단위 테스트 (API 호출 없음)
  db          — DB 커넥션 + INSERT/SELECT 테스트
  auth        — 토큰 발급 + 파일 캐싱 테스트
  holiday     — 휴장일 조회 테스트
  daily_base  — 일별 시세 조회 + DB 저장 테스트
  ws          — WebSocket 10초 연결 테스트
  all         — 전체 순차 실행
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import sys
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test")


# ============================================================
# 1. parser 테스트 (오프라인, API 호출 없음)
# ============================================================
def test_parser():
    from parser import parse_message, TRADE_FIELDS, ORDERBOOK_FIELDS

    logger.info("=== parser 테스트 시작 ===")

    # JSON 제어 메시지
    json_msg = '{"header":{"tr_id":"H0STASP0","tr_key":"005930","encrypt":"N"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS"}}'
    result = parse_message(json_msg)
    assert result is not None
    assert result["_type"] == "control"
    assert result["tr_id"] == "H0STASP0"
    logger.info("  JSON 제어 메시지 파싱 OK")

    # 체결 데이터 (파이프 구분자)
    trade_fields = ["005930", "093012", "71000", "2", "500", "0.71", "70500",
                    "70000", "71500", "69500", "71100", "70900", "10", "500000",
                    "35000000000", "100", "200", "100", "120.5", "300000",
                    "360000", "1", "54.5", "105.3", "090000", "2", "1000",
                    "091500", "2", "500", "092000", "5", "-500", "20260331",
                    "21", "N", "5000", "3000", "100000", "80000", "1.5",
                    "450000", "111.1", "0", "0", "71000"]
    trade_msg = f"0|H0STCNT0|001|{'^'.join(trade_fields)}"
    result = parse_message(trade_msg)
    assert result is not None
    assert result["_type"] == "trade"
    assert result["symbol"] == "005930"
    assert result["price"] == 71000
    assert result["volume"] == 10
    assert result["trade_dir"] == "1"
    assert result["mkop_code"] == "21"
    assert result["hour_cls"] == "0"
    logger.info("  체결 데이터 파싱 OK: price=%d, volume=%d", result["price"], result["volume"])

    # 호가 데이터
    ob_fields = ["005930", "093012", "0"]
    ob_fields += [str(71000 + i * 100) for i in range(10)]   # ask 1~10
    ob_fields += [str(70900 - i * 100) for i in range(10)]   # bid 1~10
    ob_fields += [str(1000 + i) for i in range(10)]           # ask qty 1~10
    ob_fields += [str(2000 + i) for i in range(10)]           # bid qty 1~10
    ob_fields += ["50000", "60000", "1000", "2000"]           # total ask/bid, ovtm
    ob_fields += ["70950", "100", "500", "50", "2", "0.07"]   # antc
    ob_fields += ["500000", "100", "-200", "50", "-30"]        # acml, icdc
    ob_fields += ["01", "21"]                                  # deal_cls, mkop
    ob_msg = f"0|H0STASP0|001|{'^'.join(ob_fields)}"
    result = parse_message(ob_msg)
    assert result is not None
    assert result["_type"] == "orderbook"
    assert result["symbol"] == "005930"
    assert len(result["ask_prices"]) == 10
    assert len(result["bid_qtys"]) == 10
    assert result["mkop_code"] == "21"
    logger.info("  호가 데이터 파싱 OK: ask1=%d, bid1=%d", result["ask_prices"][0], result["bid_prices"][0])

    # 잘못된 메시지
    assert parse_message("garbage") is None
    assert parse_message("0|UNKNOWN|001|data") is None
    logger.info("  잘못된 메시지 처리 OK")

    # 필드 수 부족
    short_msg = "0|H0STCNT0|001|005930^093012^71000"
    assert parse_message(short_msg) is None
    logger.info("  필드 부족 처리 OK")

    logger.info("=== parser 테스트 완료 ===\n")


# ============================================================
# 2. DB 테스트 (로컬 PostgreSQL 필요)
# ============================================================
async def test_db():
    from db import Database

    logger.info("=== DB 테스트 시작 ===")
    db = Database()
    await db.init()

    # 체결 데이터 INSERT
    trade_rec = {
        "symbol": "005930", "cntg_hour": "093012", "price": 71000,
        "volume": 10, "trade_dir": "1", "ask1": 71100, "bid1": 70900,
        "ask1_qty": 5000, "bid1_qty": 3000, "mkop_code": "21",
        "hour_cls": "0", "vi_std_price": 71000,
    }
    await db.add_trade(trade_rec)
    await db.flush()

    async with db._pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM ws_trade WHERE symbol = '005930'")
    logger.info("  ws_trade INSERT 후 count: %d", count)
    assert count >= 1

    # 호가 데이터 INSERT
    ob_rec = {
        "symbol": "005930", "bsop_hour": "093012", "hour_cls": "0",
        "mkop_code": "21",
        "ask_prices": [71000 + i * 100 for i in range(10)],
        "bid_prices": [70900 - i * 100 for i in range(10)],
        "ask_qtys": [1000 + i for i in range(10)],
        "bid_qtys": [2000 + i for i in range(10)],
    }
    await db.add_orderbook(ob_rec)
    await db.flush()

    async with db._pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM ws_orderbook WHERE symbol = '005930'")
    logger.info("  ws_orderbook INSERT 후 count: %d", count)
    assert count >= 1

    # REST 테이블 INSERT
    member_rec = {
        "symbol": "005930",
        "sell_qtys": [100, 200, 300, 400, 500],
        "buy_qtys": [600, 700, 800, 900, 1000],
        "glob_sell_qty": 5000, "glob_buy_qty": 6000, "glob_net_qty": 1000,
    }
    await db.insert_member(member_rec)
    async with db._pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM rest_member WHERE symbol = '005930'")
    logger.info("  rest_member INSERT 후 count: %d", count)
    assert count >= 1

    base_rec = {
        "trade_date": datetime.date.today(), "symbol": "005930",
        "base_price": 70000, "upper_limit": 91000, "lower_limit": 49000,
        "tick_unit": 100, "listed_shares": 5969782550, "status_code": "55 ",
    }
    await db.insert_daily_base(base_rec)
    async with db._pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM rest_daily_base WHERE symbol = '005930'")
    logger.info("  rest_daily_base INSERT 후 count: %d", count)
    assert count >= 1

    inv_rec = {
        "trade_date": datetime.date.today(), "symbol": "005930",
        "prsn_net_qty": -50000, "frgn_net_qty": 30000, "orgn_net_qty": 20000,
    }
    await db.insert_daily_investor(inv_rec)
    async with db._pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM rest_daily_investor WHERE symbol = '005930'")
    logger.info("  rest_daily_investor INSERT 후 count: %d", count)
    assert count >= 1

    # 테스트 데이터 정리
    async with db._pool.acquire() as conn:
        await conn.execute("DELETE FROM ws_trade WHERE symbol = '005930'")
        await conn.execute("DELETE FROM ws_orderbook WHERE symbol = '005930'")
        await conn.execute("DELETE FROM rest_member WHERE symbol = '005930'")
        await conn.execute("DELETE FROM rest_daily_base WHERE symbol = '005930'")
        await conn.execute("DELETE FROM rest_daily_investor WHERE symbol = '005930'")
    logger.info("  테스트 데이터 정리 완료")

    await db.close()
    logger.info("=== DB 테스트 완료 ===\n")


# ============================================================
# 3. 토큰 발급 테스트 (KIS API 호출)
# ============================================================
async def test_auth():
    from auth import AuthManager

    logger.info("=== auth 테스트 시작 ===")
    auth = AuthManager()
    await auth.ensure_tokens()

    assert auth.approval_key, "approval_key가 비어있음"
    assert auth.access_token, "access_token이 비어있음"
    logger.info("  approval_key: %s...", auth.approval_key[:20])
    logger.info("  access_token: %s...", auth.access_token[:20])

    # 두 번째 호출 — 파일 캐시에서 로드되어야 함
    auth2 = AuthManager()
    await auth2.ensure_tokens()
    assert auth2.approval_key == auth.approval_key, "캐싱된 토큰이 다름"
    logger.info("  파일 캐싱 재사용 OK")

    await auth.close()
    await auth2.close()
    logger.info("=== auth 테스트 완료 ===\n")


# ============================================================
# 4. 휴장일 조회 테스트 (KIS API 호출)
# ============================================================
async def test_holiday():
    from auth import AuthManager
    from db import Database
    from rest import RESTPoller

    logger.info("=== 휴장일 조회 테스트 시작 ===")
    auth = AuthManager()
    await auth.ensure_tokens()

    db = Database()
    await db.init()
    rest = RESTPoller(auth, db)

    # 오늘
    is_open = await rest.is_market_open()
    logger.info("  오늘 개장 여부: %s", is_open)

    # 일요일 (반드시 휴장)
    next_sunday = datetime.date.today()
    while next_sunday.weekday() != 6:
        next_sunday += datetime.timedelta(days=1)
    is_open_sun = await rest.is_market_open(next_sunday)
    assert not is_open_sun, f"{next_sunday}(일요일)이 개장으로 응답됨"
    logger.info("  %s(일요일) 휴장 확인 OK", next_sunday)

    await rest.close()
    await db.close()
    await auth.close()
    logger.info("=== 휴장일 조회 테스트 완료 ===\n")


# ============================================================
# 5. daily_base 조회 + DB 저장 테스트 (KIS API 호출)
# ============================================================
async def test_daily_base():
    from auth import AuthManager
    from db import Database
    from rest import RESTPoller

    logger.info("=== daily_base 테스트 시작 ===")
    auth = AuthManager()
    await auth.ensure_tokens()

    db = Database()
    await db.init()
    rest = RESTPoller(auth, db)

    await rest.poll_daily_base()

    async with db._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, base_price, upper_limit, lower_limit "
            "FROM rest_daily_base WHERE trade_date = $1",
            datetime.date.today(),
        )
    for row in rows:
        logger.info("  %s: 기준가=%d 상한=%d 하한=%d",
                     row["symbol"], row["base_price"], row["upper_limit"], row["lower_limit"])
    assert len(rows) > 0, "daily_base 데이터 없음"

    # 정리
    async with db._pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM rest_daily_base WHERE trade_date = $1",
            datetime.date.today(),
        )

    await rest.close()
    await db.close()
    await auth.close()
    logger.info("=== daily_base 테스트 완료 ===\n")


# ============================================================
# 6. WebSocket 10초 연결 테스트 (KIS API 호출)
# ============================================================
async def test_ws():
    from auth import AuthManager
    from db import Database
    from ws import WSClient

    logger.info("=== WebSocket 테스트 시작 (10초) ===")
    auth = AuthManager()
    await auth.ensure_tokens()

    db = Database()
    await db.init()
    ws = WSClient(auth, db)

    ws_task = asyncio.create_task(ws.run())
    await asyncio.sleep(10)
    ws.stop()
    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    trade_count = len(db._trade_buf)
    ob_count = len(db._orderbook_buf)
    logger.info("  10초간 수신 — 체결 버퍼: %d건, 호가 버퍼: %d건", trade_count, ob_count)

    # 버퍼 내용을 DB에 저장
    await db.flush()

    async with db._pool.acquire() as conn:
        t = await conn.fetchval("SELECT COUNT(*) FROM ws_trade")
        o = await conn.fetchval("SELECT COUNT(*) FROM ws_orderbook")
    logger.info("  DB 저장 후 — ws_trade: %d건, ws_orderbook: %d건", t, o)

    # 정리
    async with db._pool.acquire() as conn:
        await conn.execute("DELETE FROM ws_trade")
        await conn.execute("DELETE FROM ws_orderbook")

    await db.close()
    await auth.close()
    logger.info("=== WebSocket 테스트 완료 ===\n")


# ============================================================
# 실행
# ============================================================
TESTS = {
    "parser": lambda: test_parser(),
    "db": lambda: asyncio.run(test_db()),
    "auth": lambda: asyncio.run(test_auth()),
    "holiday": lambda: asyncio.run(test_holiday()),
    "daily_base": lambda: asyncio.run(test_daily_base()),
    "ws": lambda: asyncio.run(test_ws()),
}


def run_all():
    for name, fn in TESTS.items():
        try:
            fn()
            logger.info(">>> %s PASSED\n", name)
        except Exception as e:
            logger.error(">>> %s FAILED: %s\n", name, e)


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"

    if target == "all":
        run_all()
    elif target in TESTS:
        try:
            TESTS[target]()
            logger.info(">>> %s PASSED", target)
        except Exception as e:
            logger.error(">>> %s FAILED: %s", target, e)
    else:
        print(f"알 수 없는 테스트: {target}")
        print(f"사용 가능: {', '.join(TESTS.keys())}, all")
