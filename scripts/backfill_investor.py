"""rest_daily_investor 누락 종목 백필.

inquire-investor API는 output이 최근 N일 리스트로 들어오므로
한 번 호출로 여러 과거 날짜 데이터를 얻을 수 있다.

실행 전 조건:
  - 장 마감 후 실행 권장 (수집기와의 앱키 충돌 방지)
  - 수집기가 돌고 있으면 같은 계정의 토큰이 갱신될 수 있음 → 다른 계정 사용

사용법:
  python scripts/backfill_investor.py                    # 자동 감지 누락분 백필
  python scripts/backfill_investor.py --dates 2026-04-06 2026-04-09
  python scripts/backfill_investor.py --account MAIN-1
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import os
import sys

# 프로젝트 루트 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

from app.auth import AuthManager
from app.config import settings
from app.rest import RESTPoller
from app.db import Database

KST = datetime.timezone(datetime.timedelta(hours=9))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")


async def find_missing(dates: list[datetime.date], all_symbols: list[str]) -> dict[datetime.date, list[str]]:
    """날짜별로 누락된 종목 리스트 반환."""
    conn = await asyncpg.connect(settings.db_dsn)
    try:
        missing = {}
        for d in dates:
            rows = await conn.fetch(
                "SELECT symbol FROM rest_daily_investor WHERE trade_date=$1",
                d,
            )
            present = {r["symbol"] for r in rows}
            lacking = [s for s in all_symbols if s not in present]
            missing[d] = lacking
            logger.info("  %s: %d/%d 저장됨, %d 누락",
                        d, len(present), len(all_symbols), len(lacking))
        return missing
    finally:
        await conn.close()


async def backfill_one_symbol(
    poller: RESTPoller, db: Database, symbol: str, target_dates: set[datetime.date]
) -> dict[datetime.date, bool]:
    """특정 종목에 대해 API 호출 → output 리스트에서 target_dates 매칭 → DB 저장.

    반환: {date: 성공여부}
    """
    results = {d: False for d in target_dates}
    try:
        data = await poller._request(
            "/uapi/domestic-stock/v1/quotations/inquire-investor",
            "FHKST01010900",
            {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
        )
        items = data.get("output", [])
        if not items:
            logger.warning("  %s: output 비어있음", symbol)
            return results

        for item in items:
            bsop_date = item.get("stck_bsop_date", "")
            if len(bsop_date) != 8:
                continue
            try:
                d = datetime.date(int(bsop_date[:4]), int(bsop_date[4:6]), int(bsop_date[6:8]))
            except ValueError:
                continue
            if d not in target_dates:
                continue
            if not item.get("prsn_ntby_qty"):
                logger.debug("  %s %s: prsn_ntby_qty 비어있음, skip", symbol, d)
                continue
            rec = {
                "trade_date": d,
                "symbol": symbol,
                "prsn_net_qty": int(item.get("prsn_ntby_qty") or 0),
                "frgn_net_qty": int(item.get("frgn_ntby_qty") or 0),
                "orgn_net_qty": int(item.get("orgn_ntby_qty") or 0),
            }
            await db.insert_daily_investor(rec)
            results[d] = True
            logger.info("  ✓ %s %s: 개인=%d 외인=%d 기관=%d",
                        symbol, d, rec["prsn_net_qty"], rec["frgn_net_qty"], rec["orgn_net_qty"])
    except Exception as e:
        logger.exception("  %s: 백필 실패: %s", symbol, e)
    return results


async def main():
    parser = argparse.ArgumentParser(description="rest_daily_investor 백필")
    parser.add_argument("--dates", nargs="*", help="YYYY-MM-DD 형식. 미지정 시 최근 14일 중 누락분 자동 감지")
    parser.add_argument("--account", default=None, help="사용할 계정 이름 (기본: 첫 번째)")
    parser.add_argument("--delay", type=float, default=None, help="API 호출 간격(초)")
    args = parser.parse_args()

    # 날짜 결정
    if args.dates:
        dates = [datetime.date.fromisoformat(d) for d in args.dates]
    else:
        today = datetime.datetime.now(KST).date()
        dates = [today - datetime.timedelta(days=i) for i in range(1, 15)]
        # 주말 제외
        dates = [d for d in dates if d.weekday() < 5]

    # 계정 선택
    account_list = settings.account_list
    if not account_list:
        logger.error("config에 계정이 없습니다")
        return
    if args.account:
        account = next((a for a in account_list if a.name == args.account), None)
        if not account:
            logger.error("계정 %s 못 찾음. 사용 가능: %s",
                         args.account, [a.name for a in account_list])
            return
    else:
        account = account_list[0]

    all_symbols = account.symbols
    logger.info("계정 %s (symbols %d개)", account.name, len(all_symbols))

    # 누락 종목 파악
    logger.info("누락 종목 조회...")
    missing = await find_missing(dates, all_symbols)
    missing = {d: syms for d, syms in missing.items() if syms}  # 누락 없는 날짜 제외
    if not missing:
        logger.info("누락 없음. 종료.")
        return

    logger.info("백필 대상:")
    for d, syms in missing.items():
        logger.info("  %s: %d 종목", d, len(syms))

    # 백필 실행 (종목별로 한 번씩 호출)
    # 한 종목을 호출하면 최근 N일이 함께 옴 → 여러 누락 날짜를 한 번에 처리
    symbols_to_call = set()
    for syms in missing.values():
        symbols_to_call.update(syms)

    logger.info("API 호출 예정 종목: %d개", len(symbols_to_call))

    auth = AuthManager(account)
    await auth.ensure_tokens()
    db = Database()
    await db.init()
    try:
        poller = RESTPoller(auth, db)
        try:
            rest_delay = args.delay if args.delay is not None else settings.rest_delay
            filled = {d: 0 for d in missing}
            attempted = {d: 0 for d in missing}
            for i, symbol in enumerate(sorted(symbols_to_call), 1):
                target_dates = {d for d, syms in missing.items() if symbol in syms}
                if i % 10 == 0:
                    logger.info("진행: %d/%d", i, len(symbols_to_call))
                results = await backfill_one_symbol(poller, db, symbol, target_dates)
                for d, ok in results.items():
                    attempted[d] += 1
                    if ok:
                        filled[d] += 1
                await asyncio.sleep(rest_delay)
            logger.info("━━ 백필 결과 ━━")
            for d in sorted(missing):
                logger.info("  %s: %d/%d 성공 (원래 누락 %d)",
                            d, filled[d], attempted[d], len(missing[d]))
        finally:
            await poller.close()
    finally:
        await db.close()
        await auth.close()


if __name__ == "__main__":
    asyncio.run(main())
