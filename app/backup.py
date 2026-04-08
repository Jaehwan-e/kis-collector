from __future__ import annotations

import datetime
import json
import logging
import os
import time

import asyncpg

from app.config import settings
from app import notify

logger = logging.getLogger(__name__)

KST = datetime.timezone(datetime.timedelta(hours=9))
STATE_FILE = os.path.join(os.path.dirname(__file__), "..", ".backup_state.json")

TABLES = [
    ("ws_trade", "ts"),
    ("ws_orderbook", "ts"),
    ("rest_member", "ts"),
]
DAILY_TABLES = [
    ("rest_daily_base", "trade_date"),
]
# investor는 전일 확정 데이터로 저장되므로 별도 처리
INVESTOR_TABLE = ("rest_daily_investor", "trade_date")


# -- 상태 관리 --

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_last_success_date() -> datetime.date | None:
    state = _load_state()
    d = state.get("last_success_date")
    if d:
        return datetime.date.fromisoformat(d)
    return None


def get_last_success_route() -> str | None:
    state = _load_state()
    return state.get("last_success_route")


def get_state_summary() -> dict:
    return _load_state()


def _get_pending_dates() -> list[datetime.date]:
    """마지막 성공 이후 ~ 오늘까지 밀린 날짜 목록"""
    last = get_last_success_date()
    today = datetime.datetime.now(KST).date()
    if not last or last >= today:
        return [today]
    dates = []
    d = last + datetime.timedelta(days=1)
    while d <= today:
        dates.append(d)
        d += datetime.timedelta(days=1)
    return dates


# -- 연결 (자동 폴백) --

async def _connect_remote() -> tuple[asyncpg.Connection, str] | tuple[None, None]:
    """등록된 원격 DSN을 순서대로 시도, 첫 성공 반환"""
    remotes = settings.backup_remote_list
    if not remotes:
        await notify.send("⚠️ 백업 실패: BACKUP_REMOTES 미설정")
        return None, None

    for name, dsn in remotes:
        try:
            conn = await asyncpg.connect(dsn, timeout=10)
            logger.info("원격 연결 성공: %s", name)
            return conn, name
        except Exception as e:
            logger.warning("원격 연결 실패 [%s]: %s", name, e)
            continue

    await notify.send(
        "⚠️ 백업 실패: 모든 원격 연결 실패\n"
        + "\n".join(f"  - {name}" for name, _ in remotes)
    )
    return None, None


# -- 백업 실행 --

async def run_backup_all():
    """밀린 날짜 전부 + 오늘 데이터 백업"""
    dates = _get_pending_dates()
    if len(dates) > 1:
        await notify.send(f"🔄 밀린 백업 {len(dates)}일치 전송 시작\n{dates[0]} ~ {dates[-1]}")

    for d in dates:
        success, route = await _backup_single_day(d)
        if not success:
            await notify.send(f"⚠️ {d} 백업 실패 — 이후 날짜 중단")
            return

        state = _load_state()
        state["last_success_date"] = d.isoformat()
        state["last_success_route"] = route
        state["last_success_time"] = datetime.datetime.now(KST).isoformat()
        _save_state(state)

    if len(dates) > 1:
        await notify.send(f"✅ 밀린 백업 {len(dates)}일치 전송 완료")


async def run_backup(date: datetime.date | None = None):
    """단일 날짜 백업 (명령어용)"""
    if date:
        await _backup_single_day(date)
    else:
        await run_backup_all()


async def _backup_single_day(target_date: datetime.date) -> tuple[bool, str | None]:
    """하루치 데이터를 원격 DB로 전송. (성공여부, 경로명) 반환."""
    remote, route = await _connect_remote()
    if not remote:
        return False, None

    start = time.time()
    total_rows = 0
    errors = []

    await notify.send(f"🔄 백업 시작 ({target_date}) → {route}")

    local = None
    try:
        local = await asyncpg.connect(settings.db_dsn)
        await _ensure_remote_schema(remote)

        # ts 기반 테이블
        day_start_ms = int(
            datetime.datetime(target_date.year, target_date.month, target_date.day,
                              tzinfo=KST).timestamp() * 1000
        )
        day_end_ms = day_start_ms + 86400000

        for table, ts_col in TABLES:
            try:
                await remote.execute(
                    f"DELETE FROM {table} WHERE {ts_col} >= $1 AND {ts_col} < $2",
                    day_start_ms, day_end_ms,
                )
                rows = await _copy_table(
                    local, remote, table,
                    f"{ts_col} >= $1 AND {ts_col} < $2",
                    day_start_ms, day_end_ms,
                )
                total_rows += rows
                logger.info("백업 %s: %d건", table, rows)
            except Exception as e:
                errors.append(f"{table}: {e}")
                logger.exception("백업 실패 %s", table)

        # date 기반 테이블 (당일)
        for table, date_col in DAILY_TABLES:
            try:
                await remote.execute(
                    f"DELETE FROM {table} WHERE {date_col} = $1",
                    target_date,
                )
                rows = await _copy_table(
                    local, remote, table,
                    f"{date_col} = $1",
                    target_date,
                )
                total_rows += rows
                logger.info("백업 %s: %d건", table, rows)
            except Exception as e:
                errors.append(f"{table}: {e}")
                logger.exception("백업 실패 %s", table)

        # investor (전일 확정 데이터 — 수집일 기준 전 영업일)
        inv_table, inv_col = INVESTOR_TABLE
        # target_date 당일에 수집된 investor의 trade_date는 전일 이하
        # target_date와 직전 7일 범위로 포함 (주말/연휴 고려)
        inv_start = target_date - datetime.timedelta(days=7)
        try:
            await remote.execute(
                f"DELETE FROM {inv_table} WHERE {inv_col} >= $1 AND {inv_col} <= $2",
                inv_start, target_date,
            )
            rows = await _copy_table(
                local, remote, inv_table,
                f"{inv_col} >= $1 AND {inv_col} <= $2",
                inv_start, target_date,
            )
            total_rows += rows
            logger.info("백업 %s: %d건 (%s~%s)", inv_table, rows, inv_start, target_date)
        except Exception as e:
            errors.append(f"{inv_table}: {e}")
            logger.exception("백업 실패 %s", inv_table)

    except Exception as e:
        errors.append(f"전송 중 에러: {e}")
        logger.exception("백업 전송 실패")
    finally:
        if local and not local.is_closed():
            await local.close()
        if not remote.is_closed():
            await remote.close()

    elapsed = time.time() - start

    if errors:
        await notify.send(
            f"⚠️ 백업 실패 ({target_date}) [{route}]\n"
            f"{total_rows:,}건 | {elapsed:.0f}초\n"
            f"에러: {'; '.join(errors)}"
        )
        return False, route
    else:
        await notify.send(
            f"✅ 백업 완료 ({target_date}) [{route}]\n"
            f"{total_rows:,}건 | {elapsed:.0f}초"
        )
        return True, route


BATCH_SIZE = 5000


async def _copy_table(
    local: asyncpg.Connection,
    remote: asyncpg.Connection,
    table: str,
    where: str,
    *args,
) -> int:
    """로컬에서 커서로 읽으며 원격에 배치 INSERT (메모리 절약)"""
    total = 0
    insert_sql = None

    async with local.transaction():
        cursor = await local.cursor(f"SELECT * FROM {table} WHERE {where}", *args)

        while True:
            rows = await cursor.fetch(BATCH_SIZE)
            if not rows:
                break

            if insert_sql is None:
                columns = list(rows[0].keys())
                placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
                insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

            await remote.executemany(insert_sql, [tuple(r.values()) for r in rows])
            total += len(rows)
            await asyncio.sleep(0)

    return total


async def _ensure_remote_schema(remote: asyncpg.Connection):
    """원격 DB에 테이블이 없으면 생성"""
    schema_sql = """
    CREATE TABLE IF NOT EXISTS ws_trade (
        ts BIGINT NOT NULL, symbol VARCHAR(10) NOT NULL,
        cntg_hour VARCHAR(10) NOT NULL, price INT NOT NULL,
        volume INT NOT NULL, trade_dir VARCHAR(5) NOT NULL,
        ask1 INT NOT NULL, bid1 INT NOT NULL,
        ask1_qty INT NOT NULL, bid1_qty INT NOT NULL,
        mkop_code VARCHAR(10) NOT NULL, hour_cls VARCHAR(5) NOT NULL,
        vi_std_price INT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ws_orderbook (
        ts BIGINT NOT NULL, symbol VARCHAR(10) NOT NULL,
        bsop_hour VARCHAR(10) NOT NULL, hour_cls VARCHAR(5) NOT NULL,
        ask_prices INT[] NOT NULL, bid_prices INT[] NOT NULL,
        ask_qtys INT[] NOT NULL, bid_qtys INT[] NOT NULL
    );
    CREATE TABLE IF NOT EXISTS rest_member (
        ts BIGINT NOT NULL, symbol VARCHAR(10) NOT NULL,
        sell_qtys INT[], buy_qtys INT[],
        glob_sell_qty INT, glob_buy_qty INT, glob_net_qty INT
    );
    CREATE TABLE IF NOT EXISTS rest_daily_base (
        trade_date DATE NOT NULL, symbol VARCHAR(10) NOT NULL,
        base_price INT NOT NULL, upper_limit INT NOT NULL,
        lower_limit INT NOT NULL, tick_unit INT NOT NULL,
        listed_shares BIGINT NOT NULL, status_code VARCHAR(10) NOT NULL,
        PRIMARY KEY (trade_date, symbol)
    );
    CREATE TABLE IF NOT EXISTS rest_daily_investor (
        trade_date DATE NOT NULL, symbol VARCHAR(10) NOT NULL,
        prsn_net_qty INT NOT NULL, frgn_net_qty INT NOT NULL,
        orgn_net_qty INT NOT NULL,
        PRIMARY KEY (trade_date, symbol)
    );
    """
    await remote.execute(schema_sql)
