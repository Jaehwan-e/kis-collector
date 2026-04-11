#!/usr/bin/env python3
"""수집 서버에서 로컬 DB로 데이터를 당겨오는 Pull 방식 백업 스크립트.

포트포워딩이나 pg_hba.conf 수정 없이, SSH 터널을 통해 수집 서버의
PostgreSQL에 접속하여 날짜 범위별로 데이터를 가져온다.

## 동작 흐름
1. SSH 터널 생성 (로컬 15432 포트 → 수집 서버의 127.0.0.1:5432)
2. 로컬 15432 포트로 원격 PG에 접속 (원격 입장에서는 localhost 접속으로 보임)
3. 날짜 범위의 데이터를 커서로 스트리밍 조회 → 로컬 PG에 배치 INSERT
4. 완료 시 SSH 터널 자동 종료

## 사용법
```bash
# 어제 하루치 가져오기 (기본값)
python scripts/pull_backup.py

# 특정 날짜 하루치
python scripts/pull_backup.py --date 2026-04-10

# 날짜 범위
python scripts/pull_backup.py --from 2026-04-07 --to 2026-04-10

# SSH 호스트 지정 (기본: config.json의 ssh_host 또는 ubuntu@collector)
python scripts/pull_backup.py --ssh ubuntu@1.2.3.4 --date 2026-04-10
```

## 필수 조건
- 로컬에서 수집 서버로 SSH 접속이 가능해야 함 (키 인증 권장)
- 수집 서버의 PostgreSQL이 localhost:5432에서 리슨 중이어야 함
- 로컬에 config.json이 있어야 함 (로컬 DB DSN 및 원격 DB 인증정보 읽음)
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import os
import subprocess
import sys
import time
from urllib.parse import urlparse

import asyncpg

# 로컬 config.json 읽기 위해 프로젝트 루트를 path에 추가
_BASE_DIR = os.path.join(os.path.dirname(__file__), os.pardir)
sys.path.insert(0, os.path.abspath(_BASE_DIR))

from app.config import settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pull_backup")

KST = datetime.timezone(datetime.timedelta(hours=9))

# app/backup.py와 동일한 테이블 정의
TABLES = [
    ("ws_trade", "ts"),
    ("ws_orderbook", "ts"),
    ("rest_member", "ts"),
]
DAILY_TABLES = [
    ("rest_daily_base", "trade_date"),
]
# investor는 전일 확정 데이터라 별도 처리
INVESTOR_TABLE = ("rest_daily_investor", "trade_date")

# 한 번에 전송할 행 수 (메모리 절약 + 네트워크 안정성)
BATCH_SIZE = 5000

# SSH 터널에서 사용할 로컬 포트 (원격 서버의 5432를 여기로 포워딩)
LOCAL_TUNNEL_PORT = 15432


class SSHTunnel:
    """수집 서버의 PostgreSQL(5432)을 로컬 15432 포트로 포워딩하는 SSH 터널.

    with 문으로 사용하면 자동으로 열고 닫힘.
    `-o ExitOnForwardFailure=yes` 로 포워딩 실패 시 즉시 종료.
    `-N` 으로 원격 명령 실행 없이 포워딩만 수행.
    """

    def __init__(self, ssh_host: str, remote_pg_port: int = 5432):
        self.ssh_host = ssh_host
        self.remote_pg_port = remote_pg_port
        self.proc: subprocess.Popen | None = None

    def __enter__(self):
        cmd = [
            "ssh",
            "-N",  # 원격 명령 실행 없음 (터널만)
            "-L", f"{LOCAL_TUNNEL_PORT}:127.0.0.1:{self.remote_pg_port}",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            self.ssh_host,
        ]
        logger.info("SSH 터널 생성: %s → localhost:%d", self.ssh_host, LOCAL_TUNNEL_PORT)
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        # SSH 터널이 실제로 열릴 때까지 잠시 대기 (최대 5초)
        for _ in range(50):
            time.sleep(0.1)
            if self.proc.poll() is not None:
                # SSH가 이미 종료된 경우 → 포워딩 실패
                stderr = self.proc.stderr.read().decode() if self.proc.stderr else ""
                raise RuntimeError(f"SSH 터널 생성 실패: {stderr}")
            if _is_port_open("127.0.0.1", LOCAL_TUNNEL_PORT):
                logger.info("SSH 터널 준비 완료")
                return self
        raise RuntimeError("SSH 터널 대기 타임아웃 (5초)")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            logger.info("SSH 터널 종료")


def _is_port_open(host: str, port: int) -> bool:
    """해당 host:port에 TCP 연결 가능한지 확인 (터널 준비 상태 체크용)"""
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        return sock.connect_ex((host, port)) == 0
    finally:
        sock.close()


def _build_remote_dsn() -> str:
    """원격 DB에 접속할 DSN 구성.

    로컬 DB DSN에서 user/password/database는 그대로 쓰고, host와 port만
    SSH 터널 쪽으로 변경. 수집 서버의 config.json이 로컬과 동일한 인증
    정보를 사용한다고 가정.
    """
    parsed = urlparse(settings.db_dsn)
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo += f":{parsed.password}"
        userinfo += "@"
    db_name = parsed.path or "/stock_data"
    return f"postgresql://{userinfo}127.0.0.1:{LOCAL_TUNNEL_PORT}{db_name}"


async def _copy_table(
    remote: asyncpg.Connection,
    local: asyncpg.Connection,
    table: str,
    where: str,
    *args,
) -> int:
    """원격에서 커서로 읽으며 로컬에 배치 INSERT (메모리 절약).

    전체 데이터를 메모리에 올리면 수백 MB가 될 수 있으므로, 트랜잭션 내에서
    서버 사이드 커서를 사용해 5000건씩 읽고 즉시 로컬에 쓴다.
    """
    total = 0
    insert_sql: str | None = None

    async with remote.transaction():
        # 원격 PG에 서버 사이드 커서 생성 — 결과 전체를 메모리에 올리지 않음
        cursor = await remote.cursor(f"SELECT * FROM {table} WHERE {where}", *args)

        while True:
            rows = await cursor.fetch(BATCH_SIZE)
            if not rows:
                break

            # 첫 배치에서 INSERT SQL을 한 번만 생성
            if insert_sql is None:
                columns = list(rows[0].keys())
                placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
                insert_sql = (
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )

            # 로컬에 배치 INSERT
            await local.executemany(insert_sql, [tuple(r.values()) for r in rows])
            total += len(rows)
            # 다른 비동기 작업에 이벤트 루프 양보 (장시간 블로킹 방지)
            await asyncio.sleep(0)

    return total


async def _pull_single_day(remote: asyncpg.Connection, local: asyncpg.Connection,
                            target_date: datetime.date) -> int:
    """하루치 데이터를 원격에서 당겨와 로컬에 저장.

    기존 데이터는 DELETE 후 INSERT하여 중복 없이 최신 상태로 덮어쓴다.
    """
    total = 0

    # ts 기반 테이블 (KST 자정 기준 하루 범위)
    day_start_ms = int(
        datetime.datetime(target_date.year, target_date.month, target_date.day,
                          tzinfo=KST).timestamp() * 1000
    )
    day_end_ms = day_start_ms + 86400000  # 24시간 = 86,400,000 ms

    for table, ts_col in TABLES:
        try:
            # 로컬의 해당 날짜 데이터 삭제 후 원격에서 다시 당겨옴
            await local.execute(
                f"DELETE FROM {table} WHERE {ts_col} >= $1 AND {ts_col} < $2",
                day_start_ms, day_end_ms,
            )
            rows = await _copy_table(
                remote, local, table,
                f"{ts_col} >= $1 AND {ts_col} < $2",
                day_start_ms, day_end_ms,
            )
            total += rows
            logger.info("  %s: %d건", table, rows)
        except Exception:
            logger.exception("  %s 실패", table)

    # 날짜 기반 테이블 (당일 trade_date)
    for table, date_col in DAILY_TABLES:
        try:
            await local.execute(
                f"DELETE FROM {table} WHERE {date_col} = $1",
                target_date,
            )
            rows = await _copy_table(
                remote, local, table,
                f"{date_col} = $1",
                target_date,
            )
            total += rows
            logger.info("  %s: %d건", table, rows)
        except Exception:
            logger.exception("  %s 실패", table)

    # investor: 전일 확정 데이터 특성상 target_date 전후 7일 범위 포함
    # (주말/연휴 공백으로 인한 누락 방지)
    inv_table, inv_col = INVESTOR_TABLE
    inv_start = target_date - datetime.timedelta(days=7)
    try:
        await local.execute(
            f"DELETE FROM {inv_table} WHERE {inv_col} >= $1 AND {inv_col} <= $2",
            inv_start, target_date,
        )
        rows = await _copy_table(
            remote, local, inv_table,
            f"{inv_col} >= $1 AND {inv_col} <= $2",
            inv_start, target_date,
        )
        total += rows
        logger.info("  %s: %d건 (%s~%s)", inv_table, rows, inv_start, target_date)
    except Exception:
        logger.exception("  %s 실패", inv_table)

    return total


async def _run(ssh_host: str, dates: list[datetime.date]):
    """메인 실행 함수: SSH 터널 → 양쪽 DB 연결 → 날짜별 순차 당겨오기"""
    with SSHTunnel(ssh_host):
        remote_dsn = _build_remote_dsn()
        logger.info("원격 DB 연결: %s", remote_dsn.split("@")[-1])

        remote = await asyncpg.connect(remote_dsn, timeout=30)
        local = await asyncpg.connect(settings.db_dsn, timeout=30)

        try:
            for d in dates:
                logger.info("=== %s 당겨오는 중 ===", d)
                start = time.time()
                total = await _pull_single_day(remote, local, d)
                elapsed = time.time() - start
                logger.info("=== %s 완료: %d건 | %.0f초 ===", d, total, elapsed)
        finally:
            await remote.close()
            await local.close()


def _parse_date(s: str) -> datetime.date:
    """YYYY-MM-DD 형식 파싱"""
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


def _date_range(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """start ~ end 사이의 모든 날짜 리스트 반환 (양 끝 포함)"""
    dates = []
    d = start
    while d <= end:
        dates.append(d)
        d += datetime.timedelta(days=1)
    return dates


def main():
    parser = argparse.ArgumentParser(description="수집 서버에서 로컬 DB로 데이터 당겨오기")
    parser.add_argument("--ssh", required=True, help="SSH 접속 호스트 (예: ubuntu@1.2.3.4)")
    parser.add_argument("--date", type=_parse_date, help="특정 날짜 (YYYY-MM-DD)")
    parser.add_argument("--from", dest="start", type=_parse_date, help="시작 날짜")
    parser.add_argument("--to", dest="end", type=_parse_date, help="종료 날짜")
    args = parser.parse_args()

    # 날짜 인자 처리: --date 단일, 또는 --from/--to 범위, 또는 기본값(어제)
    if args.date:
        dates = [args.date]
    elif args.start and args.end:
        dates = _date_range(args.start, args.end)
    elif args.start or args.end:
        parser.error("--from 과 --to 는 함께 사용해야 합니다")
    else:
        # 기본값: 어제 하루치
        yesterday = datetime.datetime.now(KST).date() - datetime.timedelta(days=1)
        dates = [yesterday]
        logger.info("날짜 미지정 → 어제(%s) 당겨옴", yesterday)

    asyncio.run(_run(args.ssh, dates))


if __name__ == "__main__":
    main()
