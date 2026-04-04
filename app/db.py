from __future__ import annotations

import asyncio
import logging
import time

import asyncpg

from .config import settings

logger = logging.getLogger(__name__)

TRADE_INSERT = """
    INSERT INTO ws_trade (ts, symbol, cntg_hour, price, volume, trade_dir,
        ask1, bid1, ask1_qty, bid1_qty, mkop_code, hour_cls, vi_std_price)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
"""

ORDERBOOK_INSERT = """
    INSERT INTO ws_orderbook (ts, symbol, bsop_hour, hour_cls,
        ask_prices, bid_prices, ask_qtys, bid_qtys)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
"""

MEMBER_INSERT = """
    INSERT INTO rest_member (ts, symbol, sell_qtys, buy_qtys,
        glob_sell_qty, glob_buy_qty, glob_net_qty)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
"""

DAILY_BASE_INSERT = """
    INSERT INTO rest_daily_base (trade_date, symbol, base_price, upper_limit,
        lower_limit, tick_unit, listed_shares, status_code)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (trade_date, symbol) DO NOTHING
"""

DAILY_INVESTOR_INSERT = """
    INSERT INTO rest_daily_investor (trade_date, symbol,
        prsn_net_qty, frgn_net_qty, orgn_net_qty)
    VALUES ($1,$2,$3,$4,$5)
    ON CONFLICT (trade_date, symbol) DO NOTHING
"""


class Database:
    def __init__(self):
        self._pool: asyncpg.Pool | None = None
        self._trade_buf: list[tuple] = []
        self._orderbook_buf: list[tuple] = []
        self._lock = asyncio.Lock()

    async def init(self):
        num_accounts = len(settings.account_list)
        min_sz = max(2, num_accounts + 1)
        max_sz = max(10, num_accounts * 4 + 2)
        self._pool = await asyncpg.create_pool(
            settings.db_dsn, min_size=min_sz, max_size=max_sz
        )
        logger.info("DB 커넥션 풀 생성 (min=%d, max=%d, 계정=%d)", min_sz, max_sz, num_accounts)

    async def close(self):
        await self.flush()
        if self._pool:
            await self._pool.close()
            logger.info("DB 커넥션 풀 종료")

    # -- 틱 데이터 버퍼링 --

    async def add_trade(self, rec: dict):
        ts = int(time.time() * 1000)
        row = (
            ts, rec["symbol"], rec["cntg_hour"], rec["price"], rec["volume"],
            rec["trade_dir"], rec["ask1"], rec["bid1"], rec["ask1_qty"],
            rec["bid1_qty"], rec["mkop_code"], rec["hour_cls"], rec["vi_std_price"],
        )
        async with self._lock:
            self._trade_buf.append(row)

    async def add_orderbook(self, rec: dict):
        ts = int(time.time() * 1000)
        row = (
            ts, rec["symbol"], rec["bsop_hour"], rec["hour_cls"],
            rec["ask_prices"], rec["bid_prices"], rec["ask_qtys"], rec["bid_qtys"],
        )
        async with self._lock:
            self._orderbook_buf.append(row)

    # -- 배치 플러시 --

    async def _flush_trades(self):
        if not self._trade_buf:
            return
        buf = self._trade_buf
        self._trade_buf = []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(TRADE_INSERT, buf)
            logger.debug("체결 %d건 저장 (마지막: %s %s원 %s주)",
                         len(buf), buf[-1][1], buf[-1][3], buf[-1][4])
        except Exception as e:
            logger.exception("체결 저장 실패 (%d건 폐기)", len(buf))
            from . import notify
            await notify.send_error("체결 저장 실패", f"{len(buf)}건 폐기: {e}")

    async def _flush_orderbooks(self):
        if not self._orderbook_buf:
            return
        buf = self._orderbook_buf
        self._orderbook_buf = []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(ORDERBOOK_INSERT, buf)
            logger.debug("호가 %d건 저장 (마지막: %s ask1=%s bid1=%s)",
                         len(buf), buf[-1][1], buf[-1][4][0], buf[-1][5][0])
        except Exception as e:
            logger.exception("호가 저장 실패 (%d건 폐기)", len(buf))
            from . import notify
            await notify.send_error("호가 저장 실패", f"{len(buf)}건 폐기: {e}")

    async def flush(self):
        async with self._lock:
            await self._flush_trades()
            await self._flush_orderbooks()

    # -- REST 데이터 (버퍼링 불필요, 건수 적음) --

    async def insert_member(self, rec: dict):
        ts = int(time.time() * 1000)
        row = (
            ts, rec["symbol"], rec["sell_qtys"], rec["buy_qtys"],
            rec["glob_sell_qty"], rec["glob_buy_qty"], rec["glob_net_qty"],
        )
        async with self._pool.acquire() as conn:
            await conn.execute(MEMBER_INSERT, *row)

    async def insert_daily_base(self, rec: dict):
        row = (
            rec["trade_date"], rec["symbol"], rec["base_price"],
            rec["upper_limit"], rec["lower_limit"], rec["tick_unit"],
            rec["listed_shares"], rec["status_code"],
        )
        async with self._pool.acquire() as conn:
            await conn.execute(DAILY_BASE_INSERT, *row)

    async def insert_daily_investor(self, rec: dict):
        row = (
            rec["trade_date"], rec["symbol"], rec["prsn_net_qty"],
            rec["frgn_net_qty"], rec["orgn_net_qty"],
        )
        async with self._pool.acquire() as conn:
            await conn.execute(DAILY_INVESTOR_INSERT, *row)
