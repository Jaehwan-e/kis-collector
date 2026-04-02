from __future__ import annotations

import asyncio
import datetime
import logging

import aiohttp

from .auth import AuthManager
from .config import settings
from .db import Database

logger = logging.getLogger(__name__)

KST = datetime.timezone(datetime.timedelta(hours=9))


class RESTPoller:
    def __init__(self, auth: AuthManager, db: Database):
        self._auth = auth
        self._db = db
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(self, path: str, tr_id: str, params: dict) -> dict:
        token = self._auth.access_token
        session = await self._get_session()
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": settings.app_key,
            "appsecret": settings.app_secret,
            "tr_id": tr_id,
        }
        url = f"{settings.rest_url}{path}"
        async with session.get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"REST 요청 실패: {resp.status} {data}")
            return data

    # -- 휴장일 조회 (CTCA0903R) --

    async def is_market_open(self, date: datetime.date | None = None) -> bool:
        """오늘(또는 지정일)이 개장일인지 조회 (1일 1회만 호출)"""
        dt = date or datetime.date.today()
        data = await self._request(
            "/uapi/domestic-stock/v1/quotations/chk-holiday",
            "CTCA0903R",
            {
                "BASS_DT": dt.strftime("%Y%m%d"),
                "CTX_AREA_NK": "",
                "CTX_AREA_FK": "",
            },
        )
        output = data.get("output", [])
        if not output:
            logger.warning("휴장일 조회 응답 없음, 개장일로 간주")
            return True
        opnd_yn = output[0].get("opnd_yn", "Y")
        is_open = opnd_yn == "Y"
        logger.info("개장일 조회: %s → %s", dt, "개장" if is_open else "휴장")
        return is_open

    # -- 회원사 (FHKST01010600) --

    async def poll_member(self):
        """장중(09:00~15:30) 30초 주기 폴링"""
        while True:
            now = datetime.datetime.now(KST)
            market_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
            market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)

            if now < market_start:
                await asyncio.sleep((market_start - now).total_seconds())
                continue
            if now >= market_end:
                break

            for symbol in settings.symbol_list:
                try:
                    data = await self._request(
                        "/uapi/domestic-stock/v1/quotations/inquire-member",
                        "FHKST01010600",
                        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
                    )
                    out = data.get("output", {})
                    if isinstance(out, list):
                        out = out[0] if out else {}
                    sell_qtys = [int(out.get(f"total_seln_qty{i}") or 0) for i in range(1, 6)]
                    buy_qtys = [int(out.get(f"total_shnu_qty{i}") or 0) for i in range(1, 6)]
                    rec = {
                        "symbol": symbol,
                        "sell_qtys": sell_qtys,
                        "buy_qtys": buy_qtys,
                        "glob_sell_qty": int(out.get("glob_total_seln_qty") or 0),
                        "glob_buy_qty": int(out.get("glob_total_shnu_qty") or 0),
                        "glob_net_qty": int(out.get("glob_ntby_qty") or 0),
                    }
                    await self._db.insert_member(rec)
                    logger.debug("회원사 저장: %s sell=%s buy=%s glob=%d/%d/%d",
                                 symbol, sell_qtys, buy_qtys,
                                 rec["glob_sell_qty"], rec["glob_buy_qty"], rec["glob_net_qty"])
                except Exception:
                    logger.exception("회원사 폴링 실패: %s", symbol)

            await asyncio.sleep(30)

    # -- 장 시작 전 시세 (FHKST01010100) --

    async def poll_daily_base(self):
        for symbol in settings.symbol_list:
            try:
                data = await self._request(
                    "/uapi/domestic-stock/v1/quotations/inquire-price",
                    "FHKST01010100",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
                )
                out = data.get("output", {})
                rec = {
                    "trade_date": datetime.date.today(),
                    "symbol": symbol,
                    "base_price": int(out.get("stck_sdpr", 0)),
                    "upper_limit": int(out.get("stck_mxpr", 0)),
                    "lower_limit": int(out.get("stck_llam", 0)),
                    "tick_unit": int(out.get("aspr_unit", 0)),
                    "listed_shares": int(out.get("lstn_stcn", 0)),
                    "status_code": out.get("iscd_stat_cls_code", "000"),
                }
                await self._db.insert_daily_base(rec)
                logger.info("일별 시세 저장: %s 기준=%d 상한=%d 하한=%d 호가단위=%d 상장주수=%d",
                            symbol, rec["base_price"], rec["upper_limit"],
                            rec["lower_limit"], rec["tick_unit"], rec["listed_shares"])
            except Exception:
                logger.exception("일별 시세 실패: %s", symbol)

    # -- 장 마감 후 투자자 (FHKST01010900) --

    async def poll_daily_investor(self):
        for symbol in settings.symbol_list:
            try:
                data = await self._request(
                    "/uapi/domestic-stock/v1/quotations/inquire-investor",
                    "FHKST01010900",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
                )
                items = data.get("output", [])
                # 당일 데이터는 빈 문자열 → 값이 있는 첫 항목 사용
                row = None
                for item in items:
                    if item.get("prsn_ntby_qty"):
                        row = item
                        break
                if not row:
                    continue
                bsop_date = row.get("stck_bsop_date", "")
                trade_date = (
                    datetime.date(int(bsop_date[:4]), int(bsop_date[4:6]), int(bsop_date[6:8]))
                    if len(bsop_date) == 8
                    else datetime.date.today()
                )
                rec = {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "prsn_net_qty": int(row.get("prsn_ntby_qty") or 0),
                    "frgn_net_qty": int(row.get("frgn_ntby_qty") or 0),
                    "orgn_net_qty": int(row.get("orgn_ntby_qty") or 0),
                }
                await self._db.insert_daily_investor(rec)
                logger.info("일별 투자자 저장: %s [%s] 개인=%d 외인=%d 기관=%d",
                            symbol, rec["trade_date"], rec["prsn_net_qty"],
                            rec["frgn_net_qty"], rec["orgn_net_qty"])
            except Exception:
                logger.exception("일별 투자자 실패: %s", symbol)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
