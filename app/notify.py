from __future__ import annotations

import logging
import datetime

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)

KST = datetime.timezone(datetime.timedelta(hours=9))

_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def send(text: str):
    """텔레그램 메시지 전송"""
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return
    try:
        session = await _get_session()
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
        await session.post(url, data={
            "chat_id": settings.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
        })
    except Exception:
        logger.exception("텔레그램 전송 실패")


async def send_daily_report(stats: dict):
    """일일 종합 보고"""
    now = datetime.datetime.now(KST)
    disk = _get_disk_usage()
    text = (
        f"<b>📊 일일 수집 보고 ({now:%Y-%m-%d})</b>\n\n"
        f"체결: <b>{stats.get('trade_count', 0):,}</b>건\n"
        f"호가: <b>{stats.get('orderbook_count', 0):,}</b>건\n"
        f"회원사: <b>{stats.get('member_count', 0):,}</b>건\n"
        f"일별시세: <b>{stats.get('daily_base_count', 0):,}</b>건\n"
        f"투자자: <b>{stats.get('investor_count', 0):,}</b>건\n\n"
        f"DB 용량: <b>{stats.get('db_size', '?')}</b>\n"
        f"디스크: <b>{disk}</b>\n\n"
        f"WS 재접속: <b>{stats.get('ws_reconnects', 0)}</b>회\n"
        f"에러: <b>{stats.get('error_count', 0)}</b>건\n\n"
        f"수집 시간: {stats.get('start_time', '')} ~ {stats.get('end_time', '')}"
    )
    await send(text)


def _get_disk_usage() -> str:
    """디스크 사용량 (used/total, percent)"""
    try:
        import shutil
        usage = shutil.disk_usage("/")
        used_gb = usage.used / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        pct = usage.used / usage.total * 100
        return f"{used_gb:.1f}GB / {total_gb:.1f}GB ({pct:.0f}%)"
    except Exception:
        return "확인 실패"


async def send_error(error_type: str, detail: str):
    """에러 알림"""
    now = datetime.datetime.now(KST)
    text = (
        f"⚠️ <b>{error_type}</b>\n"
        f"{now:%H:%M:%S} | {detail}"
    )
    await send(text)


async def send_startup():
    """시작 알림"""
    now = datetime.datetime.now(KST)
    text = (
        f"🟢 <b>수집 시작</b> ({now:%Y-%m-%d %H:%M})\n"
        f"종목: {len(settings.symbol_list)}개"
    )
    await send(text)


async def send_shutdown():
    """종료 알림"""
    now = datetime.datetime.now(KST)
    await send(f"🔴 <b>수집 종료</b> ({now:%Y-%m-%d %H:%M})")


async def close():
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None
