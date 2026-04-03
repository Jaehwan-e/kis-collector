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


async def send_daily_report(daily_stats: dict):
    """일일 종합 보고"""
    from app import stats as stats_mod

    now = datetime.datetime.now(KST)
    disk = _get_disk_usage()
    all_st = stats_mod.all_stats()
    t = stats_mod.totals()

    lines = [f"<b>📊 일일 수집 보고 ({now:%Y-%m-%d})</b>\n"]

    for name, s in all_st.items():
        lines.append(f"<b>[{name}]</b>")
        lines.append(f"  체결: {s.trade_count:,} | 호가: {s.orderbook_count:,}")
        lines.append(f"  회원사: {s.member_count:,} | 시세: {s.daily_base_count:,} | 투자자: {s.investor_count:,}")
        lines.append(f"  WS재접속: {s.ws_reconnects} | 에러: {s.errors}")

    lines.append(f"\n<b>합계</b>")
    lines.append(f"  체결: <b>{t.trade_count:,}</b> | 호가: <b>{t.orderbook_count:,}</b>")
    lines.append(f"  회원사: <b>{t.member_count:,}</b> | 시세: <b>{t.daily_base_count:,}</b> | 투자자: <b>{t.investor_count:,}</b>")
    lines.append(f"  WS재접속: <b>{t.ws_reconnects}</b> | 에러: <b>{t.errors}</b>")

    lines.append(f"\nDB 용량: <b>{daily_stats.get('db_size', '?')}</b>")
    lines.append(f"디스크: <b>{disk}</b>")
    lines.append(f"\n계정: <b>{daily_stats.get('account_count', 1)}</b>개"
                 f" | 종목: <b>{daily_stats.get('total_symbols', '?')}</b>개")
    lines.append(f"수집 시간: {daily_stats.get('start_time', '')} ~ {daily_stats.get('end_time', '')}")

    await send("\n".join(lines))


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
    """시작 알림 (싱글 계정)"""
    now = datetime.datetime.now(KST)
    text = (
        f"🟢 <b>수집 시작</b> ({now:%Y-%m-%d %H:%M})\n"
        f"종목: {len(settings.symbol_list)}개"
    )
    await send(text)


async def send_startup_multi(accounts):
    """시작 알림 (멀티 계정)"""
    now = datetime.datetime.now(KST)
    total = sum(len(a.symbols) for a in accounts)
    lines = [f"🟢 <b>수집 시작</b> ({now:%Y-%m-%d %H:%M})\n"]
    lines.append(f"계정: {len(accounts)}개 | 종목: {total}개\n")
    for acc in accounts:
        lines.append(f"  [{acc.name}] {len(acc.symbols)}종목")
    await send("\n".join(lines))


async def send_shutdown():
    """종료 알림"""
    now = datetime.datetime.now(KST)
    await send(f"🔴 <b>수집 종료</b> ({now:%Y-%m-%d %H:%M})")


async def close():
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None
