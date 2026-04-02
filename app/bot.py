from __future__ import annotations

import asyncio
import datetime
import logging

import aiohttp

from app.config import settings
from app import notify

logger = logging.getLogger(__name__)

KST = datetime.timezone(datetime.timedelta(hours=9))


class TelegramBot:
    """텔레그램 명령어 폴링 (long polling)"""

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._offset = 0
        self._stop = False
        self._handlers: dict[str, callable] = {}

    def command(self, cmd: str):
        """명령어 핸들러 등록 데코레이터"""
        def decorator(fn):
            self._handlers[cmd] = fn
            return fn
        return decorator

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def run(self):
        """명령어 폴링 루프"""
        if not settings.telegram_bot_token:
            return

        logger.info("텔레그램 봇 명령어 리스너 시작")
        while not self._stop:
            try:
                session = await self._get_session()
                url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/getUpdates"
                async with session.get(url, params={
                    "offset": self._offset,
                    "timeout": 10,
                    "allowed_updates": '["message"]',
                }) as resp:
                    data = await resp.json()

                for update in data.get("result", []):
                    self._offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    text = msg.get("text", "")

                    # 본인 채팅만 허용
                    if chat_id != settings.telegram_chat_id:
                        continue

                    if text.startswith("/"):
                        cmd = text.split()[0].split("@")[0]  # /backup@botname → /backup
                        handler = self._handlers.get(cmd)
                        if handler:
                            asyncio.create_task(handler())
                        else:
                            await notify.send(f"알 수 없는 명령어: {cmd}")

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("텔레그램 봇 폴링 에러")
                await asyncio.sleep(5)

    def stop(self):
        self._stop = True

    async def close(self):
        self.stop()
        if self._session and not self._session.closed:
            await self._session.close()


async def _get_db_size() -> str:
    try:
        import asyncpg
        from app.config import settings as s
        conn = await asyncpg.connect(s.db_dsn, timeout=5)
        size = await conn.fetchval("SELECT pg_size_pretty(pg_database_size('stock_data'))")
        await conn.close()
        return size
    except Exception:
        return "확인 실패"


# 봇 인스턴스
bot = TelegramBot()


@bot.command("/backup")
async def cmd_backup():
    """밀린 날짜 + 오늘 데이터 전부 백업"""
    from app.backup import run_backup_all
    await run_backup_all()


@bot.command("/status")
async def cmd_status():
    """수집 + 백업 통합 상태"""
    from app.main import _error_count, _ws_reconnects
    from app.backup import get_state_summary, _get_pending_dates
    now = datetime.datetime.now(KST)
    state = get_state_summary()
    pending = _get_pending_dates()
    last_date = state.get("last_success_date", "없음")
    last_route = state.get("last_success_route", "없음")
    last_time = state.get("last_success_time", "")

    from app.notify import _get_disk_usage
    disk = _get_disk_usage()

    text = (
        f"📋 <b>현재 상태</b> ({now:%H:%M:%S})\n\n"
        f"<b>수집</b>\n"
        f"  에러: {_error_count}건\n"
        f"  WS 재접속: {_ws_reconnects}회\n\n"
        f"<b>저장공간</b>\n"
        f"  디스크: {disk}\n\n"
        f"<b>백업</b>\n"
        f"  마지막 성공: {last_date}"
        + (f" [{last_route}]" if last_route else "")
        + f"\n  시각: {last_time[:19] if last_time else '없음'}"
        + f"\n  밀린 날짜: {len(pending)}일"
    )
    if len(pending) > 1:
        text += f" ({pending[0]} ~ {pending[-1]})"
    await notify.send(text)


@bot.command("/help")
async def cmd_help():
    await notify.send(
        "<b>📌 명령어 목록</b>\n\n"
        "/status — 수집 + 백업 상태\n"
        "/backup — 밀린 날짜 + 오늘 전부 백업\n"
        "/help — 명령어 목록"
    )
