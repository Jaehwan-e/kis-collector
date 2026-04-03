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

    def command(self, cmd: str, has_args: bool = False):
        """명령어 핸들러 등록 데코레이터. has_args=True면 메시지 텍스트를 인자로 전달."""
        def decorator(fn):
            self._handlers[cmd] = (fn, has_args)
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
                        entry = self._handlers.get(cmd)
                        if entry:
                            fn, has_args = entry
                            if has_args:
                                asyncio.create_task(fn(text))
                            else:
                                asyncio.create_task(fn())
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
    from app import stats
    from app.backup import get_state_summary, _get_pending_dates
    now = datetime.datetime.now(KST)
    state = get_state_summary()
    pending = _get_pending_dates()
    last_date = state.get("last_success_date", "없음")
    last_route = state.get("last_success_route", "없음")
    last_time = state.get("last_success_time", "")

    from app.notify import _get_disk_usage
    disk = _get_disk_usage()

    t = stats.totals()
    all_st = stats.all_stats()

    lines = [f"📋 <b>현재 상태</b> ({now:%H:%M:%S})\n"]

    # 계정별 수집 현황
    for name, s in all_st.items():
        lines.append(f"<b>[{name}]</b>")
        lines.append(f"  체결: {s.trade_count:,} | 호가: {s.orderbook_count:,}")
        lines.append(f"  회원사: {s.member_count:,} | 시세: {s.daily_base_count:,} | 투자자: {s.investor_count:,}")
        lines.append(f"  WS재접속: {s.ws_reconnects} | 에러: {s.errors}")

    # 합계
    lines.append(f"\n<b>합계</b>")
    lines.append(f"  체결: {t.trade_count:,} | 호가: {t.orderbook_count:,}")
    lines.append(f"  WS재접속: {t.ws_reconnects} | 에러: {t.errors}")

    lines.append(f"\n<b>저장공간</b>")
    lines.append(f"  디스크: {disk}")

    lines.append(f"\n<b>백업</b>")
    lines.append(f"  마지막 성공: {last_date}"
                 + (f" [{last_route}]" if last_route else ""))
    lines.append(f"  시각: {last_time[:19] if last_time else '없음'}")
    pending_text = f"  밀린 날짜: {len(pending)}일"
    if len(pending) > 1:
        pending_text += f" ({pending[0]} ~ {pending[-1]})"
    lines.append(pending_text)

    await notify.send("\n".join(lines))


@bot.command("/remotes")
async def cmd_remotes():
    """백업 원격지 목록 + 현재 IP 표시"""
    from app.config import get_ip_overrides
    from urllib.parse import urlparse
    remotes = settings.backup_remote_list
    overrides = get_ip_overrides()

    if not remotes:
        await notify.send("백업 원격지 미설정")
        return

    lines = ["<b>🔗 백업 원격지</b>\n"]
    for name, dsn in remotes:
        parsed = urlparse(dsn)
        ip = parsed.hostname
        port = parsed.port
        tag = " (수정됨)" if name in overrides else ""
        lines.append(f"  <b>{name}</b>: {ip}:{port}{tag}")
    lines.append(f"\n사용법: /setip [이름] [IP]")
    lines.append(f"초기화: /resetip [이름]")
    await notify.send("\n".join(lines))


@bot.command("/setip", has_args=True)
async def cmd_setip(text: str):
    """원격지 IP 변경: /setip datalinker 1.2.3.4"""
    from app.config import save_ip_override
    parts = text.split()
    if len(parts) != 3:
        await notify.send("사용법: /setip [이름] [IP]\n예: /setip datalinker 192.168.1.100")
        return

    name = parts[1]
    ip = parts[2]

    # 등록된 원격지인지 확인
    known = [n for n, _ in settings.backup_remote_list]
    # 오버라이드 적용 전 원본 기준으로 확인
    base_names = []
    for entry in settings.backup_remotes.split(","):
        entry = entry.strip()
        if ":" in entry:
            base_names.append(entry.split(":", 1)[0].strip())

    if name not in base_names:
        await notify.send(f"알 수 없는 원격지: {name}\n등록된 원격지: {', '.join(base_names)}")
        return

    save_ip_override(name, ip)
    await notify.send(f"✅ <b>{name}</b> IP 변경: <b>{ip}</b>")


@bot.command("/resetip", has_args=True)
async def cmd_resetip(text: str):
    """원격지 IP 초기화 (.env 원본으로): /resetip datalinker"""
    from app.config import remove_ip_override
    parts = text.split()
    if len(parts) != 2:
        await notify.send("사용법: /resetip [이름]\n예: /resetip datalinker")
        return

    name = parts[1]
    if remove_ip_override(name):
        await notify.send(f"✅ <b>{name}</b> IP 초기화 (.env 원본 사용)")
    else:
        await notify.send(f"{name}은 오버라이드가 없습니다")


@bot.command("/help")
async def cmd_help():
    await notify.send(
        "<b>📌 명령어 목록</b>\n\n"
        "/status — 수집 + 백업 상태\n"
        "/backup — 밀린 날짜 + 오늘 전부 백업\n"
        "/remotes — 백업 원격지 목록\n"
        "/setip [이름] [IP] — 원격지 IP 변경\n"
        "/resetip [이름] — IP 초기화\n"
        "/help — 명령어 목록"
    )
