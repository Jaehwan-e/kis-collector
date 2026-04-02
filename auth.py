from __future__ import annotations

import json
import logging
import os
import time

import aiohttp

from config import settings

logger = logging.getLogger(__name__)

TOKEN_FILE = os.path.join(os.path.dirname(__file__), ".tokens.json")


class AuthManager:
    """한국투자증권 인증 관리 — 파일 캐싱 + 만료 시 자동 재발급"""

    def __init__(self):
        self._approval_key: str | None = None
        self._approval_expires: float = 0
        self._access_token: str | None = None
        self._access_expires: float = 0
        self._session: aiohttp.ClientSession | None = None
        self._load_from_file()

    def _load_from_file(self):
        if not os.path.exists(TOKEN_FILE):
            return
        try:
            with open(TOKEN_FILE) as f:
                data = json.load(f)
            self._approval_key = data.get("approval_key")
            self._approval_expires = data.get("approval_expires", 0)
            self._access_token = data.get("access_token")
            self._access_expires = data.get("access_expires", 0)
            logger.info("토큰 파일 로드 완료")
        except Exception:
            logger.warning("토큰 파일 로드 실패, 새로 발급합니다")

    def _save_to_file(self):
        data = {
            "approval_key": self._approval_key,
            "approval_expires": self._approval_expires,
            "access_token": self._access_token,
            "access_expires": self._access_expires,
        }
        with open(TOKEN_FILE, "w") as f:
            json.dump(data, f)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _today_cutoff(self) -> float:
        """오늘 08:00 KST의 epoch 타임스탬프"""
        import datetime
        kst = datetime.timezone(datetime.timedelta(hours=9))
        today_8 = datetime.datetime.now(kst).replace(
            hour=8, minute=0, second=0, microsecond=0
        )
        return today_8.timestamp()

    async def ensure_tokens(self):
        """유효한 토큰 확보 — 만료가 오늘 08:00 이후면 재사용, 아니면 재발급"""
        cutoff = self._today_cutoff()
        if not self._approval_key or self._approval_expires < cutoff:
            await self._issue_approval_key()
        if not self._access_token or self._access_expires < cutoff:
            await self._issue_access_token()

    async def force_issue_approval_key(self):
        """WS 재접속 시 강제로 새 approval_key 발급"""
        await self._issue_approval_key()

    async def _issue_approval_key(self):
        session = await self._get_session()
        url = f"{settings.rest_url}/oauth2/Approval"
        body = {
            "grant_type": "client_credentials",
            "appkey": settings.app_key,
            "secretkey": settings.app_secret,
        }
        async with session.post(url, json=body) as resp:
            data = await resp.json()
            if resp.status == 200 and "approval_key" in data:
                self._approval_key = data["approval_key"]
                self._approval_expires = time.time() + 24 * 3600
                self._save_to_file()
                logger.info("approval_key 발급 성공")
                return
            raise RuntimeError(f"approval_key 발급 실패: {resp.status} {data}")

    async def _issue_access_token(self):
        session = await self._get_session()
        url = f"{settings.rest_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": settings.app_key,
            "appsecret": settings.app_secret,
        }
        async with session.post(url, json=body) as resp:
            data = await resp.json()
            if resp.status == 200 and "access_token" in data:
                self._access_token = data["access_token"]
                self._access_expires = time.time() + 24 * 3600
                self._save_to_file()
                logger.info("access_token 발급 성공")
                return
            raise RuntimeError(f"access_token 발급 실패: {resp.status} {data}")

    @property
    def approval_key(self) -> str:
        if not self._approval_key:
            raise RuntimeError("approval_key 미발급 — ensure_tokens() 먼저 호출")
        return self._approval_key

    @property
    def access_token(self) -> str:
        if not self._access_token:
            raise RuntimeError("access_token 미발급 — ensure_tokens() 먼저 호출")
        return self._access_token

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
