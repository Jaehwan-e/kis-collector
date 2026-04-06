from __future__ import annotations

import json
import logging
import os
import time

import aiohttp

from .config import AccountConfig, settings

logger = logging.getLogger(__name__)

TOKEN_DIR = os.path.join(os.path.dirname(__file__), os.pardir)


class AuthManager:
    """한국투자증권 인증 관리 — 파일 캐싱 + 만료 시 자동 재발급"""

    def __init__(self, account: AccountConfig):
        self._account = account
        self._approval_key: str | None = None
        self._approval_expires: float = 0
        self._access_token: str | None = None
        self._access_expires: float = 0
        self._session: aiohttp.ClientSession | None = None
        self._token_file = os.path.join(TOKEN_DIR, f".tokens_{account.name}.json")
        self._load_from_file()

    @property
    def account(self) -> AccountConfig:
        return self._account

    def _load_from_file(self):
        if not os.path.exists(self._token_file):
            return
        try:
            with open(self._token_file) as f:
                data = json.load(f)
            self._approval_key = data.get("approval_key")
            self._approval_expires = data.get("approval_expires", 0)
            self._access_token = data.get("access_token")
            self._access_expires = data.get("access_expires", 0)
            logger.info("[%s] 토큰 파일 로드 완료", self._account.name)
        except Exception:
            logger.warning("[%s] 토큰 파일 로드 실패, 새로 발급합니다", self._account.name)

    def _save_to_file(self):
        data = {
            "approval_key": self._approval_key,
            "approval_expires": self._approval_expires,
            "access_token": self._access_token,
            "access_expires": self._access_expires,
        }
        with open(self._token_file, "w") as f:
            json.dump(data, f)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _should_reissue(self, expires: float) -> bool:
        """토큰 재발급 필요 여부 판단"""
        import datetime as _dt
        kst = _dt.timezone(_dt.timedelta(hours=9))
        now = _dt.datetime.now(kst)
        # 08시대면 무조건 재발급
        if now.hour == 8:
            return True
        # 만료 시각이 오늘 18:00 이전이면 재발급
        today_18 = now.replace(hour=18, minute=0, second=0, microsecond=0)
        return expires < today_18.timestamp()

    async def ensure_tokens(self):
        """유효한 토큰 확보 — 만료가 당일 18:00까지 유효하면 재사용, 아니면 재발급"""
        if not self._approval_key or self._should_reissue(self._approval_expires):
            await self._issue_approval_key()
        if not self._access_token or self._should_reissue(self._access_expires):
            await self._issue_access_token()

    async def reissue_access_token(self):
        """토큰 만료 시 외부에서 호출하는 재발급"""
        await self._issue_access_token()

    async def force_issue_approval_key(self):
        """WS 재접속 시 강제로 새 approval_key 발급"""
        await self._issue_approval_key()

    async def _issue_approval_key(self):
        session = await self._get_session()
        url = f"{settings.rest_url}/oauth2/Approval"
        body = {
            "grant_type": "client_credentials",
            "appkey": self._account.app_key,
            "secretkey": self._account.app_secret,
        }
        async with session.post(url, json=body) as resp:
            data = await resp.json()
            if resp.status == 200 and "approval_key" in data:
                self._approval_key = data["approval_key"]
                self._approval_expires = time.time() + 24 * 3600
                self._save_to_file()
                logger.info("[%s] approval_key 발급 성공", self._account.name)
                return
            raise RuntimeError(f"[{self._account.name}] approval_key 발급 실패: {resp.status} {data}")

    async def _issue_access_token(self):
        session = await self._get_session()
        url = f"{settings.rest_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": self._account.app_key,
            "appsecret": self._account.app_secret,
        }
        async with session.post(url, json=body) as resp:
            data = await resp.json()
            if resp.status == 200 and "access_token" in data:
                self._access_token = data["access_token"]
                self._access_expires = time.time() + 24 * 3600
                self._save_to_file()
                logger.info("[%s] access_token 발급 성공", self._account.name)
                return
            raise RuntimeError(f"[{self._account.name}] access_token 발급 실패: {resp.status} {data}")

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
