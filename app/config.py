from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse, urlunparse

_BASE_DIR = os.path.join(os.path.dirname(__file__), os.pardir)
_CONFIG_FILE = os.path.join(_BASE_DIR, "config.json")
_REMOTES_OVERRIDE_FILE = os.path.join(_BASE_DIR, ".backup_remotes.json")


@dataclass
class AccountConfig:
    """계정별 설정"""
    name: str
    app_key: str
    app_secret: str
    symbols: list[str] = field(default_factory=list)


class Settings:
    def __init__(self):
        with open(_CONFIG_FILE) as f:
            raw = json.load(f)

        self.db_dsn: str = raw.get("db_dsn", "postgresql://gyeol@localhost:5432/stock_data")
        self.ws_url: str = raw.get("ws_url", "ws://ops.koreainvestment.com:21000")
        self.rest_url: str = raw.get("rest_url", "https://openapi.koreainvestment.com:9443")
        self.log_level: str = raw.get("log_level", "INFO")
        self.flush_interval: float = raw.get("flush_interval", 1.0)
        self.telegram_bot_token: str = raw.get("telegram_bot_token", "")
        self.telegram_chat_id: str = raw.get("telegram_chat_id", "")
        self._backup_remotes_raw = raw.get("backup_remotes", [])

        # REST API 모드: "production" (초당 20건) / "test" (초당 3건)
        self.api_mode: str = raw.get("api_mode", "production")
        self.poll_interval: int = raw.get("poll_interval", 20)

    @property
    def rest_delay(self) -> float:
        """종목 간 REST 호출 딜레이 (초)"""
        if self.api_mode == "test":
            return 0.35  # 초당 ~3건
        return 0.05  # 초당 ~20건

        # 계정
        raw_accounts = raw.get("accounts", [])
        if raw_accounts:
            self._accounts = []
            for i, acc in enumerate(raw_accounts):
                symbols_raw = acc.get("symbols", "")
                symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
                self._accounts.append(AccountConfig(
                    name=acc.get("name", f"account{i+1}"),
                    app_key=acc["app_key"],
                    app_secret=acc["app_secret"],
                    symbols=symbols,
                ))
        else:
            # 싱글 계정
            symbols_raw = raw.get("symbols", "")
            symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
            self._accounts = [AccountConfig(
                name="default",
                app_key=raw["app_key"],
                app_secret=raw["app_secret"],
                symbols=symbols,
            )]

    @property
    def account_list(self) -> list[AccountConfig]:
        return self._accounts

    @property
    def symbol_list(self) -> list[str]:
        """싱글 계정 호환용"""
        return self._accounts[0].symbols if self._accounts else []

    @property
    def is_multi_account(self) -> bool:
        return len(self._accounts) > 1

    @property
    def backup_remote_list(self) -> list[tuple[str, str]]:
        if not self._backup_remotes_raw:
            return []
        overrides = _load_ip_overrides()
        result = []
        for entry in self._backup_remotes_raw:
            name = entry["name"]
            dsn = entry["dsn"]
            if name in overrides:
                dsn = _replace_host(dsn, overrides[name])
            result.append((name, dsn))
        return result


settings = Settings()


def _load_ip_overrides() -> dict[str, str]:
    if not os.path.exists(_REMOTES_OVERRIDE_FILE):
        return {}
    try:
        with open(_REMOTES_OVERRIDE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_ip_override(name: str, ip: str):
    overrides = _load_ip_overrides()
    overrides[name] = ip
    with open(_REMOTES_OVERRIDE_FILE, "w") as f:
        json.dump(overrides, f, indent=2)


def remove_ip_override(name: str) -> bool:
    overrides = _load_ip_overrides()
    if name not in overrides:
        return False
    del overrides[name]
    with open(_REMOTES_OVERRIDE_FILE, "w") as f:
        json.dump(overrides, f, indent=2)
    return True


def get_ip_overrides() -> dict[str, str]:
    return _load_ip_overrides()


def _replace_host(dsn: str, new_host: str) -> str:
    parsed = urlparse(dsn)
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo += f":{parsed.password}"
        userinfo += "@"
    port = f":{parsed.port}" if parsed.port else ""
    new_netloc = f"{userinfo}{new_host}{port}"
    replaced = parsed._replace(netloc=new_netloc)
    return urlunparse(replaced)
