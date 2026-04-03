from __future__ import annotations

import json
from dataclasses import dataclass, field

from pydantic_settings import BaseSettings


@dataclass
class AccountConfig:
    """계정별 설정"""
    name: str
    app_key: str
    app_secret: str
    symbols: list[str] = field(default_factory=list)


class Settings(BaseSettings):
    # 싱글 계정 (기본)
    app_key: str = ""
    app_secret: str = ""
    symbols: str = ""

    # 멀티 계정 (JSON 배열)
    accounts: str = ""

    # 공통
    db_dsn: str = "postgresql://gyeol@localhost:5432/stock_data"
    ws_url: str = "ws://ops.koreainvestment.com:21000"
    rest_url: str = "https://openapi.koreainvestment.com:9443"
    log_level: str = "INFO"
    flush_interval: float = 1.0
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    backup_remotes: str = ""

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip() for s in self.symbols.split(",") if s.strip()]

    @property
    def account_list(self) -> list[AccountConfig]:
        """ACCOUNTS 설정 시 멀티 계정, 없으면 기존 단일 계정"""
        if self.accounts:
            raw = json.loads(self.accounts)
            result = []
            for i, acc in enumerate(raw):
                symbols_raw = acc.get("symbols", "")
                symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
                result.append(AccountConfig(
                    name=acc.get("name", f"account{i+1}"),
                    app_key=acc["app_key"],
                    app_secret=acc["app_secret"],
                    symbols=symbols,
                ))
            return result
        # 싱글 계정 폴백
        return [AccountConfig(
            name="default",
            app_key=self.app_key,
            app_secret=self.app_secret,
            symbols=self.symbol_list,
        )]

    @property
    def is_multi_account(self) -> bool:
        return len(self.account_list) > 1

    @property
    def backup_remote_list(self) -> list[tuple[str, str]]:
        """'name:dsn,name:dsn' → [(name, dsn), ...]"""
        if not self.backup_remotes:
            return []
        result = []
        for entry in self.backup_remotes.split(","):
            entry = entry.strip()
            if ":" in entry:
                name, dsn = entry.split(":", 1)
                result.append((name.strip(), dsn.strip()))
        return result

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
