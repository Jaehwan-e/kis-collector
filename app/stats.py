from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AccountStats:
    trade_count: int = 0
    orderbook_count: int = 0
    member_count: int = 0
    daily_base_count: int = 0
    investor_count: int = 0
    ws_reconnects: int = 0
    errors: int = 0

    def reset(self):
        self.trade_count = 0
        self.orderbook_count = 0
        self.member_count = 0
        self.daily_base_count = 0
        self.investor_count = 0
        self.ws_reconnects = 0
        self.errors = 0


_stats: dict[str, AccountStats] = {}


def get(name: str) -> AccountStats:
    if name not in _stats:
        _stats[name] = AccountStats()
    return _stats[name]


def all_stats() -> dict[str, AccountStats]:
    return _stats


def totals() -> AccountStats:
    t = AccountStats()
    for s in _stats.values():
        t.trade_count += s.trade_count
        t.orderbook_count += s.orderbook_count
        t.member_count += s.member_count
        t.daily_base_count += s.daily_base_count
        t.investor_count += s.investor_count
        t.ws_reconnects += s.ws_reconnects
        t.errors += s.errors
    return t


def reset_all():
    for s in _stats.values():
        s.reset()
