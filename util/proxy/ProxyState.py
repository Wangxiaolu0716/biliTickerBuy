from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import os
import random
import time
from typing import Callable


@dataclass
class ProxyStateEntry:
    raw_proxy: str
    display_name: str
    cooldown_until: float = 0.0
    total_failures: int = 0
    total_successes: int = 0
    last_reason: str = ""
    recent_failures: deque[float] = field(default_factory=deque)

    def is_available(self, now: float | None = None) -> bool:
        current_time = time.time() if now is None else now
        return current_time >= self.cooldown_until

    def cooldown_remaining(self, now: float | None = None) -> int:
        current_time = time.time() if now is None else now
        return max(0, int(self.cooldown_until - current_time))


class ProxyStateRegistry:
    def __init__(
        self,
        proxy_list: list[str],
        *,
        mask_proxy: Callable[[str], str],
        failure_threshold: int = 2,
        failure_window_seconds: float = 45.0,
        cooldown_seconds: float = 180.0,
    ):
        self.failure_threshold = max(1, int(failure_threshold))
        self.failure_window_seconds = max(1.0, float(failure_window_seconds))
        self.cooldown_seconds = max(1.0, float(cooldown_seconds))
        self.states = [
            ProxyStateEntry(
                raw_proxy=proxy,
                display_name=mask_proxy(proxy) or proxy,
            )
            for proxy in proxy_list
        ]
        # 起始线路随机：多个任务（各自一个进程）共用同一份代理列表时，
        # 如果都从 index 0 开始，T0 会把同一个 IP 瞬间打爆（立刻 412）。
        # 单线路时无影响；BTB_PROXY_RANDOM_START=0 可关掉。
        self.current_index = self._pick_initial_index(len(self.states))

    @staticmethod
    def _pick_initial_index(size: int) -> int:
        if size <= 1:
            return 0
        if str(os.environ.get("BTB_PROXY_RANDOM_START", "1")).strip().lower() in {
            "0",
            "false",
            "off",
            "no",
        }:
            return 0
        return random.randrange(size)

    def set_current_index(self, index: int) -> None:
        if index < 0 or index >= len(self.states):
            raise IndexError("proxy index out of range")
        self.current_index = index

    def current_state(self) -> ProxyStateEntry:
        return self.states[self.current_index]

    def current_display_name(self) -> str:
        return self.current_state().display_name

    def _trim_failures(self, state: ProxyStateEntry, now: float) -> None:
        window_start = now - self.failure_window_seconds
        while state.recent_failures and state.recent_failures[0] < window_start:
            state.recent_failures.popleft()

    def record_current_success(self) -> None:
        state = self.current_state()
        state.total_successes += 1
        state.recent_failures.clear()

    def record_current_failure(self, reason: str) -> bool:
        now = time.time()
        state = self.current_state()
        state.total_failures += 1
        state.last_reason = reason
        state.recent_failures.append(now)
        self._trim_failures(state, now)
        if len(state.recent_failures) < self.failure_threshold:
            return False
        state.cooldown_until = max(state.cooldown_until, now + self.cooldown_seconds)
        state.recent_failures.clear()
        return True

    def cool_down_current(self, reason: str = "", seconds: float | None = None) -> bool:
        """立刻把当前线路打入冷却，不等失败阈值。

        用于 412 这种"硬信号"：它表示该 IP 已被限流/封禁（社区实测约 10~15 分钟）。
        按阈值累计（默认 2 次）意味着要白白多打一发请求，还会让已封禁的 IP 在轮换里被再次选中。
        """

        now = time.time()
        state = self.current_state()
        state.total_failures += 1
        state.last_reason = reason or "cool_down"
        window = float(seconds) if seconds is not None else float(self.cooldown_seconds)
        state.cooldown_until = max(state.cooldown_until, now + max(1.0, window))
        state.recent_failures.clear()
        return True

    def available_count(self, now: float | None = None) -> int:
        current_time = time.time() if now is None else now
        return sum(1 for state in self.states if state.is_available(current_time))

    def cooldown_count(self, now: float | None = None) -> int:
        current_time = time.time() if now is None else now
        return sum(1 for state in self.states if not state.is_available(current_time))

    def has_available_proxy(self, now: float | None = None) -> bool:
        return self.available_count(now) > 0

    def is_current_available(self, now: float | None = None) -> bool:
        return self.current_state().is_available(now)

    def switch_to_next_available(self) -> bool:
        now = time.time()
        if len(self.states) <= 1:
            return False
        for offset in range(1, len(self.states)):
            next_index = (self.current_index + offset) % len(self.states)
            if self.states[next_index].is_available(now):
                self.current_index = next_index
                return True
        return False

    def ensure_current_available(self) -> bool:
        if self.is_current_available():
            return True
        return self.switch_to_next_available()

    def current_status_text(self) -> str:
        return (
            f"{self.current_display_name()} | "
            f"可用 {self.available_count()}/{len(self.states)} | "
            f"冷却 {self.cooldown_count()}"
        )

    def describe_all_states(self) -> str:
        now = time.time()
        parts: list[str] = []
        for index, state in enumerate(self.states):
            label = state.display_name
            if index == self.current_index:
                label += "(当前)"
            if state.is_available(now):
                status = "可用"
            else:
                status = f"冷却 {state.cooldown_remaining(now)} 秒"
            parts.append(f"{label}:{status}")
        return "；".join(parts)
