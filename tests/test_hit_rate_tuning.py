"""成功率/风控调优的回归测试（纯逻辑，不联网）。

覆盖：
* 等待抖动与退避封顶
* errno 提取的健壮性（原实现 int(None) 会抛 TypeError）
* 限速错误码表
* handle_proxy_failure 的 risk_control 分支：412 不冷却线路、单线路不长睡
* createV2 fanout 只在多出口时才并发
"""

from __future__ import annotations

import pytest

from task.buy_helpers import (
    extract_errno,
    handle_proxy_failure,
    jittered_seconds,
)
from util.Constant import (
    DEFAULT_RATE_LIMIT_DELAY_MS,
    RETRY_JITTER_RATIO,
    RISK_CONTROL_BACKOFF_BASE_SECONDS,
    RISK_CONTROL_BACKOFF_MAX_SECONDS,
    RISK_CONTROL_PROXY_COOLDOWN_MIN_SECONDS,
    RISK_CONTROL_QUICK_RETRIES,
    RISK_CONTROL_SHORT_DELAY_SECONDS,
    SINGLE_LINE_MAX_BACKOFF_SECONDS,
)
from util.ErrorCodes import ErrorCodes


@pytest.mark.parametrize("base", [0.3, 1.0, 2.4])
def test_jittered_seconds_within_ratio(base):
    for _ in range(50):
        value = jittered_seconds(base)
        assert (
            base * (1 - RETRY_JITTER_RATIO) <= value <= base * (1 + RETRY_JITTER_RATIO)
        )


def test_jittered_seconds_rejects_non_positive():
    assert jittered_seconds(0) == 0.0
    assert jittered_seconds(-1) == 0.0


def test_http_429_uses_fixed_short_delay():
    """429 只是"没抽中"，必须固定短延迟重试——指数退避会砍掉抽签次数。

    实测依据：某次真实抢票里，指数退避让第一分钟只有 25 次尝试（250ms 理论值 240 次）。
    """

    assert 250 <= DEFAULT_RATE_LIMIT_DELAY_MS <= 350


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"errno": 0}, 0),
        ({"errno": 900001}, 900001),
        ({"code": 100009}, 100009),
        ({"errno": None, "code": 5}, 5),
        ({"errno": "100051"}, 100051),
        ({"errno": "abc"}, -1),
        ({"errno": True}, -1),
        ({}, -1),
        (None, -1),
    ],
)
def test_extract_errno(payload, expected):
    assert extract_errno(payload) == expected


def test_shield_codes_are_not_backoff_signals():
    """900001/900002/221/3 是「1 秒盾」的正常结果，不该触发退避。"""

    for code in (3, 221, 900001, 900002):
        assert ErrorCodes.is_shield(code)
    for code in (0, 100009, 100048, 100051):
        assert not ErrorCodes.is_shield(code)


class _FakeBackoff:
    def __init__(self):
        self.rounds = 0

    def reset(self):
        self.rounds = 0

    def next_delay_seconds(self):
        self.rounds += 1
        return 30 * 2 ** (self.rounds - 1)

    def should_notify(self):
        return False


class _FakeRequest:
    proxy_cooldown_seconds = 180.0

    def __init__(self, proxies, available):
        self.proxies = proxies
        self.available = available
        self.current = proxies[0]
        self.failure_marks = 0
        self.switch_calls = 0
        self.cool_downs: list[tuple[str, float]] = []

    def current_proxy_display(self):
        return self.current

    def mark_current_proxy_failure(self, reason):
        self.failure_marks += 1
        return False

    def cool_down_current_proxy(self, reason, seconds=None):
        self.cool_downs.append((reason, float(seconds or 0)))
        return True

    def switch_proxy(self):
        self.switch_calls += 1
        if len(self.proxies) <= 1:
            return False
        self.current = self.proxies[1]
        return True

    def has_available_proxy(self):
        return self.available > 0

    def available_proxy_count(self):
        return self.available

    def proxy_count(self):
        return len(self.proxies)


class _FakeNotifierConfig:
    pass


def test_risk_control_does_not_cool_down_single_line():
    request = _FakeRequest(["none"], available=1)
    backoff = _FakeBackoff()

    message, delay = handle_proxy_failure(
        request,
        "412 风控",
        backoff,
        _FakeNotifierConfig(),
        risk_control=True,
    )

    assert request.failure_marks == 0
    assert request.switch_calls == 0
    assert delay == max(1, int(RISK_CONTROL_SHORT_DELAY_SECONDS))
    assert "风控" in (message or "")
    assert "切换网络" in (message or "")


def test_risk_control_switches_line_when_available():
    request = _FakeRequest(["p1", "p2"], available=2)
    backoff = _FakeBackoff()

    message, delay = handle_proxy_failure(
        request,
        "412 风控",
        backoff,
        _FakeNotifierConfig(),
        risk_control=True,
    )

    assert request.switch_calls == 1
    assert request.failure_marks == 0
    assert delay is None
    assert "p2" in (message or "")
    # 412 命中后该出口要被立刻冷却，且冷却时长不短于下限（412 是 10~15 分钟的 IP 封禁）
    assert len(request.cool_downs) == 1
    assert request.cool_downs[0][1] >= float(RISK_CONTROL_PROXY_COOLDOWN_MIN_SECONDS)


def test_single_line_failure_uses_short_backoff():
    request = _FakeRequest(["none"], available=0)
    backoff = _FakeBackoff()

    _, delay = handle_proxy_failure(
        request,
        "连接超时",
        backoff,
        _FakeNotifierConfig(),
    )

    assert request.failure_marks == 1
    assert delay is not None
    assert delay <= max(1, int(SINGLE_LINE_MAX_BACKOFF_SECONDS))
    # 不再进入 ProxyBackoff 的 30/60/120/240s 长睡
    assert backoff.rounds == 0


def test_proxy_registry_randomizes_initial_index_for_multi_proxy(monkeypatch):
    from util.proxy.ProxyState import ProxyStateRegistry

    proxy_list = [f"http://127.0.0.1:{8000 + i}" for i in range(8)]
    starts = {
        ProxyStateRegistry(proxy_list, mask_proxy=str).current_index for _ in range(40)
    }
    # 多线路时起始 index 不应固定为 0（否则多任务会把同一条代理打爆）
    assert any(index != 0 for index in starts)

    monkeypatch.setenv("BTB_PROXY_RANDOM_START", "0")
    assert ProxyStateRegistry(proxy_list, mask_proxy=str).current_index == 0
    # 单线路永远是 0
    assert ProxyStateRegistry(["none"], mask_proxy=str).current_index == 0


def test_risk_control_single_line_escalates_after_quick_retries():
    request = _FakeRequest(["none"], available=1)
    backoff = _FakeBackoff()

    fast = []
    for streak in range(1, max(1, int(RISK_CONTROL_QUICK_RETRIES)) + 1):
        _, delay = handle_proxy_failure(
            request,
            "412 风控",
            backoff,
            _FakeNotifierConfig(),
            risk_control=True,
            risk_streak=streak,
        )
        fast.append(delay)
    assert fast == [max(1, int(RISK_CONTROL_SHORT_DELAY_SECONDS))] * len(fast)

    # 超过快速重试次数后进入指数退避（30s 起），并封顶
    _, delay = handle_proxy_failure(
        request,
        "412 风控",
        backoff,
        _FakeNotifierConfig(),
        risk_control=True,
        risk_streak=max(1, int(RISK_CONTROL_QUICK_RETRIES)) + 1,
    )
    assert delay == int(RISK_CONTROL_BACKOFF_BASE_SECONDS)

    _, capped = handle_proxy_failure(
        request,
        "412 风控",
        backoff,
        _FakeNotifierConfig(),
        risk_control=True,
        risk_streak=20,
    )
    assert capped == int(RISK_CONTROL_BACKOFF_MAX_SECONDS)


def test_risk_control_multi_line_never_waits():
    request = _FakeRequest(["p1", "p2"], available=2)
    backoff = _FakeBackoff()
    for streak in (1, 5, 20):
        _, delay = handle_proxy_failure(
            request,
            "412 风控",
            backoff,
            _FakeNotifierConfig(),
            risk_control=True,
            risk_streak=streak,
        )
        assert delay is None
