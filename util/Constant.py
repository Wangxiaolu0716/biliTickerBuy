import datetime


BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8), name="Asia/Shanghai")
GO_UPLOADED_FILES_STATE_KEY = "go.uploaded_config_files"
DEFAULT_REQUEST_INTERVAL = 1000
DEFAULT_RATE_LIMIT_DELAY_MS = 300
DEFAULT_CREATE_REQUEST_BATCH_SIZE = 3
DEFAULT_PROXY_MAX_CONSECUTIVE_FAILURES = 2
DEFAULT_PROXY_COOLDOWN_SECONDS = 180
DEFAULT_PROXY_BACKOFF_MAX_SECONDS = 600
DEFAULT_LOG_RETENTION_DAYS = 7
DEFAULT_MAX_LOG_FILES = 200
DEFAULT_MAX_RUN_DIRS = 100
BASE_URL = "https://show.bilibili.com"
# 预热/复检在离开票多少秒前触发。留 30s 而不是 5s：预热本身要发两个网络往返，
# 放在 T-5s 会挤占首发 createV2 的时间，网络慢时甚至把 prepare 推到 T0 之后。
WARMUP_AT_SECONDS = 30.0
COUNTDOWN_REPORT_INTERVAL_SECONDS = 15
DEFAULT_CREATE_RETRY_LIMIT = 20
DEFAULT_OUTER_LOOP_INTERVAL = 0
UPDATE_CHANNEL_KEY = "update_channel"
PACKAGE_NAME = "bilitickerbuy"
_LOG_VIEW_ROUTE = "/__btb/logs/view"
_LOG_STREAM_ROUTE = "/__btb/logs/stream"
MEOW_API_BASE = "https://api.chuckfang.com"
DEFAULT_TIMEOUT = (3.05, 8)
H2_TIMEOUT = {
    "connect": 3.05,
    "read": 5.0,
    "write": 5.0,
    "pool": 5.0,
}
H2_LIMITS = {
    "max_keepalive_connections": 10,
    "max_connections": 20,
    "keepalive_expiry": 60.0,
}

# ---------------------------------------------------------------- #
# 抢票成功率 / 风控调优参数（改这些不需要动业务代码）
# ---------------------------------------------------------------- #

# 命中 412（风控）后的处理策略。
# 社区实测（discussions#390 / issues#963）语义是：412 = 该 IP（或账号）被限流/封禁，
# 常见 10~15 分钟；请求越猛封越久。所以既不能"1 秒一次硬撞 15 分钟"，也不能"一撞就睡 30~240 秒"：
# 先快速重试 RISK_CONTROL_QUICK_RETRIES 次（可能只是瞬时抖动），仍 412 再按指数退避到上限，
# 并在消息里提示"换网络/换代理/换账号"——这才是能真正解开 412 的动作。
RISK_CONTROL_SHORT_DELAY_SECONDS = 1
RISK_CONTROL_QUICK_RETRIES = 2
RISK_CONTROL_BACKOFF_BASE_SECONDS = 30
RISK_CONTROL_BACKOFF_MAX_SECONDS = 180
# 多线路时，412 命中就把该出口冷却这么久（至少），不再等失败阈值：
# 412 是 IP 级封禁，社区实测 10~15 分钟，180s 太短会让程序反复选回已封的 IP。
RISK_CONTROL_PROXY_COOLDOWN_MIN_SECONDS = 600

# 只有一条线路（直连/单代理）时，线路级失败的最长退避秒数。
# 原逻辑在这种情况下会走 ProxyBackoff 的 30/60/120/240s，开票窗口里等于出局。
SINGLE_LINE_MAX_BACKOFF_SECONDS = 3

# 429 换出口的判据：**同一条 IP 连续 429 且持续 ≥N 秒且至少 M 次** 就换下一条。
#
# 为什么不用固定次数：次数阈值和 interval 挂钩（250ms 下 6 次才 1.5 秒，1000ms 下 6 次已经 6 秒），
# 时间窗对任何节奏都稳定。
# 为什么不能"1 次就换"：429 是概率性放行，不是"IP 被拉黑"（那是 412）。
# 实测成功样本：同一 IP 连续 5 次 429 后第 6 发抢到；1 次就换会直接打断这条成功路径。
RATE_LIMIT_ROTATE_MIN_SECONDS = 1.5
RATE_LIMIT_ROTATE_MIN_COUNT = 3

# 把整个池子轮过一圈仍然全是 429（说明整池都被 CDN 限速）时，暂停这么久再继续，
# 避免在 250ms 节奏下把所有出口一起打进更重的限速等级。
RATE_LIMIT_ALL_EXHAUSTED_SLEEP_SECONDS = 1.5

# 注：HTTP 429 的重试间隔用 config.rate_limit_delay_ms（默认 DEFAULT_RATE_LIMIT_DELAY_MS=300ms）。
# 依据 issues#963 + discussions#390：429 应"等 333ms 继续"，**不能指数退避**
# —— 429 只是没抽中，退避会直接减少有效请求次数（实测指数退避让每分钟尝试从 240 掉到 25）。
