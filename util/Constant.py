import datetime


BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8), name="Asia/Shanghai")
GO_UPLOADED_FILES_STATE_KEY = "go.uploaded_config_files"
DEFAULT_REQUEST_INTERVAL = 1000
DEFAULT_RATE_LIMIT_DELAY_MS = 800
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

# 请求间隔抖动比例：固定周期的请求串是最容易被识别的机器特征之一。
RETRY_JITTER_RATIO = 0.25

# 命中 412（风控）后的短退避秒数。
# 412 的含义是"打太快了"，正确反应是降速重试；把唯一线路冷却 30~240 秒等于放弃本场。
RISK_CONTROL_SHORT_DELAY_SECONDS = 1

# 只有一条线路（直连/单代理）时，线路级失败的最长退避秒数。
# 原逻辑在这种情况下会走 ProxyBackoff 的 30/60/120/240s，开票窗口里等于出局。
SINGLE_LINE_MAX_BACKOFF_SECONDS = 3

# 异常分支的统一退避（原来异常路径完全不 sleep，会变成零间隔空转）。
EXCEPTION_BACKOFF_BASE_SECONDS = 0.2
EXCEPTION_BACKOFF_MAX_SECONDS = 3.0

# 限速类错误码（下单过快/请求过多）的短指数退避。
RATE_LIMIT_BACKOFF_BASE_SECONDS = 0.3
RATE_LIMIT_BACKOFF_MAX_SECONDS = 2.4
