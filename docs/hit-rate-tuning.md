# 抢票成功率 / 风控调优说明

这份分支（`feat/hit-rate-tuning`）只做一件事：**在不改安全模型、不改协议的前提下，把"首发命中时间 / 请求重复度 / 被限流后的恢复策略"这三件事修对**。

改动全部集中在 6 个文件，其余代码未动。

---

## 1. 首发命中时间

### 1.1 预热从 T-5s 挪到 T-30s

`util/Constant.py`：`WARMUP_AT_SECONDS` 5.0 → 30.0。

原来 `_wait_until_start` 在离开票 5 秒时于**倒计时同一线程**里触发 `refresh_hot_and_warm()`，
里面是 `fetch_project_payload` + `prewarm_h2_connection` 两个网络往返（`task/buy.py`）。
网络一慢就把后面的 `POST /order/prepare` 推到 T0 之后 —— 首发包天然晚一个 RTT 已经够亏了，不能再叠加。

### 1.2 预热点亮整个连接池

`util/h2client/ja_h2_client.py` 新增 `warm_connections(url)`；`BiliRequest.prewarm_h2_connection`
在 HEAD 之后调用它，把池里每条连接都 `connect()`。

原来只发一个 HEAD，而选路是 `random.choice`，所以开票前池里只有 1 条连接是热的；
`local_fanout` 的其余几路会在 T0 现场做 TCP + TLS1.3 握手，第一秒的对冲等于不存在。

### 1.3 连接丢了会补，且不在锁外关闭

* `_ensure_pool` 从"有池就直接返回"改成"按每个 source 应有多少条补齐"；
* `_discard_slot` 的 `close()` 挪进 `slot.lock` 内。

原来一次 412/异常就永久少一条连接（池空之前不会重建），几次之后退化成单连接串行；
关闭又不在锁内，会打断别的线程正在读的连接，形成"丢一条 → 误伤一条 → 再丢"的雪崩。

### 1.4 fanout 只在多出口时并发

`_fanout_slots()`：每个出口 source 只保留一条连接参与对冲；只剩 1 个出口时直接走串行路径。

原来单代理/直连会把**同一个请求（同 ctoken、同 header、同 cookie）**在毫秒内从同一个 IP
并发发 3 次 —— 比串行更像"机器并发提交"，只会更快触发风控。多出口时仍然对冲（这才是它的用途）。

---

## 2. 请求重复度

### 2.1 每次 create 都重算 payload

`task/buy.py`：`_prepare_create_request` 从 batch 外层移进每次 attempt。

原来一个 batch 内 N 次 createV2 共用同一份 `ctoken` / `timestamp` / `ptoken`。
ctoken 是一次性风控 token，重放是最直白的机器特征。顺带修掉 `100034 票价错误` 之后
batch 内仍按旧价继续打的问题（新价在下一次 payload 里自动生效）。

### 2.2 所有等待加抖动

`task/buy_helpers.jittered_seconds()`，比例 `RETRY_JITTER_RATIO = 0.25`。
间隔 sleep、异常退避、429 退避全部走它。固定 1000ms ± 0ms 的周期是最容易被识别的特征之一。

### 2.3 限速类错误码单独处理

`util/ErrorCodes.RATE_LIMIT_CODES = {3, 221, 900001, 900002}`（下单过于频繁 / 请求过多 / 下单过快）。

`task/buy.py` 命中时走 `rate_limit_backoff_seconds()`：0.3 → 0.6 → 1.2 → 2.4s（带抖动）。
原来这四个码和 `100009 库存不足` 走同一条路径 —— 对"你打太快了"的告警不减速，只会升级成 412。

### 2.4 createV2 路径随机编码做成开关

`BiliRequest._random_url_path_enabled()`，环境变量 `BTB_RANDOM_URL_PATH`，默认 `1` 保持原行为。

真实浏览器不会把 `/api/ticket/order/createV2` 随机编码成 `cre%61teV2`，而 prepare 又不这么做。
建议实测对比：`BTB_RANDOM_URL_PATH=0` 跑一场，看 412 率有没有变好；没有就关掉。

---

## 3. 被限流后的恢复策略（本次最关键的改动）

### 3.1 412 不再冷却线路

`task/buy_helpers.handle_proxy_failure(..., risk_control=True)`，`task/buy.py` 的 412 分支传入。

改之前：`_request` 对 412 不抛异常 → 主循环 `.json()` 抛 `JSONDecodeError` → 命中 `"412 风控"` →
`mark_current_proxy_failure` → **阈值 2 次就把当前线路冷却 180 秒** → 单线路换不了线、
`has_available_proxy()` 为假 → `ProxyBackoff` 给 **30 秒**（之后 60/120/240）。
结果是直连用户开票 2 秒内吃两次 412，接下来 30 秒一个请求都不发。

改之后（`risk_control=True`）：

* 不把当前线路记为失败；
* 有备用线路（`available_proxy_count() > 1`）才换线，换完 `proxy_backoff.reset()`；
* 单线路原地降速，等待 `RISK_CONTROL_SHORT_DELAY_SECONDS = 1` 秒。

412 的判定同时改成看 `response.status_code == 412`，不再依赖 `"status=412"` / `"412 风控"`
两次字符串匹配（原来改一个空格就静默失效）。

### 3.2 单线路线路失败也不长睡

`handle_proxy_failure` 在"没有可用备用线路"时，退避从 `ProxyBackoff` 的 30/60/120/240s
改成 `SINGLE_LINE_MAX_BACKOFF_SECONDS = 3` 秒。

真·线路坏了（连接超时/TLS 失败）仍然会记失败、仍然会冷却 —— 只是不再把唯一线路按 30 秒起步惩罚。

### 3.3 异常分支统一退避

`task/buy.py` 的 `JSONDecodeError`（非 412）、`RequestException`、通用 `Exception`、
以及外层 `BiliConnectionError` / `Exception` 全部走 `exception_backoff_seconds(fail_streak)`：
0.2 → 0.4 → 0.8 → … → 3.0s（带抖动，成功响应清零）。

原来这些分支**完全不 sleep**，一次结构性异常（比如返回体缺字段，`int(ret.get("errno", ...))`
直接 TypeError）就会变成零间隔 `prepare → create` 无限空转 —— 这是最快把账号打进风控的写法。
顺带把 errno 提取改成 `extract_errno()`，缺字段时返回 -1 而不是抛异常。

---

## 4. 改完怎么调参

| 参数 | 位置 | 默认 | 说明 |
| --- | --- | --- | --- |
| `WARMUP_AT_SECONDS` | `util/Constant.py` | 30.0 | 预热提前多少秒 |
| `RETRY_JITTER_RATIO` | `util/Constant.py` | 0.25 | 等待抖动比例 |
| `RISK_CONTROL_SHORT_DELAY_SECONDS` | `util/Constant.py` | 1 | 412 后的降速等待 |
| `SINGLE_LINE_MAX_BACKOFF_SECONDS` | `util/Constant.py` | 3 | 单线路失败最长退避 |
| `EXCEPTION_BACKOFF_*` / `RATE_LIMIT_BACKOFF_*` | `util/Constant.py` | 0.2/3.0、0.3/2.4 | 异常与限速退避 |
| `DEFAULT_RATE_LIMIT_DELAY_MS` | `util/Constant.py` | 800（原 300） | HTTP 429 的基础退避 |
| `BTB_RANDOM_URL_PATH` | 环境变量 | 1 | `0` = 关闭 createV2 路径随机编码 |

界面里原有的参数语义没变，仍然可以照常调：`--interval`（每次失败后的间隔）、
`--create-retry-limit`、`--create-request-batch-size`、`--h2-connections-per-source-ip`。

**仍然建议 `--wait-for-buy-button` 保持关闭**：开启后 T-5s 起每 0.5s 一次 `timeout=4.0` 的
页面校验，未命中再补一次 4s 的 POST，判据还是整页 HTML 子串搜"立即购票"，误判一次就等到超时（默认 60s）。

---

## 5. 怎么验证改动有没有用

1. **跑测试**：`python -m pytest tests/test_hit_rate_tuning.py -q`（新增，纯逻辑、不联网）。
2. **看日志关键词**：
   * `H2 warm_connections ... warmed=N/N` —— 开票前池子是否全热；
   * `被风控，降速重试` / `无备用线路可用，3 秒后继续用当前线路重试` —— 说明新的恢复策略在生效；
   * `触发限速，退避 x.xxs 后继续` —— 说明限速码分支命中（改之前这里会直接照原节奏继续打）。
3. **对比实验**：同一场票，`BTB_RANDOM_URL_PATH=1` vs `0`；单代理 vs 多代理；`--interval 1000` vs `350`。
   记录 412 次数、900001/221 次数、以及"首发 createV2 时间 − 开票时间"。

## 6. 本次没有改的

* 安全模型（Web UI 鉴权、日志里的 cookie、TLS 证书校验、更新器校验）——按需另行处理；
* 下单成功后的收尾异常仍可能回流到下一轮（`task/buy.py` 的成功分支与外层 `try` 共享）——
  这是重复下单风险，改动比本次大，建议单独一轮处理；
* `isRunning` 死标志 / 进程内取消通道 —— 同上。
