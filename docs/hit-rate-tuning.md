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

> ⚠️ **这一条与上游的实测策略有冲突，注意 A/B**：上游 `#963`（cc004 实测）给出的结论是
> 「接口访问速度不能超过 3req/s，因此 429 时等 333ms，或'连发 3 请求 + 固定等 1s'」，
> `#1058`/`#1065` 的「代理池并发」正是照这个做的（每 IP 3 条连接、默认并行数 3，作者标注"不建议修改"）。
> 也就是说：**单 IP 3 并发是人家调过的**，我这边改成 1 路是因为那 3 个请求内容完全相同（同 ctoken），
> 判重风险 > 对冲收益。要不要恢复上游行为，用环境变量切：
>
> - `BTB_FANOUT_SAME_SOURCE=0`（默认）：每出口 1 路，靠"更快的间隔"提高命中（见下面 6.x 的速率预算）
> - `BTB_FANOUT_SAME_SOURCE=1`：恢复上游"每 IP 3 条连接并发"，适合你确认多 IP + 低延迟代理时对比

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

### 3.1 412 的处理：不再一撞就冷却 180s，也不无脑每秒硬撞

**先纠正一个容易踩的坑**：412 的真实语义不是"你打太快，慢点就好"，而是
**该 IP（少数情况是该账号）被限流/封禁，社区实测常见 10~15 分钟，请求越猛封得越久**
（见 `discussions#390`「关于抢票时出现的常见问题和个人经验总结」与 `issues#963`）。
所以正确反应是"**换出口 / 换账号**"，而不是原地降速，也不是硬打。

改之前：`_request` 对 412 不抛异常 → 主循环 `.json()` 抛 `JSONDecodeError` → 命中 `"412 风控"` →
`mark_current_proxy_failure` → 阈值 2 次就把当前线路冷却 180 秒 → 单线路换不了线、
`has_available_proxy()` 为假 → `ProxyBackoff` 给 **30 秒**（之后 60/120/240）。
单线路用户开票 2 秒内吃两次 412，接下来 30 秒一个请求都不发。

改之后（`risk_control=True`，分级处理）：

| 情况 | 行为 |
| --- | --- |
| 有多条可用线路 | **把该出口立刻冷却（≥`RISK_CONTROL_PROXY_COOLDOWN_MIN_SECONDS`=600s，覆盖 412 的 10~15 分钟封禁）+ 换线**，不额外等待 |
| 单线路，第 1~2 次 412 | 等 `RISK_CONTROL_SHORT_DELAY_SECONDS`（1s）重试 —— 也可能是瞬时抖动 |
| 单线路，第 3 次起 | 指数退避 30 → 60 → 120 → 180s 封顶，**并在日志里明确提示"IP 可能已被封 10~15 分钟，请切换网络/代理；若换出口仍 412 说明是账号级风控，需换账号"** |
| 收到正常 JSON 响应 | `risk_streak` 清零（说明没被封） |

这样既避免了"一次就睡 30s"的过度惩罚，也避免了我最初想写的"1 秒一次硬撞 15 分钟"
——按社区经验那样只会把封禁拖长。

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
| `RISK_CONTROL_SHORT_DELAY_SECONDS` | `util/Constant.py` | 1 | 412 首次快速重试的等待 |
| `RISK_CONTROL_QUICK_RETRIES` | `util/Constant.py` | 2 | 412 连续几次后转为指数退避 |
| `RISK_CONTROL_BACKOFF_BASE/MAX` | `util/Constant.py` | 30 / 180 | 单线路 412 的指数退避与封顶（秒） |
| `RISK_CONTROL_PROXY_COOLDOWN_MIN_SECONDS` | `util/Constant.py` | 600 | 多线路时 412 命中后该出口的最小冷却（秒） |
| `SINGLE_LINE_MAX_BACKOFF_SECONDS` | `util/Constant.py` | 3 | 单线路失败最长退避 |
| `EXCEPTION_BACKOFF_*` / `RATE_LIMIT_BACKOFF_*` | `util/Constant.py` | 0.2/3.0、0.3/2.4 | 异常与限速退避 |
| `DEFAULT_RATE_LIMIT_DELAY_MS` | `util/Constant.py` | 800（原 300） | HTTP 429 的基础退避 |
| `BTB_RANDOM_URL_PATH` | 环境变量 | 1 | `0` = 关闭 createV2 路径随机编码 |
| `BTB_FANOUT_SAME_SOURCE` | 环境变量 | 0 | `1` = 恢复上游"同一出口 3 条连接并发"策略 |
| `BTB_PROXY_RANDOM_START` | 环境变量 | 1 | `0` = 关闭"多线路随机选取起始代理" |

界面里原有的参数语义没变，仍然可以照常调：`--interval`（每次失败后的间隔）、
`--create-retry-limit`、`--create-request-batch-size`、`--h2-connections-per-source-ip`。

**仍然建议 `--wait-for-buy-button` 保持关闭**：开启后 T-5s 起每 0.5s 一次 `timeout=4.0` 的
页面校验，未命中再补一次 4s 的 POST，判据还是整页 HTML 子串搜"立即购票"，误判一次就等到超时（默认 60s）。

---

## 4.5 这些数字不是拍脑袋：上游 issue/discussion 里的实测结论

改动前先查了仓库的 issue / PR / discussion，下面几条直接决定参数怎么设：

- **`issues#963`（cc004，实测）**：单 IP「接口访问速度不能超过 **3 req/s**」；429 时应等
  **333ms**，或用「连发 3 请求 + 固定等 1s」的策略。→ 这就是默认 `rate_limit_delay_ms`
  与「代理池并发默认 3 条连接」（`#1058` / `#1065`，作者标注"不建议修改"）的由来。
- **`discussions#390`（社区 BW2026 版 FAQ）**：
  * 会员购有 **1 秒盾**：同账号同场次 1 秒内只能有 1 次有效请求，**与 IP 无关**；
  * 所以请求间隔低于 1000ms **不会提高有效请求数**，只是提高"卡在 1s 窗口边界上"的概率，
    因此推荐用 1000 **的整除数**：200 / **250** / 500ms；
  * 社区参数：抢票间隔 **250ms** + 429 延迟 **300ms**，单 IP 单账号单程序实测不 412；
  * 412 一般封 **10 分钟**左右，越猛越久；账号级 412 只能等；
  * 「**一 IP 一账号一程序**」；429 太多时才让一个账号用多个 IP（突破 429）；
  * 抢未开售场次会触发 `100041` → 账号被封会员购 5 分钟起，**别提前开抢**。
- **`issues#685`**：同一 IP 多开多个配置，立刻 412 且 24 小时才解开。
- **`pr#1028`（作者实测）**：8 个 IP → 单账号平均 1.4s 摸一次内层；16 IP → 1.1s。

把上面的结论套进本项目的参数：

| 你的场景 | 建议 |
| --- | --- |
| 单账号 + 单 IP（直连） | `--interval 250`（1000 的整除数）+ `rate_limit_delay_ms 300`；先按 1000 跑一场确认不 412 再降 |
| 多账号 + 每号一个独享 IP | 每号一套 interval 250~1000ms；`--create-request-proxy-strategy local_fanout`；`h2_connections_per_source_ip` 保持 3 |
| 想要"多发几次提高抽中率" | 别在同 IP 上并发；加 IP 比提高间隔更有效（1s 盾是账号级，加 IP 只解决 412/429） |

> 因此我前面那份专项报告里"前 3 秒降到 250~400ms 提高 QPS"的说法要**收窄**成：
> **250ms 是社区实测的稳妥下限，且必须是 1000 的整除数；再快没有收益（1s 盾），只会加速 412。**

## 4.6 短效代理池（30~60 分钟有效期）怎么配最省

短效代理（提取后 30~60 分钟有效）是性价比最高的玩法，但它有两个特性必须照顾：
**有效期从提取/首次连接开始计时**，以及 **412 之后这个 IP 基本就废了（10~15 分钟封禁）**。

1. **数量**：按「一 IP 一账号」算，再乘 3~5 倍冗余。例：5 个账号 × 有效期 30min × 每 IP 挨一发 412 就废
   → 每个号准备 4~6 个 IP，总共 20~30 个；50 个 IP 跑 10 个号是合理区间。
2. **`proxy_cooldown_seconds` 调到 600~900**：默认 180s 比 412 的封禁时长短，
   会让程序在封禁期内又选回同一个 IP（本分支已经把 412 的冷却强制拉到 ≥600s，这里保持一致）。
3. **别用「测试代理连通性」测整池**：那个按钮会让每个 IP 都发真实请求，
   短效代理商那边就开始计时/扣量。最多抽 1~2 条验证格式；要验证"代理能不能访问 B 站"，
   直接开跑看日志里的 412 率更准。
4. **用「获取并填入代理」而不是手动粘**：把服务商后台的提取 API/demo 链接粘贴到「代理 API 地址」，
   协议选「自动识别」，点「获取并填入代理」——它会一次性拉取并**覆盖**代理列表。
   提取数量取 `queueConcurrencyLimit`（代理池并发数），没设则取当前代理列表的行数，
   所以先把行数/并发数设成你想要的 IP 数再点。
5. **什么时候点**：**开票前 2~3 分钟点一次**，不要提前半小时（有效期从提取开始算）。
6. **多任务不要从同一条代理起跑**：每个任务是一个独立进程，各自持有一份同一顺序的代理列表；
   上游实现的起始索引固定是 0，10 个任务同时开跑会把**同一条 IP** 瞬间打爆（立刻 412）。
   本分支把起始线路改成按列表长度随机（单线路无影响，`BTB_PROXY_RANDOM_START=0` 可关闭）。
7. **自动补池的副作用**：池子耗尽时（全部冷却）程序会自动调一次代理 API，
   但那是**同步请求（超时 15s）且发生在失败处理链里**——开票窗口里最坏会卡 15 秒。
   如果你预计池子会在 T0 附近耗尽，手动提前补一次更稳。

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
