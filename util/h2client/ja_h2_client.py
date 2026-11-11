from __future__ import annotations

import json
import os
import random
import socket
import threading
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import httpx
from loguru import logger

try:
    from .abstract_h2_client import AbstractH2Client
    from .constants import (
        H2CLIENT_CONNECTIONS_PER_SOURCE_IP,
        H2CLIENT_HEALTHCHECK_HOST,
        H2CLIENT_HEALTHCHECK_TIMEOUT,
        H2CLIENT_HEALTHCHECK_URL,
    )
    from .h2connection import AddressFamily, H2Connection, H2Response
except ImportError:  # pragma: no cover - supports direct import from util/h2client.
    from abstract_h2_client import AbstractH2Client
    from constants import (
        H2CLIENT_CONNECTIONS_PER_SOURCE_IP,
        H2CLIENT_HEALTHCHECK_HOST,
        H2CLIENT_HEALTHCHECK_TIMEOUT,
        H2CLIENT_HEALTHCHECK_URL,
    )
    from h2connection import AddressFamily, H2Connection, H2Response


HeaderItems = Mapping[str, str] | Sequence[tuple[str, str]]
SourceIPProvider = Callable[[], list["SourceAddress"]]
ConnectionFactory = Callable[..., H2Connection]
SlotChooser = Callable[[Sequence["ConnectionSlot"]], "ConnectionSlot"]
FallbackClientFactory = Callable[..., Any]


def _normalize_fanout_sources(
    proxy_pool: Sequence[str | None] | None,
    proxy: str | None,
) -> list[str | None]:
    raw_values: list[str | None]
    if proxy_pool is not None:
        raw_values = list(proxy_pool)
    elif proxy:
        raw_values = [item.strip() for item in str(proxy).split(",")]
    else:
        raw_values = []

    values: list[str | None] = []
    seen: set[str] = set()
    for raw in raw_values:
        if raw is None:
            item = ""
        else:
            item = str(raw).strip()
        if not item or item.lower() in {"none", "direct"}:
            key = "direct"
            value = None
        else:
            key = item.lower()
            value = item
        if key in seen:
            continue
        seen.add(key)
        values.append(value)
    return values


@dataclass(frozen=True)
class SourceAddress:
    ip: str
    family: AddressFamily
    interface_alias: str = ""
    proxy_url: str | None = None

    @property
    def display(self) -> str:
        return self.proxy_url or self.ip

    @property
    def bind_ip(self) -> str | None:
        if self.interface_alias == "direct":
            return None
        return self.ip


@dataclass
class ConnectionSlot:
    source: SourceAddress
    connection: H2Connection
    lock: threading.Lock = field(default_factory=threading.Lock)

    def close(self) -> None:
        self.connection.close()


@dataclass(frozen=True)
class FanoutOutcome:
    slot: ConnectionSlot
    response: httpx.Response | None = None
    exc: Exception | None = None


@dataclass(frozen=True)
class HealthcheckOutcome:
    source: SourceAddress
    attempt: int
    ok: bool


def _dedupe_sources(sources: Sequence[SourceAddress]) -> list[SourceAddress]:
    seen: set[tuple[str, AddressFamily]] = set()
    result: list[SourceAddress] = []
    for source in sources:
        key = (source.ip, source.family)
        if key in seen:
            continue
        seen.add(key)
        result.append(source)
    return result


def _append_params(url: str, params: Any) -> str:
    if not params:
        return url
    parsed = urllib.parse.urlsplit(url)
    query = parsed.query
    if isinstance(params, str):
        new_query = params
    else:
        new_query = urllib.parse.urlencode(params, doseq=True)
    query = f"{query}&{new_query}" if query and new_query else query or new_query
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment)
    )


def _json_body(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _form_body(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return urllib.parse.urlencode(value, doseq=True).encode("utf-8")


def _header_mapping(headers: HeaderItems | None) -> httpx.Headers:
    return httpx.Headers(headers or {})


def _set_default_header(headers: httpx.Headers, name: str, value: str) -> None:
    if name not in headers:
        headers[name] = value


def _timeout_seconds(timeout: Any, default: float) -> float:
    if timeout is None:
        return default
    if isinstance(timeout, (int, float)):
        return float(timeout)
    values = [
        value
        for value in (
            getattr(timeout, "connect", None),
            getattr(timeout, "read", None),
            getattr(timeout, "write", None),
        )
        if isinstance(value, (int, float))
    ]
    return float(max(values)) if values else default


def _content_length(content: bytes | str | None) -> int:
    if content is None:
        return 0
    if isinstance(content, bytes):
        return len(content)
    return len(content.encode("utf-8"))


def _response_errno(response: httpx.Response) -> int | None:
    try:
        payload = response.json()
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    raw_errno = payload.get("errno", payload.get("code"))
    if raw_errno is None:
        return None
    try:
        return int(raw_errno)
    except (TypeError, ValueError):
        return None


class _SourcePoolJA3H2Client(AbstractH2Client):
    def __init__(
        self,
        *,
        http2: bool = True,
        verify: bool | str = True,
        proxy: str | None = None,
        timeout: Any = None,
        limits: Any = None,
        headers: HeaderItems | None = None,
        cookies: Any = None,
        connections_per_source_ip: int = H2CLIENT_CONNECTIONS_PER_SOURCE_IP,
        healthcheck_url: str = H2CLIENT_HEALTHCHECK_URL,
        healthcheck_host: str = H2CLIENT_HEALTHCHECK_HOST,
        source_ip_provider: SourceIPProvider | None = None,
        connection_factory: ConnectionFactory = H2Connection,
        slot_chooser: SlotChooser | None = None,
        fallback_client_factory: FallbackClientFactory = httpx.Client,
        **_: Any,
    ) -> None:
        if not http2:
            raise ValueError("_SourcePoolJA3H2Client only supports HTTP/2")
        if source_ip_provider is None:
            raise ValueError(
                "source_ip_provider is required; use "
                "ProxyPoolCreateV2FanoutJA3H2Client for proxy-pool fanout"
            )

        self.http2 = http2
        self.verify = verify
        self.proxy = proxy
        self.limits = limits
        fallback_kwargs: dict[str, Any] = {
            "http2": http2,
            "verify": verify,
            "headers": headers,
            "cookies": cookies,
        }
        if proxy is not None:
            fallback_kwargs["proxy"] = proxy
        if timeout is not None:
            fallback_kwargs["timeout"] = timeout
        if limits is not None:
            fallback_kwargs["limits"] = limits
        self._fallback_client = fallback_client_factory(**fallback_kwargs)
        self._headers = self._fallback_client.headers
        self._cookies = self._fallback_client.cookies
        self._connections_per_source_ip = max(1, int(connections_per_source_ip))
        self._healthcheck_url = healthcheck_url
        self._healthcheck_host = healthcheck_host
        self._timeout = _timeout_seconds(timeout, H2CLIENT_HEALTHCHECK_TIMEOUT)
        self._source_ip_provider = source_ip_provider
        self._connection_factory = connection_factory
        self._slot_chooser = slot_chooser or random.choice
        self._lock = threading.RLock()
        self._available_sources: list[SourceAddress] = []
        self._pools: dict[tuple[str, int], list[ConnectionSlot]] = {}

        try:
            self._init_healthchecked_pool()
        except Exception:
            self._fallback_client.close()
            raise

    @property
    def headers(self) -> httpx.Headers:
        return self._headers

    @property
    def cookies(self) -> httpx.Cookies:
        return self._cookies

    @property
    def available_source_ips(self) -> list[str]:
        return [source.ip for source in self._available_sources]

    def close(self) -> None:
        with self._lock:
            slots = [slot for pool in self._pools.values() for slot in pool]
            self._pools.clear()
            self._available_sources.clear()
        for slot in slots:
            slot.close()
        self._fallback_client.close()

    def head(self, url: str) -> httpx.Response:
        if self._should_use_fallback(url):
            return self._fallback_client.head(url)
        response = self.get(url)
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            content=b"",
            request=response.request,
        )

    def get(
        self,
        url: str,
        *,
        params: Any = None,
        headers: HeaderItems | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        if self._should_use_fallback(url):
            return self._fallback_client.get(
                url,
                params=params,
                headers=headers,
                **kwargs,
            )
        return self._request("GET", _append_params(url, params), headers=headers)

    def post(
        self,
        url: str,
        *,
        data: Any = None,
        json: Any = None,
        content: bytes | str | None = None,
        headers: HeaderItems | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        if self._should_use_fallback(url):
            return self._fallback_client.post(
                url,
                data=data,
                json=json,
                content=content,
                headers=headers,
                **kwargs,
            )
        request_headers = _header_mapping(headers)
        if json is not None:
            body = _json_body(json)
            _set_default_header(request_headers, "content-type", "application/json")
        elif content is not None:
            body = content if isinstance(content, bytes) else content.encode("utf-8")
        else:
            body = _form_body(data)
            if data is not None and not isinstance(data, (bytes, str)):
                _set_default_header(
                    request_headers,
                    "content-type",
                    "application/x-www-form-urlencoded",
                )
        return self._request("POST", url, headers=request_headers, content=body)

    def _should_use_fallback(self, url: str) -> bool:
        parsed = urllib.parse.urlsplit(url)
        return (parsed.hostname or "").lower() != self._healthcheck_host.lower()

    def _init_healthchecked_pool(self) -> None:
        sources = _dedupe_sources(self._source_ip_provider())
        if not sources:
            raise httpx.ConnectError("no usable H2 sources were provided")

        key = (self._healthcheck_host, 443)
        passed: dict[SourceAddress, int] = {source: 0 for source in sources}
        max_workers = max(1, min(len(sources) * self._connections_per_source_ip, 64))
        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="btb-h2-healthcheck",
        ) as executor:
            futures = [
                executor.submit(self._healthcheck_source_once, source, attempt)
                for source in sources
                for attempt in range(1, self._connections_per_source_ip + 1)
            ]
            for future in as_completed(futures):
                outcome = future.result()
                if outcome.ok:
                    passed[outcome.source] += 1

        available_sources = [source for source in sources if passed[source] > 0]
        if not available_sources:
            raise httpx.ConnectError("no H2 source passed healthcheck")

        self._available_sources = available_sources
        self._pools[key] = [
            ConnectionSlot(
                source=source,
                connection=self._build_connection(self._healthcheck_host, 443, source),
            )
            for source in available_sources
            for _ in range(self._connections_per_source_ip)
        ]
        logger.info(
            "H2 healthcheck completed host={} usable_sources={} pool_slots={}",
            self._healthcheck_host,
            len(self._available_sources),
            len(self._pools[key]),
        )

    def _healthcheck_source_once(
        self,
        source: SourceAddress,
        attempt: int,
    ) -> HealthcheckOutcome:
        connection = self._build_connection(self._healthcheck_host, 443, source)
        try:
            response = connection.get(
                self._healthcheck_url,
                headers=self._request_headers("GET", self._healthcheck_url),
            )
            ok = response.status is not None and response.status < 500
            if ok:
                logger.debug(
                    "H2 healthcheck passed source_ip={} attempt={} status={}",
                    source.ip,
                    attempt,
                    response.status,
                )
            else:
                logger.warning(
                    "H2 healthcheck rejected source_ip={} attempt={} status={}",
                    source.ip,
                    attempt,
                    response.status,
                )
            return HealthcheckOutcome(source=source, attempt=attempt, ok=ok)
        except Exception as exc:
            logger.warning(
                "H2 healthcheck failed source_ip={} attempt={} error={}: {}",
                source.ip,
                attempt,
                exc.__class__.__name__,
                exc,
            )
            return HealthcheckOutcome(source=source, attempt=attempt, ok=False)
        finally:
            connection.close()

    def _build_connection(
        self,
        host: str,
        port: int,
        source: SourceAddress,
    ) -> H2Connection:
        return self._connection_factory(
            host,
            source.bind_ip,
            port=port,
            sni=host,
            family=source.family,
            timeout=self._timeout,
            proxy_url=source.proxy_url,
            assert_ja=True,
        )

    def _ensure_pool(self, host: str, port: int) -> list[ConnectionSlot]:
        key = (host, port)
        with self._lock:
            pool = self._pools.get(key)
            if pool is None:
                pool = []
                self._pools[key] = pool

            # 按"每个 source 应有多少条"补齐，而不是有池就直接返回。
            # 原实现里 discard 掉的连接永远不补，几次 412/异常之后就退化成单连接串行，
            # 开票窗口内的并发度会莫名其妙掉到 1/N。
            counts: dict[str, int] = {}
            for slot in pool:
                counts[slot.source.ip] = counts.get(slot.source.ip, 0) + 1
            for source in self._available_sources:
                missing = self._connections_per_source_ip - counts.get(source.ip, 0)
                for _ in range(max(0, missing)):
                    pool.append(
                        ConnectionSlot(
                            source=source,
                            connection=self._build_connection(host, port, source),
                        )
                    )

            if not pool:
                raise httpx.ConnectError(f"no available H2 connections for {host}")
            return pool

    def warm_connections(self, url: str) -> int:
        """把连接池里所有连接都握手点亮，返回成功建连数。

        ``prewarm_h2_connection`` 只发一个 HEAD，而 ``_slot_chooser`` 是随机挑一条，
        因此开票前池里通常只有一条连接是热的；fanout 的其余几路会在 T0 现场做
        TCP + TLS1.3 握手，第一秒的对冲等于不存在。
        """

        parsed = urllib.parse.urlsplit(url)
        host = parsed.hostname or self._healthcheck_host
        port = parsed.port or 443
        try:
            pool = list(self._ensure_pool(host, port))
        except Exception:  # noqa: BLE001 - 预热失败不影响正式流程
            return 0

        warmed = 0
        for slot in pool:
            try:
                with slot.lock:
                    slot.connection.connect()
                warmed += 1
            except Exception:  # noqa: BLE001 - 单条连接预热失败不影响其它连接
                continue
        if warmed:
            logger.info(
                "H2 warm_connections host={}:{} slots={}/{} warmed",
                host,
                port,
                warmed,
                len(pool),
            )
        return warmed

    def _discard_slot(
        self,
        host: str,
        port: int,
        slot: ConnectionSlot,
        *,
        reason: str = "unspecified",
        exc: Exception | None = None,
    ) -> None:
        removed = False
        with self._lock:
            pool = self._pools.get((host, port), [])
            if slot in pool:
                pool.remove(slot)
                removed = True
        if exc is None:
            logger.warning(
                "H2 discard connection source_ip={} host={}:{} reason={} removed={}",
                slot.source.ip,
                host,
                port,
                reason,
                removed,
            )
        else:
            logger.warning(
                "H2 discard connection source_ip={} host={}:{} "
                "reason={} removed={} error={}: {}",
                slot.source.ip,
                host,
                port,
                reason,
                removed,
                exc.__class__.__name__,
                exc,
            )
        # 关闭必须在 slot 锁内：否则可能把另一个线程正在读同一条连接的请求打断，
        # 那个请求随后报异常、又被当成"线路失败"再丢一条，形成雪崩。
        try:
            with slot.lock:
                slot.close()
        except Exception:  # noqa: BLE001 - 关闭失败无所谓，连接已经不在池里
            pass

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: HeaderItems | None = None,
        content: bytes | str | None = None,
    ) -> httpx.Response:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("only absolute https:// URLs are supported")
        host = parsed.hostname
        port = parsed.port or 443
        request = httpx.Request(method, url)
        request_headers = self._request_headers(
            method,
            url,
            headers=headers,
            content=content,
        )

        last_exc: Exception | None = None
        failed_sources: set[str] = set()
        while True:
            pool = [
                slot
                for slot in self._ensure_pool(host, port)
                if slot.source.ip not in failed_sources
            ]
            if not pool:
                break

            slot = self._slot_chooser(pool)
            try:
                with slot.lock:
                    if method == "GET":
                        h2_response = slot.connection.get(url, headers=request_headers)
                    else:
                        h2_response = slot.connection.post(
                            url,
                            headers=request_headers,
                            content=content,
                        )
                return self._to_httpx_response(request, h2_response)
            except Exception as exc:
                last_exc = exc
                failed_sources.add(slot.source.ip)
                self._discard_slot(
                    host,
                    port,
                    slot,
                    reason="request_exception",
                    exc=exc,
                )

        raise self._to_httpx_error(method, url, last_exc)

    def _request_headers(
        self,
        method: str,
        url: str,
        headers: HeaderItems | None = None,
        content: bytes | str | None = None,
    ) -> httpx.Headers:
        request_headers = httpx.Headers(self._headers)
        request_headers.update(headers or {})
        request = httpx.Request(method, url, headers=request_headers)
        self._cookies.set_cookie_header(request)
        if method.upper() == "POST":
            request.headers["content-length"] = str(_content_length(content))
        return request.headers

    def _to_httpx_response(
        self,
        request: httpx.Request,
        response: H2Response,
    ) -> httpx.Response:
        headers = [
            (name, value)
            for name, value in response.headers
            if not name.startswith(":")
        ]
        return httpx.Response(
            response.status or 0,
            headers=headers,
            content=response.body,
            request=request,
        )

    def _to_httpx_error(
        self,
        method: str,
        url: str,
        exc: Exception | None,
    ) -> httpx.HTTPError:
        request = httpx.Request(method, url)
        if exc is None:
            return httpx.ConnectError("no available H2 connections", request=request)
        message = str(exc) or exc.__class__.__name__
        if isinstance(exc, (socket.timeout, TimeoutError)):
            return httpx.TimeoutException(message, request=request)
        if isinstance(exc, OSError):
            return httpx.ConnectError(message, request=request)
        return httpx.LocalProtocolError(message, request=request)


class _CreateV2FanoutJA3H2Client(_SourcePoolJA3H2Client):
    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: HeaderItems | None = None,
        content: bytes | str | None = None,
    ) -> httpx.Response:
        if not self._should_fanout_create_v2(url):
            return super()._request(method, url, headers=headers, content=content)

        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("only absolute https:// URLs are supported")
        host = parsed.hostname
        port = parsed.port or 443
        request = httpx.Request(method, url)
        request_headers = self._request_headers(
            method,
            url,
            headers=headers,
            content=content,
        )
        pool = list(self._ensure_pool(host, port))
        if not pool:
            raise httpx.ConnectError(
                f"no available H2 connections for {host}",
                request=request,
            )

        # 只有一条出口 IP 时不做并发对冲：同账号、同 ctoken、同 cookie 的几个完全相同的
        # createV2 在毫秒内从同一个 IP 打出去，只会比串行更像"机器并发提交"，
        # 反而更容易被判定为重复/异常并发。多出口时才按 source 各发一路。
        pool = self._fanout_slots(pool)
        if len(pool) <= 1:
            return super()._request(method, url, headers=headers, content=content)

        executor = ThreadPoolExecutor(
            max_workers=len(pool),
            thread_name_prefix="btb-createv2-fanout",
        )
        futures = [
            executor.submit(
                self._send_fanout_request,
                slot,
                method,
                url,
                request_headers,
                content,
                request,
            )
            for slot in pool
        ]
        for future in futures:
            future.add_done_callback(
                lambda done, host=host, port=port: self._handle_fanout_done(
                    host,
                    port,
                    done,
                )
            )

        first_429: httpx.Response | None = None
        first_412: httpx.Response | None = None
        first_900001: httpx.Response | None = None
        first_other: httpx.Response | None = None
        last_exc: Exception | None = None
        wait_for_futures = True
        try:
            for future in as_completed(futures):
                outcome = future.result()
                if outcome.response is None:
                    last_exc = outcome.exc
                    continue

                status_code = outcome.response.status_code
                if status_code == 200:
                    errno = _response_errno(outcome.response)
                    if errno != 900001:
                        wait_for_futures = False
                        return outcome.response
                    if first_900001 is None:
                        first_900001 = outcome.response
                if status_code == 429 and first_429 is None:
                    first_429 = outcome.response
                elif status_code == 412 and first_412 is None:
                    first_412 = outcome.response
                elif status_code not in {200, 412, 429} and first_other is None:
                    first_other = outcome.response

            if first_900001 is not None:
                return first_900001
            if first_429 is not None:
                return first_429
            if first_other is not None:
                return first_other
            if first_412 is not None:
                return first_412
            raise self._to_httpx_error(method, url, last_exc)
        finally:
            executor.shutdown(
                wait=wait_for_futures,
                cancel_futures=not wait_for_futures,
            )

    def _should_fanout_create_v2(self, url: str) -> bool:
        parsed = urllib.parse.urlsplit(url)
        decoded_path = urllib.parse.unquote(parsed.path)
        return (
            (parsed.hostname or "").lower() == self._healthcheck_host.lower()
            and "createv2" in decoded_path.lower()
        )

    @staticmethod
    def _fanout_same_source_enabled() -> bool:
        """是否允许同一个出口 IP 内多条连接并发发同一请求（默认关闭）。

        关闭（默认）：每个出口只发 1 路。同一个出口上并发多个「完全相同的 createV2」
        （同 ctoken / 同 cookie / 同 header）收益很小，更容易被判重复提交。

        打开（BTB_FANOUT_SAME_SOURCE=1）：恢复上游「每个 IP 连 3 条、每秒各打一次」的
        原始策略（见 #963/#1028/#1058 的实测：单 IP 约 3 req/s 是上限）。需要 A/B 时用它。
        """

        value = str(os.environ.get("BTB_FANOUT_SAME_SOURCE", "0")).strip().lower()
        return value in {"1", "true", "on", "yes"}

    @classmethod
    def _fanout_slots(cls, pool: Sequence[ConnectionSlot]) -> list[ConnectionSlot]:
        """默认每个出口 source 只保留一条连接用于并发对冲。"""

        if cls._fanout_same_source_enabled():
            return list(pool)

        seen: set[str] = set()
        selected: list[ConnectionSlot] = []
        for slot in pool:
            key = slot.source.ip
            if key in seen:
                continue
            seen.add(key)
            selected.append(slot)
        return selected

    def _send_fanout_request(
        self,
        slot: ConnectionSlot,
        method: str,
        url: str,
        headers: HeaderItems,
        content: bytes | str | None,
        request: httpx.Request,
    ) -> FanoutOutcome:
        try:
            with slot.lock:
                if method == "GET":
                    h2_response = slot.connection.get(url, headers=headers)
                else:
                    h2_response = slot.connection.post(
                        url,
                        headers=headers,
                        content=content,
                    )
            return FanoutOutcome(
                slot=slot,
                response=self._to_httpx_response(request, h2_response),
            )
        except Exception as exc:
            return FanoutOutcome(slot=slot, exc=exc)

    def _handle_fanout_done(
        self,
        host: str,
        port: int,
        future: Future,
    ) -> None:
        if future.cancelled():
            return
        try:
            outcome = future.result()
        except Exception:
            return
        if outcome.response is None:
            self._discard_slot(
                host,
                port,
                outcome.slot,
                reason="fanout_exception",
                exc=outcome.exc,
            )
        # 412 是账号/IP 级别的风控响应，不代表这条连接坏了：
        # 原来在这里丢弃连接，一场 412 风暴会把连接池打到 0 再重建，T0 期间全在握手。


class ProxyPoolCreateV2FanoutJA3H2Client(_CreateV2FanoutJA3H2Client):
    def __init__(
        self,
        *,
        proxy_pool: Sequence[str | None] | None = None,
        proxy: str | None = None,
        **kwargs: Any,
    ) -> None:
        proxies = _normalize_fanout_sources(proxy_pool, proxy)
        if not proxies:
            proxies = [None]
        self._proxy_pool = proxies
        super().__init__(
            proxy=proxy,
            source_ip_provider=self._proxy_sources,
            **kwargs,
        )

    def _proxy_sources(self) -> list[SourceAddress]:
        return [
            SourceAddress(
                ip="direct" if proxy is None else f"proxy-{index + 1}",
                family="auto",
                interface_alias="direct" if proxy is None else "proxy",
                proxy_url=proxy,
            )
            for index, proxy in enumerate(self._proxy_pool)
        ]

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: HeaderItems | None = None,
        content: bytes | str | None = None,
    ) -> httpx.Response:
        if not self._should_fanout_create_v2(url):
            return super()._request(method, url, headers=headers, content=content)

        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("only absolute https:// URLs are supported")
        host = parsed.hostname
        port = parsed.port or 443
        request = httpx.Request(method, url)
        request_headers = self._request_headers(
            method,
            url,
            headers=headers,
            content=content,
        )

        pool = list(self._ensure_pool(host, port))
        if not pool:
            raise self._to_httpx_error(method, url, None)

        # 同 _CreateV2FanoutJA3H2Client：只在多个出口 source 之间对冲，同一出口不并发重复提交。
        pool = self._fanout_slots(pool)
        if len(pool) <= 1:
            return super()._request(method, url, headers=headers, content=content)

        executor = ThreadPoolExecutor(
            max_workers=len(pool),
            thread_name_prefix="btb-proxy-createv2-fanout",
        )
        futures = [
            executor.submit(
                self._send_fanout_request,
                slot,
                method,
                url,
                request_headers,
                content,
                request,
            )
            for slot in pool
        ]
        for future in futures:
            future.add_done_callback(
                lambda done, host=host, port=port: self._handle_fanout_done(
                    host,
                    port,
                    done,
                )
            )

        first_429: httpx.Response | None = None
        first_412: httpx.Response | None = None
        first_900001: httpx.Response | None = None
        first_other: httpx.Response | None = None
        last_exc: Exception | None = None
        wait_for_futures = True
        try:
            for future in as_completed(futures):
                outcome = future.result()
                if outcome.response is None:
                    last_exc = outcome.exc
                    continue

                status_code = outcome.response.status_code
                errno = _response_errno(outcome.response)
                if status_code == 200:
                    if errno != 900001:
                        wait_for_futures = False
                        return outcome.response
                    if first_900001 is None:
                        first_900001 = outcome.response
                elif status_code == 429 and first_429 is None:
                    first_429 = outcome.response
                elif status_code == 412 and first_412 is None:
                    first_412 = outcome.response
                elif status_code not in {412, 429} and first_other is None:
                    first_other = outcome.response

            if first_900001 is not None:
                return first_900001
            if first_429 is not None:
                return first_429
            if first_other is not None:
                return first_other
            if first_412 is not None:
                return first_412
            raise self._to_httpx_error(method, url, last_exc)
        finally:
            executor.shutdown(
                wait=wait_for_futures,
                cancel_futures=not wait_for_futures,
            )
