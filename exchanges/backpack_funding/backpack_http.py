#!/usr/bin/env python3
"""Shared Backpack HTTP helpers for funding collectors."""

from __future__ import annotations

import os
import time
from typing import Any, Iterable

import requests
from requests import RequestException

REQUEST_CONNECT_TIMEOUT_SECONDS = float(os.getenv("BACKPACK_HTTP_CONNECT_TIMEOUT_SECONDS", "4"))
REQUEST_READ_TIMEOUT_SECONDS = float(os.getenv("BACKPACK_HTTP_READ_TIMEOUT_SECONDS", "8"))
MAX_HTTP_ATTEMPTS = max(1, int(os.getenv("BACKPACK_HTTP_MAX_ATTEMPTS", "1")))
MAX_CONSECUTIVE_TRANSIENT_FAILURES = max(1, int(os.getenv("BACKPACK_MAX_CONSECUTIVE_TRANSIENT_FAILURES", "4")))
MAX_FAILED_ITEMS_IN_LOG = max(1, int(os.getenv("BACKPACK_MAX_FAILED_ITEMS_IN_LOG", "10")))


class BackpackRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        timed_out: bool = False,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.timed_out = timed_out


class BackpackCircuitOpen(RuntimeError):
    """Raised when Backpack transient failures suggest the API is degraded."""


class BackpackFailureTracker:
    def __init__(
        self,
        label: str,
        *,
        max_consecutive_transient_failures: int | None = None,
    ) -> None:
        self.label = label
        self.max_consecutive_transient_failures = max(
            1,
            int(
                MAX_CONSECUTIVE_TRANSIENT_FAILURES
                if max_consecutive_transient_failures is None
                else max_consecutive_transient_failures
            ),
        )
        self.consecutive_transient_failures = 0

    def record_success(self) -> None:
        self.consecutive_transient_failures = 0

    def record_failure(self, *, item_label: str, exc: Exception) -> None:
        if isinstance(exc, BackpackRequestError) and exc.retryable:
            self.consecutive_transient_failures += 1
        else:
            self.consecutive_transient_failures = 0
            return
        if self.consecutive_transient_failures < self.max_consecutive_transient_failures:
            return
        raise BackpackCircuitOpen(
            f"{self.label} 连续 {self.consecutive_transient_failures} 次瞬时失败，"
            f"已在 {item_label} 处提前终止剩余请求：{exc}"
        ) from exc


def backpack_request_timeout() -> tuple[float, float]:
    return (
        max(0.5, float(REQUEST_CONNECT_TIMEOUT_SECONDS)),
        max(0.5, float(REQUEST_READ_TIMEOUT_SECONDS)),
    )


def backpack_retry_delay_seconds(resp: requests.Response | None, attempt: int) -> float:
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = float(retry_after)
            except ValueError:
                seconds = 0.0
            if seconds > 0:
                return min(seconds, 20.0)
    return min(max(0.8, 1.5 * attempt), 5.0)


def backpack_get(
    session: requests.Session,
    base_url: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: tuple[float, float] | None = None,
    max_attempts: int | None = None,
) -> Any:
    url = f"{base_url}{path}"
    attempts = max(1, MAX_HTTP_ATTEMPTS if max_attempts is None else int(max_attempts))
    request_timeout = backpack_request_timeout() if timeout is None else timeout
    last_exc: BackpackRequestError | None = None

    for attempt in range(1, attempts + 1):
        resp: requests.Response | None = None
        try:
            resp = session.get(url, params=params, timeout=request_timeout)
        except requests.Timeout as exc:
            last_exc = BackpackRequestError(f"{path} timeout: {exc}", retryable=True, timed_out=True)
        except RequestException as exc:
            last_exc = BackpackRequestError(f"{path} network error: {exc}", retryable=True)
        else:
            if resp.status_code == 429:
                last_exc = BackpackRequestError(f"{path} http 429", retryable=True)
            elif resp.status_code >= 500:
                last_exc = BackpackRequestError(f"{path} http {resp.status_code}", retryable=True)
            elif resp.status_code >= 400:
                last_exc = BackpackRequestError(f"{path} http {resp.status_code}", retryable=False)
            else:
                try:
                    return resp.json()
                except ValueError as exc:
                    last_exc = BackpackRequestError(f"{path} invalid json: {exc}", retryable=True)

        if attempt >= attempts or (last_exc is not None and not last_exc.retryable):
            break
        time.sleep(backpack_retry_delay_seconds(resp, attempt))

    raise BackpackRequestError(
        f"Backpack 请求失败: {path}, err={last_exc}",
        retryable=bool(last_exc.retryable if last_exc is not None else True),
        timed_out=bool(last_exc.timed_out if last_exc is not None else False),
    ) from last_exc


def summarize_failed_items(items: Iterable[str], *, max_items: int | None = None) -> str:
    values = [str(item) for item in items if str(item).strip()]
    if not values:
        return ""
    limit = MAX_FAILED_ITEMS_IN_LOG if max_items is None else max(1, int(max_items))
    preview = ", ".join(values[:limit])
    remaining = len(values) - min(len(values), limit)
    suffix = "" if remaining <= 0 else f" ... +{remaining}"
    return f"{preview}{suffix}"
