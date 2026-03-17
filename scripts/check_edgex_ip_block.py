#!/usr/bin/env python3
"""Diagnose whether the current outbound IP is being blocked by edgeX."""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BASE_URL = "https://pro.edgex.exchange"
METADATA_PATH = "/api/v1/public/meta/getMetaData"
TICKER_PATH = "/api/v1/public/quote/getTicker"
FUNDING_PATH = "/api/v1/public/funding/getFundingRatePage"
DEFAULT_CONTRACT_ID = "10000001"
PUBLIC_IP_SERVICES = (
    "https://api.ipify.org",
    "https://api64.ipify.org",
    "https://ifconfig.me/ip",
    "https://ipinfo.io/ip",
)


@dataclass
class TestResult:
    name: str
    method: str
    url: str
    ok: bool
    status_code: int | None
    content_type: str | None
    json_code: str | None
    json_msg: str | None
    body: str | None
    preview: str | None
    error: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=BASE_URL, help=f"edgeX base URL (default: {BASE_URL})")
    parser.add_argument(
        "--contract-id",
        default=None,
        help=f"Contract ID for ticker/funding checks (default: auto-detect, fallback {DEFAULT_CONTRACT_ID})",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument(
        "--preview-chars",
        type=int,
        default=240,
        help="How many response body characters to print per request",
    )
    return parser.parse_args()


def request_headers(base_url: str) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": f"{base_url.rstrip('/')}/trade/BTCUSDT",
        "Origin": base_url.rstrip("/"),
    }


def format_preview(payload: str | bytes | None, max_chars: int) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="replace")
    else:
        text = payload
    compact = " ".join(text.split())
    if len(compact) <= max_chars:
        return compact
    return f"{compact[:max_chars]}..."


def try_json(text: str | bytes | None) -> tuple[str | None, str | None]:
    if text is None:
        return None, None
    try:
        payload = json.loads(text)
    except Exception:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    code = payload.get("code")
    msg = payload.get("msg")
    return None if code is None else str(code), None if msg is None else str(msg)


def resolve_host(hostname: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(hostname, 443, proto=socket.IPPROTO_TCP)
    except OSError:
        return []
    addresses = sorted({item[4][0] for item in infos if item and item[4]})
    return addresses


def detect_public_ip(timeout: int) -> tuple[str | None, str | None]:
    session = requests.Session()
    for url in PUBLIC_IP_SERVICES:
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            value = resp.text.strip()
            if value:
                return value, url
        except Exception:
            continue
    return None, None


def run_requests_test(
    name: str,
    url: str,
    *,
    headers: dict[str, str],
    timeout: int,
    preview_chars: int,
) -> TestResult:
    session = requests.Session()
    session.trust_env = False
    try:
        resp = session.get(url, headers=headers, timeout=timeout)
        body = resp.text
        json_code, json_msg = try_json(body)
        ok = resp.status_code == 200 and (json_code in (None, "SUCCESS"))
        return TestResult(
            name=name,
            method="requests",
            url=resp.url,
            ok=ok,
            status_code=resp.status_code,
            content_type=resp.headers.get("Content-Type"),
            json_code=json_code,
            json_msg=json_msg,
            body=body,
            preview=format_preview(body, preview_chars),
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        return TestResult(
            name=name,
            method="requests",
            url=url,
            ok=False,
            status_code=None,
            content_type=None,
            json_code=None,
            json_msg=None,
            body=None,
            preview=None,
            error=str(exc),
        )


def run_curl_test(
    name: str,
    url: str,
    *,
    headers: dict[str, str],
    timeout: int,
    preview_chars: int,
) -> TestResult:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return TestResult(
            name=name,
            method="curl",
            url=url,
            ok=False,
            status_code=None,
            content_type=None,
            json_code=None,
            json_msg=None,
            body=None,
            preview=None,
            error="curl not found",
        )

    cmd = [
        curl_bin,
        "-sS",
        "--compressed",
        "--max-time",
        str(timeout),
        "-o",
        "-",
        "-w",
        "\n__CURL_STATUS__:%{http_code}\n__CURL_CONTENT_TYPE__:%{content_type}\n",
    ]
    for key, value in headers.items():
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.append(url)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except Exception as exc:  # noqa: BLE001
        return TestResult(
            name=name,
            method="curl",
            url=url,
            ok=False,
            status_code=None,
            content_type=None,
            json_code=None,
            json_msg=None,
            body=None,
            preview=None,
            error=str(exc),
        )

    if proc.returncode != 0:
        return TestResult(
            name=name,
            method="curl",
            url=url,
            ok=False,
            status_code=None,
            content_type=None,
            json_code=None,
            json_msg=None,
            body=None,
            preview=None,
            error=proc.stderr.strip() or f"curl exit {proc.returncode}",
        )

    body = proc.stdout
    status_code: int | None = None
    content_type: str | None = None
    body_text = body
    status_marker = "\n__CURL_STATUS__:"
    type_marker = "\n__CURL_CONTENT_TYPE__:"

    if status_marker in body:
        body_text, tail = body.rsplit(status_marker, 1)
        if type_marker in tail:
            status_text, content_type_text = tail.split(type_marker, 1)
            status_text = status_text.strip()
            content_type = content_type_text.strip() or None
            if status_text.isdigit():
                status_code = int(status_text)

    json_code, json_msg = try_json(body_text)
    ok = status_code == 200 and (json_code in (None, "SUCCESS"))
    return TestResult(
        name=name,
        method="curl",
        url=url,
        ok=ok,
        status_code=status_code,
        content_type=content_type,
        json_code=json_code,
        json_msg=json_msg,
        body=body_text,
        preview=format_preview(body_text, preview_chars),
        error=None,
    )


def build_url(base_url: str, path: str, params: dict[str, Any] | None = None) -> str:
    url = f"{base_url.rstrip('/')}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    return url


def extract_contract_id_from_response_text(text: str | None) -> str | None:
    if not text:
        return None
    try:
        payload = json.loads(text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    items = data.get("contractList")
    if not isinstance(items, list):
        return None

    first_available: str | None = None
    for item in items:
        if not isinstance(item, dict):
            continue
        contract_id = item.get("contractId")
        if not isinstance(contract_id, str) or not contract_id:
            continue
        if first_available is None:
            first_available = contract_id
        if item.get("contractName") == "BTCUSD":
            return contract_id
        if item.get("enableDisplay") and item.get("enableTrade"):
            return contract_id
    return first_available


def print_header(title: str) -> None:
    print(f"\n== {title} ==")


def print_result(result: TestResult) -> None:
    print(f"[{result.method}] {result.name}")
    print(f"  url: {result.url}")
    print(f"  ok: {result.ok}")
    print(f"  status: {result.status_code}")
    print(f"  content-type: {result.content_type}")
    if result.json_code is not None or result.json_msg is not None:
        print(f"  json code/msg: {result.json_code!r} / {result.json_msg!r}")
    if result.error:
        print(f"  error: {result.error}")
    if result.preview:
        print(f"  preview: {result.preview}")


def infer_status(results: list[TestResult]) -> tuple[str, int]:
    statuses_403 = [item for item in results if item.status_code == 403]
    successes = [item for item in results if item.ok]

    endpoint_map: dict[str, list[TestResult]] = {}
    for item in results:
        endpoint_map.setdefault(item.name, []).append(item)

    if statuses_403:
        blocked_endpoints = [
            name
            for name, items in endpoint_map.items()
            if items and all(item.status_code == 403 for item in items if item.status_code is not None)
        ]
        if blocked_endpoints:
            return (
                "LIKELY_BLOCKED: at least one edgeX endpoint returned HTTP 403 for every client path.",
                2,
            )
        return (
            "PARTIALLY_BLOCKED_OR_RATE_LIMITED: some edgeX checks returned HTTP 403.",
            2,
        )

    if successes and len(successes) == len(results):
        return ("NOT_BLOCKED_NOW: all tested edgeX endpoints succeeded from this machine.", 0)

    if successes:
        return ("INCONCLUSIVE: some checks succeeded, some failed without HTTP 403.", 1)

    return ("INCONCLUSIVE: no check succeeded, but there was no clear HTTP 403 block signal.", 1)


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    headers = request_headers(base_url)
    hostname = urlparse(base_url).hostname or "pro.edgex.exchange"

    print(f"time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"base_url: {base_url}")
    print(f"hostname: {hostname}")

    resolved = resolve_host(hostname)
    print(f"dns: {', '.join(resolved) if resolved else 'unresolved'}")

    public_ip, public_ip_source = detect_public_ip(args.timeout)
    if public_ip:
        print(f"public_ip: {public_ip} ({public_ip_source})")
    else:
        print("public_ip: unknown")

    print_header("Metadata")
    metadata_url = build_url(base_url, METADATA_PATH)
    metadata_requests = run_requests_test(
        "metadata",
        metadata_url,
        headers=headers,
        timeout=args.timeout,
        preview_chars=args.preview_chars,
    )
    metadata_curl = run_curl_test(
        "metadata",
        metadata_url,
        headers=headers,
        timeout=args.timeout,
        preview_chars=args.preview_chars,
    )
    print_result(metadata_requests)
    print_result(metadata_curl)

    contract_id = args.contract_id
    if not contract_id:
        contract_id = (
            extract_contract_id_from_response_text(metadata_requests.body)
            or extract_contract_id_from_response_text(metadata_curl.body)
            or DEFAULT_CONTRACT_ID
        )
    print(f"contract_id: {contract_id}")

    tests = [
        (
            "ticker",
            build_url(base_url, TICKER_PATH, {"contractId": contract_id}),
        ),
        (
            "funding",
            build_url(
                base_url,
                FUNDING_PATH,
                {"contractId": contract_id, "size": 5, "filterSettlementFundingRate": "true"},
            ),
        ),
    ]

    results = [metadata_requests, metadata_curl]

    for name, url in tests:
        print_header(name.title())
        requests_result = run_requests_test(
            name,
            url,
            headers=headers,
            timeout=args.timeout,
            preview_chars=args.preview_chars,
        )
        curl_result = run_curl_test(
            name,
            url,
            headers=headers,
            timeout=args.timeout,
            preview_chars=args.preview_chars,
        )
        print_result(requests_result)
        print_result(curl_result)
        results.extend([requests_result, curl_result])

    print_header("Conclusion")
    message, exit_code = infer_status(results)
    print(message)
    if public_ip:
        print(f"hint: if the server fails but your laptop succeeds, compare this outbound IP against the server outbound IP.")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
