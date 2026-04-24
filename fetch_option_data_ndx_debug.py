#!/usr/bin/env python3

from __future__ import annotations

import os
import runpy
from pathlib import Path

import requests


DEBUG_TICKER = os.getenv("ICE_DEBUG_TICKER", "NDX")
DEBUG_DATES = {
    value.strip()
    for value in os.getenv("ICE_DEBUG_DATES", "2025-05-16,2025-05-19").split(",")
    if value.strip()
}
DEBUG_RESPONSE_CHARS = int(os.getenv("ICE_DEBUG_RESPONSE_CHARS", "1200"))

ORIGINAL_SCRIPT = Path(__file__).with_name("fetch_option_data.py")


def _stringify_body(kwargs: dict) -> str:
    for key in ("data", "json"):
        if key in kwargs and kwargs[key] is not None:
            return str(kwargs[key])
    return ""


def _should_debug(url: str, kwargs: dict) -> bool:
    payload_text = _stringify_body(kwargs)
    if "idd.pt.ice.com" not in url and "idd.ice.com" not in url:
        return False
    if DEBUG_TICKER and DEBUG_TICKER not in payload_text:
        return False
    if DEBUG_DATES and not any(date in payload_text for date in DEBUG_DATES):
        return False
    return True


def _print_request(url: str, kwargs: dict) -> None:
    payload_text = _stringify_body(kwargs)
    print("")
    print("DEBUG ICE REQUEST")
    print(f"  url: {url}")
    print(f"  verify: {kwargs.get('verify', True)}")
    print(f"  timeout: {kwargs.get('timeout')}")
    print(f"  payload_preview: {payload_text[:DEBUG_RESPONSE_CHARS]}")


def _print_response(response: requests.Response) -> None:
    body_preview = response.text[:DEBUG_RESPONSE_CHARS]
    print("DEBUG ICE RESPONSE")
    print(f"  status_code: {response.status_code}")
    print(f"  reason: {response.reason}")
    print(f"  body_preview: {body_preview}")
    print("")


_original_requests_post = requests.post
_original_session_post = requests.Session.post


def _debug_post(url, *args, **kwargs):
    debug_this = _should_debug(url, kwargs)
    if debug_this:
        _print_request(url, kwargs)
    response = _original_requests_post(url, *args, **kwargs)
    if debug_this:
        _print_response(response)
    return response


def _debug_session_post(self, url, *args, **kwargs):
    debug_this = _should_debug(url, kwargs)
    if debug_this:
        _print_request(url, kwargs)
    response = _original_session_post(self, url, *args, **kwargs)
    if debug_this:
        _print_response(response)
    return response


requests.post = _debug_post
requests.Session.post = _debug_session_post


if __name__ == "__main__":
    runpy.run_path(str(ORIGINAL_SCRIPT), run_name="__main__")
