"""HTTP access to Open-Meteo with retries.

Retry: timeouts, connection errors, HTTP 429 and 5xx, and HTTP 200 with a body that is not valid
JSON (seen in practice on large responses: an empty or cut off body).
No retry: HTTP 400 and other 4xx (the request itself is wrong; retrying cannot help).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class TransientApiError(Exception):
    """Temporary failure; the request is retried."""


class ApiError(Exception):
    """Permanent failure; carries the API's own reason when it gives one."""

    def __init__(self, status: int, reason: str):
        super().__init__(f"HTTP {status}: {reason}")
        self.status = status
        self.reason = reason


@dataclass(frozen=True)
class ApiResponse:
    status: int
    content: bytes


def _reason(response: httpx.Response) -> str:
    try:
        body = response.json()
        if isinstance(body, dict) and body.get("reason"):
            return str(body["reason"])
    except ValueError:
        pass
    return response.text[:300]


class OpenMeteoClient:
    def __init__(self, http: httpx.Client | None = None, max_attempts: int = 5, max_wait_seconds: float = 60):
        self._http = http or httpx.Client(timeout=120)
        self._fetch = retry(
            retry=retry_if_exception_type((TransientApiError, httpx.TransportError)),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=2, min=min(2, max_wait_seconds), max=max_wait_seconds),
            reraise=True,
        )(self._fetch_once)

    def get(self, url: str, params: dict) -> ApiResponse:
        return self._fetch(url, params)

    def _fetch_once(self, url: str, params: dict) -> ApiResponse:
        response = self._http.get(url, params=params)
        if response.status_code in RETRYABLE_STATUS:
            raise TransientApiError(f"HTTP {response.status_code}: {_reason(response)}")
        if response.status_code != 200:
            raise ApiError(response.status_code, _reason(response))
        try:
            json.loads(response.content)
        except ValueError:
            raise TransientApiError(
                f"HTTP 200 but body is not valid JSON ({len(response.content)} bytes)"
            ) from None
        return ApiResponse(status=response.status_code, content=response.content)

    def close(self) -> None:
        self._http.close()
