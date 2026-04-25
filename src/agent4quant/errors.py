from __future__ import annotations


class DependencyUnavailableError(RuntimeError):
    def __init__(self, component: str, message: str):
        self.component = component
        super().__init__(message)

    @property
    def status_code(self) -> int:
        return 503

    @property
    def error_code(self) -> str:
        return f"{self.component}_dependency_unavailable"


class ExternalProviderError(RuntimeError):
    def __init__(self, provider: str, category: str, message: str, *, retryable: bool = True):
        self.provider = provider
        self.category = category
        self.retryable = retryable
        super().__init__(message)

    @property
    def status_code(self) -> int:
        mapping = {
            "rate_limit": 429,
            "not_found": 404,
            "access_denied": 403,
            "timeout": 504,
            "network": 502,
            "upstream": 502,
        }
        return mapping.get(self.category, 502)

    @property
    def error_code(self) -> str:
        return f"{self.provider}_{self.category}"


def classify_external_provider_error(
    *,
    provider: str,
    detail: str,
    symbol: str | None = None,
) -> ExternalProviderError:
    text = " ".join(str(detail).split())
    lower = text.lower()

    if any(token in lower for token in ("yfratelimit", "rate limit", "too many requests", "429")):
        category = "rate_limit"
        label = "rate limited"
        retryable = True
    elif any(token in lower for token in ("possibly delisted", "no price data found", "symbol not found", "404 not found")):
        category = "not_found"
        label = "symbol not found"
        retryable = False
    elif any(token in lower for token in ("forbidden", "unauthorized", "access denied", "permission denied", "403")):
        category = "access_denied"
        label = "access denied"
        retryable = False
    elif any(token in lower for token in ("timed out", "timeout", "curl: (28)", "read timeout")):
        category = "timeout"
        label = "timeout"
        retryable = True
    elif any(
        token in lower
        for token in (
            "connection",
            "curl: (56)",
            "could not connect",
            "connection reset",
            "connection refused",
            "remote end closed",
            "network",
        )
    ):
        category = "network"
        label = "network error"
        retryable = True
    else:
        category = "upstream"
        label = "upstream error"
        retryable = True

    target = f" for symbol={symbol}" if symbol else ""
    return ExternalProviderError(
        provider=provider,
        category=category,
        message=f"{provider} {label}{target}: {text}",
        retryable=retryable,
    )
