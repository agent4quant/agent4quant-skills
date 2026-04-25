from __future__ import annotations

import re

CANONICAL_SYMBOL_PATTERN = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")
_BJ_PREFIXES = (
    "430",
    "831",
    "832",
    "833",
    "834",
    "835",
    "836",
    "837",
    "838",
    "839",
    "870",
    "871",
    "872",
    "873",
)
_SH_PREFIXES = ("600", "601", "603", "605", "688", "689")
_SZ_PREFIXES = ("000", "001", "002", "003", "300", "301")
_EXCHANGE_ALIASES = {
    "SZ": "SZ",
    "XSHE": "SZ",
    "SH": "SH",
    "SSE": "SH",
    "XSHG": "SH",
    "BJ": "BJ",
    "BSE": "BJ",
}


def is_canonical_symbol(symbol: str) -> bool:
    return bool(CANONICAL_SYMBOL_PATTERN.match(symbol.strip().upper()))


def normalize_symbol(symbol: str) -> str:
    clean = symbol.strip().upper().replace(" ", "")
    if not clean:
        return clean

    clean = clean.replace("-", ".").replace("_", ".")
    if "." in clean:
        code, exchange = clean.split(".", 1)
        mapped_exchange = _EXCHANGE_ALIASES.get(exchange)
        if code.isdigit() and len(code) == 6 and mapped_exchange:
            return f"{code}.{mapped_exchange}"

    digits = "".join(ch for ch in clean if ch.isdigit())
    if len(digits) != 6:
        return clean

    if digits.startswith(_SH_PREFIXES):
        return f"{digits}.SH"
    if digits.startswith(_SZ_PREFIXES):
        return f"{digits}.SZ"
    if digits.startswith(_BJ_PREFIXES):
        return f"{digits}.BJ"
    return digits


def split_symbol(symbol: str) -> tuple[str, str]:
    canonical = normalize_symbol(symbol)
    if "." not in canonical:
        return canonical, canonical
    code, _ = canonical.split(".", 1)
    return code, canonical
