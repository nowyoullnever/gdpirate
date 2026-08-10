from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


ALLOWED_SOURCE_SCHEMES = {"http", "https"}


def normalize_source_url(raw_url: str | None) -> str | None:
    if raw_url is None:
        return None
    value = raw_url.strip()
    if not value or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value):
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme.lower() not in ALLOWED_SOURCE_SCHEMES:
        return None
    if not parsed.hostname:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    return urlunsplit((scheme, netloc, parsed.path or "", parsed.query or "", parsed.fragment or ""))


def is_public_source_url(raw_url: str | None) -> bool:
    return normalize_source_url(raw_url) is not None
