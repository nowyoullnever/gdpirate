import html
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse, urlunparse

from gdpirate.core.models import ResourceType


GOOGLE_PROVIDER = "google"
GOOGLE_HOSTS = {
    "drive.google.com",
    "docs.google.com",
    "drive.usercontent.google.com",
    "docs.googleusercontent.com",
}
DOC_KIND_TO_TYPE = {
    "document": ResourceType.DOCUMENT,
    "spreadsheets": ResourceType.SPREADSHEET,
    "presentation": ResourceType.PRESENTATION,
    "forms": ResourceType.FORM,
    "drawings": ResourceType.DRAWING,
}
TRACKING_QUERY_PREFIXES = ("utm_",)
TRAILING_PUNCTUATION = ".,;:!?)>]}'\""
RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{3,512}$")
URL_RE = re.compile(r"https?://[^\s<>'\"]+", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedGoogleUrl:
    provider: str
    resource_id: str
    resource_type: ResourceType
    canonical_url: str


def parse_google_url(raw_url: str) -> ParsedGoogleUrl | None:
    cleaned = html.unescape(raw_url.strip())
    if not cleaned:
        return None

    parsed = urlparse(cleaned)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None

    host = (parsed.hostname or "").lower().rstrip(".")
    if host not in GOOGLE_HOSTS:
        return None

    path_parts = [unquote(part) for part in parsed.path.split("/") if part]
    query = parse_qs(parsed.query, keep_blank_values=False)
    resource_type: ResourceType | None = None
    resource_id: str | None = None

    if host == "drive.google.com":
        resource_type, resource_id = _parse_drive_url(path_parts, query)
    elif host == "docs.google.com":
        resource_type, resource_id = _parse_docs_url(path_parts)
    elif host in {"drive.usercontent.google.com", "docs.googleusercontent.com"}:
        resource_type, resource_id = _parse_googleusercontent_url(query)

    if resource_type is None or resource_id is None or not _valid_resource_id(resource_id):
        return None

    return ParsedGoogleUrl(
        provider=GOOGLE_PROVIDER,
        resource_id=resource_id,
        resource_type=resource_type,
        canonical_url=canonical_url(resource_type, resource_id),
    )


def extract_google_urls(text: str) -> list[str]:
    urls: list[str] = []
    for match in URL_RE.finditer(html.unescape(text)):
        candidate = match.group(0).rstrip(TRAILING_PUNCTUATION)
        if parse_google_url(candidate):
            urls.append(candidate)
    return urls


def canonical_url(resource_type: ResourceType, resource_id: str) -> str:
    if resource_type == ResourceType.FILE:
        return f"https://drive.google.com/file/d/{resource_id}/view"
    if resource_type == ResourceType.FOLDER:
        return f"https://drive.google.com/drive/folders/{resource_id}"
    doc_paths = {
        ResourceType.DOCUMENT: "document",
        ResourceType.SPREADSHEET: "spreadsheets",
        ResourceType.PRESENTATION: "presentation",
        ResourceType.FORM: "forms",
        ResourceType.DRAWING: "drawings",
    }
    if resource_type in doc_paths:
        return f"https://docs.google.com/{doc_paths[resource_type]}/d/{resource_id}/edit"
    return f"https://drive.google.com/open?id={resource_id}"


def _parse_drive_url(
    path_parts: list[str], query: dict[str, list[str]]
) -> tuple[ResourceType | None, str | None]:
    if len(path_parts) >= 3 and path_parts[0] == "file" and path_parts[1] == "d":
        return ResourceType.FILE, path_parts[2]

    if len(path_parts) >= 3 and path_parts[0] == "drive" and path_parts[1] == "folders":
        return ResourceType.FOLDER, path_parts[2]

    if (
        len(path_parts) >= 5
        and path_parts[0] == "drive"
        and path_parts[1] == "u"
        and path_parts[2].isdigit()
        and path_parts[3] == "folders"
    ):
        return ResourceType.FOLDER, path_parts[4]

    if path_parts and path_parts[0] in {"open", "uc"}:
        return ResourceType.FILE, _single_query_value(query, "id")

    if path_parts and path_parts[0] == "folderview":
        return ResourceType.FOLDER, _single_query_value(query, "id")

    return None, None


def _parse_docs_url(path_parts: list[str]) -> tuple[ResourceType | None, str | None]:
    if len(path_parts) >= 3 and path_parts[0] in DOC_KIND_TO_TYPE and path_parts[1] == "d":
        return DOC_KIND_TO_TYPE[path_parts[0]], path_parts[2]
    return None, None


def _parse_googleusercontent_url(
    query: dict[str, list[str]]
) -> tuple[ResourceType | None, str | None]:
    resource_id = _single_query_value(query, "id")
    if resource_id:
        return ResourceType.FILE, resource_id
    return None, None


def _single_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    return values[0]


def _valid_resource_id(resource_id: str) -> bool:
    return bool(RESOURCE_ID_RE.fullmatch(resource_id))


def normalize_url_for_request(raw_url: str) -> str:
    parsed = urlparse(html.unescape(raw_url.strip()))
    query_pairs = []
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        if key.lower().startswith(TRACKING_QUERY_PREFIXES):
            continue
        for value in values:
            query_pairs.append((key, value))
    query = "&".join(f"{key}={value}" for key, value in query_pairs)
    return urlunparse(
        (
            parsed.scheme.lower(),
            (parsed.netloc or "").lower(),
            parsed.path,
            "",
            query,
            "",
        )
    )
