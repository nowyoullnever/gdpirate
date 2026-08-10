from gdpirate.core.source_urls import is_public_source_url


QUALITY = {
    "Common Crawl URL Index": 10,
    "deDigger": 20,
    "gdURL": 40,
    "Common Crawl": 80,
    "Common Crawl WAT": 80,
}
DEFAULT_QUALITY = 100


def source_quality(source_name: str | None, source_url: str | None) -> int:
    if not is_public_source_url(source_url):
        return 0
    if not source_name:
        return DEFAULT_QUALITY
    return QUALITY.get(source_name, DEFAULT_QUALITY)


def should_replace_source(
    existing_name: str | None,
    existing_url: str | None,
    new_name: str | None,
    new_url: str | None,
) -> bool:
    return source_quality(new_name, new_url) > source_quality(existing_name, existing_url)
