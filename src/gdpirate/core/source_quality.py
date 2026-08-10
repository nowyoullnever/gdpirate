QUALITY = {
    "Common Crawl URL Index": 10,
    "deDigger": 20,
    "gdURL": 30,
    "Common Crawl": 80,
}
DEFAULT_QUALITY = 70


def source_quality(source_name: str | None, source_url: str | None) -> int:
    if not source_url:
        return 0
    if not source_name:
        return 1
    return QUALITY.get(source_name, DEFAULT_QUALITY)


def should_replace_source(
    existing_name: str | None,
    existing_url: str | None,
    new_name: str | None,
    new_url: str | None,
) -> bool:
    return source_quality(new_name, new_url) > source_quality(existing_name, existing_url)
