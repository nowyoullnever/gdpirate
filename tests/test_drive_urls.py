import pytest

from gdpirate.core.drive_urls import extract_google_urls, parse_google_url
from gdpirate.core.models import ResourceType


@pytest.mark.parametrize(
    ("url", "resource_type", "canonical"),
    [
        (
            " https://drive.google.com/file/d/ABC123/view?usp=sharing&utm_source=x#frag ",
            ResourceType.FILE,
            "https://drive.google.com/file/d/ABC123/view",
        ),
        (
            "https://drive.google.com/drive/folders/FOLD_er-123",
            ResourceType.FOLDER,
            "https://drive.google.com/drive/folders/FOLD_er-123",
        ),
        (
            "https://drive.google.com/drive/u/0/folders/FOLD123",
            ResourceType.FOLDER,
            "https://drive.google.com/drive/folders/FOLD123",
        ),
        (
            "https://drive.google.com/open?id=ABC123&amp;usp=sharing",
            ResourceType.UNKNOWN,
            "https://drive.google.com/open?id=ABC123",
        ),
        (
            "https://drive.google.com/uc?export=download&id=ABC123",
            ResourceType.UNKNOWN,
            "https://drive.google.com/open?id=ABC123",
        ),
        (
            "https://drive.google.com/folderview?id=FOLDER123",
            ResourceType.FOLDER,
            "https://drive.google.com/drive/folders/FOLDER123",
        ),
        (
            "https://docs.google.com/document/d/DOC123/edit",
            ResourceType.DOCUMENT,
            "https://docs.google.com/document/d/DOC123/edit",
        ),
        (
            "https://docs.google.com/spreadsheets/d/SHEET123/pubhtml",
            ResourceType.SPREADSHEET,
            "https://docs.google.com/spreadsheets/d/SHEET123/edit",
        ),
        (
            "https://docs.google.com/presentation/d/SLIDE123/present",
            ResourceType.PRESENTATION,
            "https://docs.google.com/presentation/d/SLIDE123/edit",
        ),
        (
            "https://docs.google.com/forms/d/FORM123/viewform",
            ResourceType.FORM,
            "https://docs.google.com/forms/d/FORM123/edit",
        ),
        (
            "https://docs.google.com/drawings/d/DRAW123/edit",
            ResourceType.DRAWING,
            "https://docs.google.com/drawings/d/DRAW123/edit",
        ),
        (
            "https://drive.usercontent.google.com/download?id=ABC123&export=download",
            ResourceType.FILE,
            "https://drive.google.com/file/d/ABC123/view",
        ),
    ],
)
def test_parse_supported_google_urls(url, resource_type, canonical):
    parsed = parse_google_url(url)

    assert parsed is not None
    assert parsed.provider == "google"
    assert parsed.resource_id in canonical
    assert parsed.resource_type == resource_type
    assert parsed.canonical_url == canonical


@pytest.mark.parametrize(
    "url",
    [
        "https://drive.google.com.example.com/file/d/ABC123/view",
        "https://google-drive.example/file/d/ABC123/view",
        "https://example.com/https://drive.google.com/file/d/ABC123/view",
        "not a url",
        "ftp://drive.google.com/file/d/ABC123/view",
        "https://drive.google.com/file/d/!!/view",
        "https://drive.google.com/open",
        "https://docs.google.com/document/u/0/d/ABC123/edit",
    ],
)
def test_rejects_invalid_and_lookalike_urls(url):
    assert parse_google_url(url) is None


def test_duplicate_representations_have_same_identity():
    variants = [
        "https://drive.google.com/file/d/ABC123/view",
        "https://drive.google.com/file/d/ABC123/edit",
        "https://drive.google.com/open?id=ABC123",
        "https://drive.google.com/uc?id=ABC123&utm_campaign=x",
    ]

    identities = {
        (parse_google_url(url).provider, parse_google_url(url).resource_id)
        for url in variants
    }

    assert identities == {("google", "ABC123")}


def test_extract_google_urls_from_text_trims_punctuation():
    text = (
        "download: https://drive.google.com/file/d/ABC123/view. "
        "(https://docs.google.com/document/d/DOC123/edit), "
        "ignore https://example.com/x"
    )

    assert extract_google_urls(text) == [
        "https://drive.google.com/file/d/ABC123/view",
        "https://docs.google.com/document/d/DOC123/edit",
    ]
