from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


class PublicUIParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.buttons = []
        self.links = []
        self.scripts = []
        self.stylesheets = []
        self.visible = []
        self.result_text = []
        self.in_result = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        self.stack.append(tag)
        if tag == "button":
            self.buttons.append({"attrs": attrs, "text": ""})
        if tag == "a":
            self.links.append({"attrs": attrs, "text": ""})
        if tag == "script" and attrs.get("src"):
            self.scripts.append(attrs["src"])
        if tag == "link" and attrs.get("rel") == "stylesheet":
            self.stylesheets.append(attrs.get("href"))
        if tag == "div" and attrs.get("id") == "result":
            self.in_result = True

    def handle_endtag(self, tag):
        if tag == "div" and self.in_result:
            self.in_result = False
        if self.stack:
            self.stack.pop()

    def handle_data(self, data):
        text = data.strip()
        if not text:
            return
        if self.stack and self.stack[-1] == "button" and self.buttons:
            self.buttons[-1]["text"] += text
        elif self.stack and self.stack[-1] == "a" and self.links:
            self.links[-1]["text"] += text
        elif self.in_result:
            self.result_text.append(text)
        elif self.stack and self.stack[-1] not in {"title", "script", "style"}:
            self.visible.append(text)


def parse_index():
    parser = PublicUIParser()
    parser.feed((DOCS / "index.html").read_text(encoding="utf-8"))
    return parser


def test_static_ui_has_exact_initial_visible_content():
    parser = parse_index()

    assert parser.visible == ["Get Google drive link."]
    assert len(parser.buttons) == 1
    assert parser.buttons[0]["text"] == "I'm feeling lucky"
    assert parser.buttons[0]["attrs"].get("type") == "button"
    assert parser.links == []
    assert parser.result_text == []


def test_pages_assets_use_project_site_safe_paths():
    parser = parse_index()

    assert parser.stylesheets == ["./style.css"]
    assert parser.scripts == ["./app.js"]


def test_click_behavior_is_single_replacing_new_tab_link():
    app_js = (DOCS / "app.js").read_text(encoding="utf-8")

    assert "async function getRandomDriveLink()" in app_js
    assert "https://drive.google.com/" in app_js
    assert "result.replaceChildren(link)" in app_js
    assert "appendChild" not in app_js
    assert 'link.target = "_blank"' in app_js
    assert 'link.rel = "noopener noreferrer"' in app_js
    assert "link.textContent = payload.url" in app_js


def test_public_ui_has_no_unexpected_visible_ui_or_secrets():
    combined = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [DOCS / "index.html", DOCS / "style.css", DOCS / "app.js"]
    )

    forbidden_visible = [
        "GDPirate</h1>",
        "GitHub",
        "About",
        "Source:",
        "Random result:",
        "Open</a>",
        "Click here",
        "Drive:",
    ]
    forbidden_secret_terms = ["api_key", "token", "password", "postgresql://"]

    for term in forbidden_visible + forbidden_secret_terms:
        assert term not in combined
