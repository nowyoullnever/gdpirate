from __future__ import annotations

from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse, JSONResponse

from gdpirate.pipeline.random_selection import RandomLinkService

app = FastAPI(title="GDPIRATE")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/random")
async def api_random(response: Response):
    response.headers["Cache-Control"] = "no-store"
    link = await RandomLinkService().pick()
    if link is None:
        return JSONResponse(
            {"error": "no_verified_public_link_available"},
            status_code=503,
            headers={"Cache-Control": "no-store"},
        )
    return {
        "url": link.url,
        "source": {
            "name": link.source_name,
            "url": link.source_url,
        },
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GDPIRATE</title>
  <style>
    :root { color-scheme: light dark; font-family: Arial, sans-serif; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #f4f1e8; color: #111; }
    main { width: min(720px, calc(100vw - 32px)); }
    h1 { font-size: 56px; margin: 0 0 24px; letter-spacing: 0; }
    button { font: inherit; font-weight: 700; padding: 14px 18px; border: 2px solid #111; background: #111; color: #fff; cursor: pointer; }
    #result { margin-top: 24px; line-height: 1.5; overflow-wrap: anywhere; }
    a { color: #0645ad; }
    @media (prefers-color-scheme: dark) {
      body { background: #181818; color: #f7f7f7; }
      button { border-color: #f7f7f7; background: #f7f7f7; color: #111; }
      a { color: #8ab4f8; }
    }
  </style>
</head>
<body>
  <main>
    <h1>GDPIRATE</h1>
    <button id="random" type="button">RANDOM LINK</button>
    <div id="result" aria-live="polite"></div>
  </main>
  <script>
    const button = document.getElementById("random");
    const result = document.getElementById("result");
    button.addEventListener("click", async () => {
      button.disabled = true;
      result.textContent = "";
      try {
        const response = await fetch("/api/random", { cache: "no-store" });
        const payload = await response.json();
        if (!response.ok) {
          result.textContent = payload.error || "unavailable";
          return;
        }
        result.innerHTML = "";
        const drive = document.createElement("a");
        drive.href = payload.url;
        drive.target = "_blank";
        drive.rel = "noreferrer";
        drive.textContent = payload.url;
        const source = document.createElement("a");
        source.href = payload.source.url;
        source.target = "_blank";
        source.rel = "noreferrer";
        source.textContent = payload.source.name;
        result.append("Drive: ", drive, document.createElement("br"), "Source: ", source);
      } finally {
        button.disabled = false;
      }
    });
  </script>
</body>
</html>"""
