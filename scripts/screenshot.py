"""Capture README screenshots with headless Chrome. Seed and start the server first:

uv run durableflow demo --db data/demo.db
uv run durableflow server --db data/demo.db --workers 1 --port 8823
uv run python scripts/screenshot.py --port 8823
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome",
    "chromium",
]
DOCS = Path(__file__).resolve().parents[1] / "docs"


def chrome() -> str:
    for c in CHROME_PATHS:
        if Path(c).exists() or shutil.which(c):
            return c
    raise SystemExit("Chrome not found")


def shoot(url: str, out: Path, width: int, height: int, scale: float = 1.0) -> None:
    args = [
        chrome(),
        "--headless=new",
        "--disable-gpu",
        "--hide-scrollbars",
        f"--force-device-scale-factor={scale}",
        f"--window-size={width},{height}",
        "--virtual-time-budget=6000",
        f"--screenshot={out}",
        url,
    ]
    subprocess.run(args, check=True, capture_output=True, timeout=60)
    print(f"wrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8823)
    ap.add_argument("--workflow", default="onboard-dana.okafor")
    a = ap.parse_args()
    base = f"http://127.0.0.1:{a.port}"
    DOCS.mkdir(exist_ok=True)
    shoot(f"{base}/#/wf/{a.workflow}", DOCS / "timeline.png", 1400, 1500)
    shoot(f"{base}/#/", DOCS / "list.png", 1400, 820)


if __name__ == "__main__":
    main()
