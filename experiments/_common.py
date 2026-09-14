"""Shared setup: paths, provenance, result writing."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
RESULTS = ROOT / "results"
FIGURES = ROOT / "figures"

sys.path.insert(0, str(SRC))


def git_rev() -> str:
    try:
        rev = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                             capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(ROOT), "status", "--porcelain"],
                               capture_output=True, text=True, check=True).stdout.strip()
        return rev + ("-dirty" if dirty else "")
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unversioned"


def provenance() -> dict:
    return {
        "git": git_rev(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def write(name: str, payload: dict) -> Path:
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / name
    path.write_text(json.dumps({"provenance": provenance(), **payload}, indent=2, sort_keys=True) + "\n")
    print(f"wrote {path.relative_to(ROOT)}")
    return path


def load(name: str) -> dict:
    return json.loads((RESULTS / name).read_text())
