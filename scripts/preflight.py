#!/usr/bin/env python3
"""Check that everything `make reproduce` needs is present and actually works.

This runs *before* `distclean`. That ordering is the point: the old
`reproduce` target deleted every result file first and discovered the missing
tool afterwards, so a reader with no Docker lost the committed results and got
an error twenty minutes later.

The rule here is that existence is not evidence. macOS ships `/usr/bin/javac`
as a stub that exists, is executable, and fails with "Unable to locate a Java
Runtime" the moment you run it; `shutil.which("javac")` said yes to it for
months. Every check below therefore *executes* the tool and looks at the exit
status, rather than asking whether a file is on PATH.

Usage:  python scripts/preflight.py [--quiet]
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from typing import NamedTuple

#: How long the full `make reproduce` takes on the machine the paper was
#: built on (Apple M-series, Docker Desktop). Most of it is E3: 48 live
#: executions that really kill a process and wait for a consumer group to
#: evict it, plus the scaled downtimes the witnesses specify.
EXPECTED_MINUTES = 25


class Missing(NamedTuple):
    what: str
    detail: str
    fix: str


def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    """Exit status and combined output, or (127, reason) if it will not run."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return 127, f"{cmd[0]}: not found on PATH"
    except subprocess.TimeoutExpired:
        return 124, f"{' '.join(cmd)}: timed out after {timeout}s"
    except OSError as exc:
        return 126, f"{' '.join(cmd)}: {exc}"
    return p.returncode, (p.stdout + p.stderr).strip()


def check_python() -> Missing | None:
    if sys.version_info < (3, 11):
        return Missing("python", f"{sys.version.split()[0]}",
                       "the experiments use 3.11+ syntax; install a newer python3")
    return None


def check_docker() -> Missing | None:
    code, out = _run(["docker", "version", "--format", "{{.Server.Version}}"])
    if code != 0:
        return Missing("docker", out[:300],
                       "install Docker and make sure the daemon is running")
    return None


def check_compose() -> Missing | None:
    code, out = _run(["docker", "compose", "version"])
    if code != 0:
        return Missing("docker compose", out[:300],
                       "install the Compose v2 plugin (`docker compose`, not "
                       "`docker-compose`)")
    return None


def check_latex() -> Missing | None:
    for tool in ("latexmk", "pdflatex"):
        code, out = _run([tool, "--version"])
        if code != 0:
            return Missing(tool, out[:300],
                           "install a TeX distribution (TeX Live or MacTeX)")
    return None


def check_jdk_image() -> Missing | None:
    """The probe's JDK is a pinned image, so the host needs no Java at all.

    Checked here anyway: pulling ~450 MB is the single slowest surprise in the
    run, and it is better to fail at the start than nineteen minutes in.
    """
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent /
           "experiments" / "e8_broker.py").read_text()
    image = next((l.split('"')[1] for l in src.splitlines()
                  if l.startswith("JDK_IMAGE")), None)
    if image is None:
        return Missing("JDK image", "e8_broker.py does not name one",
                       "this is a bug in the repository, not your machine")
    code, out = _run(["docker", "image", "inspect", image], timeout=60)
    if code != 0:
        code, out = _run(["docker", "pull", image], timeout=900)
        if code != 0:
            return Missing(f"JDK image {image}", out[-300:],
                           f"`docker pull {image}` -- the broker-refusal probe "
                           "compiles and runs inside it")
    return None


CHECKS = (
    ("python 3.11+", check_python),
    ("docker daemon", check_docker),
    ("docker compose v2", check_compose),
    ("latexmk / pdflatex", check_latex),
    ("pinned JDK image", check_jdk_image),
)


def main(argv: list[str]) -> int:
    quiet = "--quiet" in argv
    problems = []
    for label, fn in CHECKS:
        problem = fn()
        if problem is None:
            if not quiet:
                print(f"  ok      {label}")
        else:
            problems.append((label, problem))
            print(f"  MISSING {label}")

    if problems:
        print("\npreflight failed; nothing has been deleted.\n")
        for label, p in problems:
            print(f"{label}\n  {p.detail}\n  fix: {p.fix}\n")
        return 1

    if not quiet:
        host_java = shutil.which("java")
        print(f"\n  note    a host JDK is not required; the broker-refusal "
              f"probe compiles and runs in the pinned image"
              f"{' (host java is present and will be ignored)' if host_java else ''}")
        print(f"  note    the full reproduction takes about "
              f"{EXPECTED_MINUTES} minutes, most of it the 48 live executions "
              f"in E3")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
