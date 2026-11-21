"""Build the arXiv package from the paper, rather than beside it.

The submission tree is flat: no ``../figures`` and no subdirectories. Copying
by hand is how a package ends up one figure behind the paper it claims to be,
which is exactly what happened here once. So derive it, and check afterwards
that every file the tex asks for is present.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
OUT = PAPER / "arxiv"

#: Fields a human chooses once. Everything else in the metadata file is counted
#: from the source or read off the built PDF, because the hand-maintained
#: version drifted: it claimed 7 pages and carried a literal
#: "10.5281/zenodo.PLACEHOLDER" into the Comments field.
TITLE = ("The Defect Is in the Arithmetic Between the Files: Static Detection "
         "of Exactly-Once Violations in Payment Pipelines")
AUTHORS = "Abhishek Sharma"
PRIMARY_CLASS = "cs.SE"
#: cs.DC is defensible: the defects are properties of checkpoint/replay and
#: broker transaction semantics in a distributed stream processor. cs.DB was
#: dropped -- nothing here is about data management, and listing it would be
#: for reach rather than fit.
CROSS_LIST = "cs.DC"
ACM_CLASS = "D.2.4; D.2.5"
REPO_URL = "https://github.com/abhisheksharma2411/streamcheck"


def _counts(tex: str, pdf: Path) -> dict:
    pages = ""
    if pdf.exists():
        out = subprocess.run(["pdfinfo", str(pdf)], capture_output=True,
                             text=True).stdout
        for line in out.splitlines():
            if line.startswith("Pages"):
                pages = line.split()[-1]
    return {
        "pages": pages,
        # Count float environments, not \includegraphics: a figure may be
        # typeset rather than included.
        "figures": len(re.findall(r"\\begin\{figure\*?\}", tex)),
        "tables": len(re.findall(r"\\begin\{table\*?\}", tex)),
    }


def _release_tag() -> str:
    """The tag CITATION.cff names, cross-checked against the paper.

    The artifact statement names a release and CITATION.cff names a version.
    They are edited at different times and nothing linked them. Disagreeing is
    a build failure, not a note in the output.
    """
    cff = (ROOT / "CITATION.cff").read_text()
    m = re.search(r"^version:\s*v?(\S+)", cff, re.M)
    if not m:
        raise SystemExit("CITATION.cff has no version field")
    tag = f"v{m.group(1)}"
    claimed = re.search(r"at\s+release\s+\\texttt\{v?([0-9][^}]*)\}",
                        (PAPER / "streamcheck.tex").read_text())
    if not claimed:
        raise SystemExit("the artifact statement names no release")
    if f"v{claimed.group(1)}" != tag:
        raise SystemExit(
            f"release mismatch: the paper says v{claimed.group(1)}, "
            f"CITATION.cff says {tag}. Reconcile them before submitting.")
    return tag


def _tag_exists(tag: str) -> str:
    """Whether the release the paper points a reader at actually exists.

    A warning rather than an error: the tag is cut after the manuscript is
    final, so the build has to work before it exists. But the artifact
    statement is a promise to a reader, and shipping it unkept is the kind of
    thing nothing else here would notice.
    """
    try:
        out = subprocess.run(["git", "tag", "--list", tag], cwd=ROOT,
                             capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return "could not read git tags"
    if out.stdout.strip() != tag:
        return f"{tag} does not exist yet -- the paper points readers at it"
    remote = subprocess.run(["git", "ls-remote", "--tags", "origin", tag],
                            cwd=ROOT, capture_output=True, text=True)
    if remote.returncode != 0 or tag not in remote.stdout:
        return f"{tag} is local only -- not pushed, so no reader can fetch it"
    return ""


def _artifact_doi(tex: str) -> str:
    """The archive DOI, read from the paper so the two cannot disagree.

    arXiv's own DOI field is for a journal DOI of this paper. A software DOI
    put there asserts a published version that does not exist, so the archive
    DOI goes in Comments instead -- and it goes there automatically, because a
    DOI minted after the manuscript was written is exactly the thing that gets
    added in one place and forgotten in the other.
    """
    m = re.search(r"archived at DOI\s*\\mbox\{\\texttt\{([^}]+)\}\}", tex)
    return m.group(1).replace(r"\_", "_") if m else ""


def _metadata(tex: str, pdf: Path, tag: str) -> str:
    c = _counts(tex, pdf)
    doi = _artifact_doi(tex)
    archived = f" Archived at DOI {doi}." if doi else ""
    comments = (
        f"{c['pages']} pages, {c['figures']} figures, {c['tables']} tables. "
        f"Checker, corpus generator, Docker configuration and every raw "
        f"results file are at {REPO_URL} (release {tag}); `make reproduce` "
        f"rebuilds every figure, table and number in the paper from the raw "
        f"results.{archived}"
    )
    return f"""TITLE
{TITLE}

AUTHORS
{AUTHORS}

PRIMARY CLASS
{PRIMARY_CLASS}

CROSS-LIST
{CROSS_LIST}

COMMENTS
{comments}

REPORT NUMBER
(leave blank -- no institutional report series)

JOURNAL REFERENCE
(leave blank -- not published in a journal or proceedings)

EXTERNAL DOI
(leave blank. This field is for a DOI of THIS PAPER in a journal or
proceedings. A Zenodo *software* DOI is not that, and putting one here asserts
a published version that does not exist. If the artifact is archived, its DOI
belongs in COMMENTS and in the paper's Artifact Availability section.)

ACM CLASS
{ACM_CLASS}

MSC CLASS
(leave blank -- not a mathematics paper)

UPLOAD
paper/streamcheck-arxiv.tar.gz -- one file. Do not upload the PDF; arXiv
compiles the source. The abstract to paste is paper/arxiv_abstract.txt.
"""


def main() -> int:
    tex = (PAPER / "streamcheck.tex").read_text()
    flat = tex.replace("{../figures/", "{")

    OUT.mkdir(exist_ok=True)
    for stale in OUT.glob("*"):
        stale.unlink()
    (OUT / "streamcheck.tex").write_text(flat)

    wanted = set(re.findall(r"\\includegraphics\[[^\]]*\]\{([^}]+)\}", flat))
    wanted |= {f"{n}.tex" for n in re.findall(r"\\input\{([^}]+)\}", flat)}

    missing = []
    for name in sorted(wanted):
        src = (ROOT / "figures" / name)
        if not src.exists():
            src = PAPER / name
        if src.exists():
            shutil.copy2(src, OUT / name)
        else:
            missing.append(name)


    if missing:
        print(f"MISSING from the package: {missing}")
        return 1
    tarball = PAPER / "streamcheck-arxiv.tar.gz"
    with tarfile.open(tarball, "w:gz") as tar:
        for f in sorted(OUT.iterdir()):
            tar.add(f, arcname=f.name)

    ok, detail = _verify(OUT, "streamcheck")
    abstract = _abstract("streamcheck")
    (PAPER / "arxiv_abstract.txt").write_text(abstract + "\n")

    tag = _release_tag()
    (PAPER / "arxiv_metadata.txt").write_text(
        _metadata(tex, PAPER / "streamcheck-arxiv-verified.pdf", tag))

    print(f"arxiv package: {len(wanted)} referenced files + streamcheck.tex")
    for f in sorted(OUT.iterdir()):
        print(f"  {f.name}")
    print(f"  tarball     {tarball.relative_to(ROOT)} ({tarball.stat().st_size/1024:.0f} KB)")
    print(f"  standalone  {"OK" if ok else "FAILED"}, {detail}")
    print(f"  abstract    {len(abstract)} chars (arXiv limit 1920)")
    print(f"  metadata    paper/arxiv_metadata.txt (release {tag})")
    warning = _tag_exists(tag)
    if warning:
        print(f"  RELEASE     {warning}")
    nonascii = sorted({c for c in abstract if ord(c) > 127})
    print(f"  abstract ascii-only: {not nonascii}"
          + (f" -- offending: {nonascii}" if nonascii else ""))
    if nonascii:
        print("  ERROR: the abstract must be ASCII for the submission form")
        return 1
    if len(abstract) > 1920:
        print("  ERROR: abstract exceeds the arXiv limit")
        return 1
    return 0 if ok else 1


def _verify(out: Path, main: str) -> tuple[bool, str]:
    """Compile in a scratch copy holding only the submission files.

    arXiv unpacks the tarball into one directory and builds there. The only
    way to know the package works is to build it the way arXiv will, with
    nothing else on the path.
    """
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp) / "sub"
        shutil.copytree(out, scratch)
        log = ""
        for _ in range(3):
            log = subprocess.run(
                ["pdflatex", "-interaction=nonstopmode", f"{main}.tex"],
                cwd=scratch, capture_output=True, text=True,
            ).stdout
        pdf = scratch / f"{main}.pdf"
        if not pdf.exists():
            return False, log[-2000:]
        problems = []
        for pattern, label in (
            (r"Citation .* undefined", "undefined citation"),
            (r"Reference .* undefined", "undefined reference"),
            (r"File .* not found", "missing file"),
            (r"LaTeX Error", "LaTeX error"),
        ):
            hits = re.findall(pattern, log)
            if hits:
                problems.append(f"{len(hits)} {label}(s)")
        info = subprocess.run(["pdfinfo", str(pdf)], capture_output=True, text=True)
        page = next((l for l in info.stdout.splitlines() if l.startswith("Pages")), "")
        shutil.copy2(pdf, PAPER / f"{main}-arxiv-verified.pdf")
        return (not problems), page + ("; " + ", ".join(problems) if problems else "; clean")


def _abstract(main: str) -> str:
    """The abstract with macros resolved, for pasting into the submission form.

    Kept generated rather than hand-copied: a stale abstract file is the one
    thing here that gets pasted somewhere permanent.
    """
    tex = (PAPER / f"{main}.tex").read_text()
    numbers = PAPER / "numbers.tex"
    macros = dict(re.findall(r"\\newcommand\{\\(\w+)\}\{([^}]*)\}",
                             numbers.read_text())) if numbers.exists() else {}
    match = re.search(r"\\begin\{abstract\}(.*?)\\end\{abstract\}", tex, re.S)
    body = match.group(1) if match else ""
    for name, value in macros.items():
        body = body.replace(f"\\{name}{{}}", value).replace(f"\\{name}", value)
    for cmd in ("textbf", "emph", "texttt", "textit"):
        body = re.sub(r"\\" + cmd + r"\{([^}]*)\}", r"\1", body)
    body = body.replace("\\%", "%").replace("\\$", "$").replace("\\&", "&")
    body = body.replace("\\ ", " ").replace("~", " ")
    body = body.replace("---", "--").replace("``", '"').replace("\'\'", '"')
    body = body.replace("{,}", ",")
    body = re.sub(r"\s+", " ", body).strip()
    return body


if __name__ == "__main__":
    sys.exit(main())
