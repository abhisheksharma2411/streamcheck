"""Write the minted Zenodo DOI into the paper and CITATION.cff.

The placeholder appears LaTeX-escaped in the paper (``ZENODO\\_DOI\\_PLACEHOLDER``)
and unescaped in the CFF.  An earlier version of this script in a sibling
project matched only the unescaped spelling, reported success on the CFF and
silently changed nothing in the paper, so both spellings are handled here and
the script fails loudly if a target it expected to change did not.

Usage:  python scripts/insert_doi.py 10.5281/zenodo.XXXXXXX
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper" / "streamcheck.tex"
CFF = ROOT / "CITATION.cff"
README = ROOT / "README.md"

#: The paper ships saying the DOI is coming rather than showing a placeholder,
#: because a literal ``ZENODO_DOI_PLACEHOLDER`` in a posted preprint is worse
#: than an honest sentence. This is the sentence to replace when the DOI exists.
FORTHCOMING = "An archival DOI is forthcoming."

#: Once the "forthcoming" sentence is gone, the DOI is appended after the
#: artifact statement's release sentence instead. Matched as a regex because
#: the LaTeX source is hard-wrapped and the sentence spans a line break --- a
#: literal anchor silently matches nothing, which is how a scripted edit
#: reports success having written nothing.
ANCHOR_RE = re.compile(
    r"which\s+is\s+the\s+revision\s+this\s+paper\s+was\s+built\s+from\.")

PLAIN = "ZENODO_DOI_PLACEHOLDER"
ESCAPED = r"ZENODO\_DOI\_PLACEHOLDER"


def main(doi: str) -> int:
    if not doi.startswith("10."):
        print(f"refusing to insert {doi!r}: does not look like a DOI")
        return 2

    changed, missed = [], []

    text = PAPER.read_text()
    if FORTHCOMING in text:
        escaped = doi.replace("_", r"\_")
        PAPER.write_text(text.replace(
            FORTHCOMING, f"It is archived at DOI \\texttt{{{escaped}}}."))
        changed.append(PAPER.name)
    elif ESCAPED in text or PLAIN in text:
        PAPER.write_text(text.replace(ESCAPED, doi.replace("_", r"\_")).replace(PLAIN, doi))
        changed.append(PAPER.name)
    elif "archived at DOI" in text:
        # Already inserted. Appending after the anchor a second time is how
        # this script shipped a paragraph carrying the same sentence twice
        # in a test; running it again must be a no-op, not an append.
        missed.append(f"{PAPER.name} (already names a DOI)")
    elif ANCHOR_RE.search(text):
        escaped = doi.replace("_", r"\_")
        PAPER.write_text(ANCHOR_RE.sub(
            lambda m: m.group(0) + f" The release is archived at DOI "
                                   f"\\texttt{{{escaped}}}.",
            text, count=1))
        changed.append(PAPER.name)
    else:
        missed.append(PAPER.name)

    cff = CFF.read_text()
    if "doi:" not in cff.split("references:")[0]:
        lines = cff.splitlines()
        for i, line in enumerate(lines):
            if line.startswith("license:"):
                lines.insert(i, f"doi: {doi}")
                break
        CFF.write_text("\n".join(lines) + "\n")
        changed.append(CFF.name)
    else:
        missed.append(f"{CFF.name} (already has a top-level doi)")

    readme = README.read_text()
    if "ZENODO" in readme:
        README.write_text(readme.replace(PLAIN, doi))
        changed.append(README.name)

    print("updated:", ", ".join(changed) or "nothing")
    if missed:
        print("not updated:", ", ".join(missed))
    if PAPER.name in [m.split()[0] for m in missed] and "archived at DOI" not in PAPER.read_text():
        print("ERROR: the paper still has no DOI. Check the placeholder spelling.")
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
