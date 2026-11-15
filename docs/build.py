#!/usr/bin/env python3
"""Build docs/cassango.pdf from docs/src/*.html with headless Chrome.

Two passes: the first finds the page each section lands on, the second fills in the
table of contents. Needs Google Chrome and `pip install pypdf`.

    python3 docs/build.py
"""
import html
import re
import subprocess
import sys
import tempfile
import unicodedata
from pathlib import Path

from pypdf import PdfReader

DOCS = Path(__file__).resolve().parent
PARTS = ["head.html", "part1.html", "part2.html", "appendix.html"]
OUT = DOCS / "cassango.pdf"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def headings(source: str):
    """(level, id, text) for part titles and numbered sections, in document order."""
    pattern = re.compile(
        r'<section class="part-title" id="([^"]+)">.*?<h1>(.*?)</h1>'
        r'|<h2[^>]*id="([^"]+)"[^>]*>(.*?)</h2>',
        re.S,
    )
    for m in pattern.finditer(source):
        if m.group(1):
            yield "part", m.group(1), m.group(2)
        else:
            yield "sub", m.group(3), m.group(4)


def plain(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", fragment))


def toc(entries, pages):
    rows = "\n".join(
        f'<li class="{"part" if level == "part" else ""}"><a href="#{hid}">{plain(text)}</a>'
        f'<span class="dots"></span><span class="pg">{pages.get(hid, "")}</span></li>'
        for level, hid, text in entries
    )
    return f'<section class="toc"><h2>Contents</h2><ol>\n{rows}\n</ol></section>'


def render(source: str, pdf: Path):
    with tempfile.NamedTemporaryFile("w", suffix=".html", dir=DOCS, delete=False) as f:
        f.write(source)
        page = Path(f.name)
    try:
        subprocess.run(
            [CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
             f"--print-to-pdf={pdf}", page.as_uri()],
            check=True, capture_output=True,
        )
    finally:
        page.unlink()


def find_pages(pdf: Path, entries):
    """The first page (after the contents) holding each heading, searching in order."""
    squash = lambda s: re.sub(r"\s+", "", unicodedata.normalize("NFKC", s))
    texts = [squash(p.extract_text() or "") for p in PdfReader(pdf).pages]
    pages, start = {}, 2
    for _, hid, text in entries:
        needle = squash(plain(text))
        found = next((i for i in range(start, len(texts)) if needle in texts[i]), None)
        if found is None:
            print(f"warning: heading not found in PDF: {plain(text)}", file=sys.stderr)
            continue
        pages[hid] = found + 1
        start = found
    return pages


def main():
    source = "".join((DOCS / "src" / p).read_text() for p in PARTS)
    entries = list(headings(source))
    render(source.replace("<!--TOC-->", toc(entries, {})), OUT)
    pages = find_pages(OUT, entries)
    render(source.replace("<!--TOC-->", toc(entries, pages)), OUT)
    print(f"{OUT} · {len(PdfReader(OUT).pages)} pages · {len(entries)} sections")


if __name__ == "__main__":
    main()
