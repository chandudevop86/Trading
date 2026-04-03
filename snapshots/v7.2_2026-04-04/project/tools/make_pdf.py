from __future__ import annotations

import argparse
from pathlib import Path


def _escape_pdf_text(s: str) -> str:
    return s.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')


def text_to_pdf(text: str, out_path: Path, title: str = "Document") -> None:
    lines = text.splitlines()
    page_width = 595
    page_height = 842
    margin_left = 50
    margin_top = 800
    line_height = 14
    max_lines_per_page = 52

    pages = []
    for i in range(0, len(lines), max_lines_per_page):
        chunk = lines[i : i + max_lines_per_page]
        y = margin_top
        content_lines = ["BT", "/F1 10 Tf"]
        for line in chunk:
            content_lines.append(f"1 0 0 1 {margin_left} {y} Tm ({_escape_pdf_text(line)}) Tj")
            y -= line_height
        content_lines.append("ET")
        pages.append("\n".join(content_lines).encode("latin-1", errors="replace"))

    objects: list[bytes] = []

    # 1: Catalog
    # 2: Pages
    # 3: Font
    # page objects start at 4
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")

    kids_refs = []
    page_obj_indices = []
    content_obj_indices = []

    base_idx = 4
    for idx, page_content in enumerate(pages):
        page_obj = base_idx + (idx * 2)
        content_obj = page_obj + 1
        page_obj_indices.append(page_obj)
        content_obj_indices.append(content_obj)
        kids_refs.append(f"{page_obj} 0 R")

    pages_obj = f"<< /Type /Pages /Kids [{' '.join(kids_refs)}] /Count {len(pages)} >>".encode("latin-1")
    objects.append(pages_obj)

    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    for i, page_content in enumerate(pages):
        page_obj = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_obj_indices[i]} 0 R >>"
        ).encode("latin-1")
        content_obj = f"<< /Length {len(page_content)} >>\nstream\n".encode("latin-1") + page_content + b"\nendstream"
        objects.append(page_obj)
        objects.append(content_obj)

    pdf = bytearray()
    pdf.extend(b"%PDF-1.4\n")

    offsets = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{i} 0 obj\n".encode("latin-1"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_start = len(pdf)
    pdf.extend(f"xref\n0 {len(objects)+1}\n".encode("latin-1"))
    pdf.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.extend(f"{off:010d} 00000 n \n".encode("latin-1"))

    trailer = (
        f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\n"
        f"startxref\n{xref_start}\n%%EOF\n"
    )
    pdf.extend(trailer.encode("latin-1"))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(pdf)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert plain text to a basic PDF")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--title", default="Document")
    args = parser.parse_args()

    text = args.input.read_text(encoding="utf-8")
    text_to_pdf(text, args.output, title=args.title)


if __name__ == "__main__":
    main()
