from pathlib import Path
import html
import re

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Flowable,
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
REPORT_MD = ROOT / "docs" / "report.md"
REPORT_PDF = ROOT / "docs" / "report.pdf"


def inline_markup(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<font face='Courier'>\1</font>", escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", escaped)
    return escaped


class PipelineFigure(Flowable):
    def __init__(self, width=15.9 * cm, height=6.2 * cm):
        super().__init__()
        self.width = width
        self.height = height

    def draw_box(self, x, y, w, h, title, lines, fill, stroke):
        c = self.canv
        c.setFillColor(fill)
        c.setStrokeColor(stroke)
        c.roundRect(x, y, w, h, 6, fill=1, stroke=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 7.4)
        c.drawString(x + 8, y + h - 14, title)
        c.setFont("Helvetica", 6.4)
        for i, line in enumerate(lines):
            c.drawString(x + 8, y + h - 30 - i * 12, line)

    def arrow(self, x1, y1, x2, y2):
        c = self.canv
        c.setStrokeColor(colors.HexColor("#333333"))
        c.line(x1, y1, x2, y2)
        c.line(x2, y2, x2 - 5, y2 + 3)
        c.line(x2, y2, x2 - 5, y2 - 3)

    def draw(self):
        c = self.canv
        c.setFont("Helvetica-Bold", 11)
        c.drawString(0, self.height - 12, "Assignment 2 Spark Text Processing Pipeline")
        blue = colors.HexColor("#eef6ff")
        green = colors.HexColor("#f7fee7")
        orange = colors.HexColor("#fff7ed")
        dark = colors.HexColor("#1f2937")

        self.draw_box(0, 100, 78, 42, "Input", ["Amazon reviews", "devset JSON"], colors.HexColor("#f8fafc"), dark)
        self.draw_box(108, 88, 118, 72, "Part 1: RDD", ["token sets", "N, Nc, Nt, Ntc", "top 75/category"], blue, colors.HexColor("#2563eb"))
        self.draw_box(260, 100, 86, 42, "output_rdd.txt", ["category rows", "union"], orange, colors.HexColor("#c2410c"))
        self.draw_box(108, 10, 118, 72, "Part 2: ML", ["RegexTokenizer", "CV + IDF", "ChiSq top 2000"], blue, colors.HexColor("#2563eb"))
        self.draw_box(260, 22, 86, 42, "output_ds.txt", ["2000 terms"], orange, colors.HexColor("#c2410c"))
        self.draw_box(385, 4, 128, 102, "Part 3: Classifier", ["60/20/20 split", "ChiSq vs variance", "Normalizer L2", "OneVsRest SVM", "SVM param grid"], blue, colors.HexColor("#2563eb"))
        self.draw_box(385, 126, 128, 48, "Acceleration", ["cache features", "parallel SVM fits"], green, colors.HexColor("#65a30d"))

        self.arrow(78, 121, 108, 121)
        self.arrow(226, 121, 260, 121)
        self.arrow(78, 121, 108, 46)
        self.arrow(226, 46, 260, 46)
        self.arrow(346, 46, 385, 46)
        self.arrow(449, 126, 449, 106)


def parse_markdown(path: Path, styles):
    story = []
    lines = path.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line:
            i += 1
            continue
        if line.startswith("# "):
            story.append(Paragraph(inline_markup(line[2:]), styles["Title"]))
            story.append(Spacer(1, 7))
            i += 1
        elif line.startswith("## "):
            story.append(Spacer(1, 5))
            story.append(Paragraph(inline_markup(line[3:]), styles["Heading2"]))
            i += 1
        elif line.startswith("### "):
            story.append(Spacer(1, 3))
            story.append(Paragraph(inline_markup(line[4:]), styles["Heading3"]))
            i += 1
        elif line.startswith("!["):
            match = re.match(r"!\[([^\]]*)\]\(([^)]+)\)", line)
            alt = match.group(1) if match else ""
            image_path = match.group(2) if match else ""
            story.append(Spacer(1, 4))
            if image_path == "assignment2_pipeline.svg":
                story.append(PipelineFigure())
            else:
                full_path = ROOT / "docs" / image_path
                if full_path.exists():
                    img = Image(str(full_path))
                    max_width = 15.9 * cm
                    max_height = 7.4 * cm
                    scale = min(max_width / img.imageWidth, max_height / img.imageHeight)
                    img.drawWidth = img.imageWidth * scale
                    img.drawHeight = img.imageHeight * scale
                    img.hAlign = "CENTER"
                    story.append(img)
                else:
                    story.append(Paragraph(inline_markup(alt), styles["BodyText"]))
            if alt:
                story.append(Paragraph(f"<i>{inline_markup(alt)}</i>", styles["Caption"]))
            story.append(Spacer(1, 7))
            i += 1
        elif line.startswith("|"):
            table_lines = []
            while i < len(lines) and lines[i].startswith("|"):
                table_lines.append(lines[i])
                i += 1
            rows = []
            for row in table_lines:
                cells = [cell.strip() for cell in row.strip("|").split("|")]
                if all(set(cell) <= {"-", ":"} for cell in cells):
                    continue
                rows.append([Paragraph(inline_markup(cell), styles["Cell"]) for cell in cells])
            if rows:
                table = Table(rows, repeatRows=1, hAlign="LEFT")
                table.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5e7eb")),
                            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#9ca3af")),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 4),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                            ("TOPPADDING", (0, 0), (-1, -1), 3),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                        ]
                    )
                )
                story.append(table)
                story.append(Spacer(1, 6))
        else:
            paragraph = [line]
            i += 1
            while i < len(lines) and lines[i].strip() and not lines[i].startswith(("#", "|", "![")):
                paragraph.append(lines[i].strip())
                i += 1
            story.append(Paragraph(inline_markup(" ".join(paragraph)), styles["BodyText"]))
            story.append(Spacer(1, 5))
    return story


def page_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#4b5563"))
    canvas.drawRightString(A4[0] - 1.5 * cm, 0.9 * cm, f"Page {doc.page}")
    canvas.restoreState()


def main():
    base = getSampleStyleSheet()
    styles = {
        "Title": ParagraphStyle(
            "Title",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=16,
            leading=19,
            alignment=TA_CENTER,
            spaceAfter=8,
        ),
        "Heading2": ParagraphStyle(
            "Heading2",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=15,
            spaceBefore=6,
            spaceAfter=4,
        ),
        "Heading3": ParagraphStyle(
            "Heading3",
            parent=base["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=13,
            spaceBefore=4,
            spaceAfter=3,
        ),
        "BodyText": ParagraphStyle(
            "BodyText",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=11,
            leading=13.4,
            spaceAfter=2,
        ),
        "Cell": ParagraphStyle(
            "Cell",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=11.0,
        ),
        "Caption": ParagraphStyle(
            "Caption",
            parent=base["BodyText"],
            fontName="Helvetica-Oblique",
            fontSize=9,
            leading=10.5,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#4b5563"),
        ),
    }
    doc = SimpleDocTemplate(
        str(REPORT_PDF),
        pagesize=A4,
        rightMargin=1.35 * cm,
        leftMargin=1.35 * cm,
        topMargin=1.25 * cm,
        bottomMargin=1.25 * cm,
        title="Assignment 2 Report",
    )
    story = parse_markdown(REPORT_MD, styles)
    doc.build(story, onFirstPage=page_footer, onLaterPages=page_footer)


if __name__ == "__main__":
    main()
