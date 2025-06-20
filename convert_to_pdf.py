from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from io import BytesIO
import re

def format_with_icons_and_bold(text: str):
    def bold_caps_words(match):
        word = match.group()
        return f"<b>{word}</b>"

    text = re.sub(r'\b[A-Z]{3,}\b', bold_caps_words, text)
    return text

def generate_structured_pdf(text: str, title="CPALMS Lesson Plan"):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=60, bottomMargin=60)
    story = []

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Header', fontSize=14, leading=16, spaceAfter=10, spaceBefore=20, fontName="Helvetica-Bold"))
    styles.add(ParagraphStyle(name='Question', fontSize=12, leading=15, spaceBefore=10, spaceAfter=4, fontName="Helvetica-Bold"))
    styles.add(ParagraphStyle(name='Indented', fontSize=11, leading=14, leftIndent=20, spaceAfter=6))
    styles.add(ParagraphStyle(name='Answer', fontSize=11, leading=14, leftIndent=30, textColor="#444444", fontName="Helvetica-Oblique"))

    story.append(Paragraph(f"<b>{title}</b>", styles['Header']))
    story.append(Spacer(1, 12))

    lines = text.split('\n')
    for line in lines:
        stripped = line.strip()
        if not stripped:
            story.append(Spacer(1, 8))
            continue

        formatted = format_with_icons_and_bold(stripped)
        if stripped.lower().startswith("question"):
            story.append(Paragraph(formatted, styles['Question']))
        elif stripped.lower().startswith("objective:"):
            story.append(Paragraph(formatted, styles['Indented']))
        elif stripped.lower().startswith("student writes:") or stripped.lower().startswith("correct answer:") or stripped.lower().startswith("answer:"):
            story.append(Paragraph(formatted, styles['Answer']))
        else:
            story.append(Paragraph(formatted, styles['Normal']))

    doc.build(story)
    buffer.seek(0)
    return buffer
