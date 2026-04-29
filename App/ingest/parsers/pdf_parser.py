import pdfplumber


def parse_pdf(file_path: str) -> str:
    text_parts = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text.strip())
            for table in page.extract_tables():
                for row in table:
                    clean = [str(c).strip() if c else "" for c in row]
                    text_parts.append(" | ".join(clean))
    return "\n\n".join(text_parts)
