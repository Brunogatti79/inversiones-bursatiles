from .pdf_parser import parse_pdf
from .docx_parser import parse_docx
from .xlsx_parser import parse_xlsx
from .txt_parser import parse_txt

PARSERS = {
    ".pdf":  parse_pdf,
    ".docx": parse_docx,
    ".doc":  parse_docx,
    ".xlsx": parse_xlsx,
    ".xls":  parse_xlsx,
    ".txt":  parse_txt,
    ".csv":  parse_txt,
    ".md":   parse_txt,
}

SUPPORTED_EXTENSIONS = set(PARSERS.keys())
