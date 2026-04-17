import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "scientific_articles_data.json")
LOG_PATH = os.path.join(BASE_DIR, "logs", "logs.log")

QUERIES = [
    "artificial intelligence",
    "natural language processing",
    "information retrieval"
]

USER_AGENT = "Mozilla/5.0 (ProjetoData; mailto: teste2026@gmail.com)"
PER_PAGE_OPENALEX = 50
ROWS_CROSSREF = 50

DATA_LIMITE = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')