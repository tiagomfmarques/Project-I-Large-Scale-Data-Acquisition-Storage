import os
from datetime import datetime, timedelta

# Caminhos e configurações globais
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "scientific_research_data.json")
LOG_PATH = os.path.join(BASE_DIR, "logs", "logs.log")

# Temas de pesquisa para artigos científicos
QUERIES = [
    "artificial intelligence",
    "natural language processing",
    "information retrieval"
]

# Áreas de interesse para datasets
UCI_SUBJECTS = [
    "Computer Science",
    "Health and Medicine",
    "Engineering",
    "Business",
    "Social Sciences"
]

# Limite de datasets por cada Subject Area
UCI_MAX_PER_SUBJECT = 50

# Configurações de API e Scraping
USER_AGENT = "Mozilla/5.0 (ProjetoData; mailto: teste2026@gmail.com)"
PER_PAGE_OPENALEX = 50
ROWS_CROSSREF = 50

# Limite de data para extração (últimos 3 dias)
DATA_LIMITE = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

SELENIUM_HEADLESS = False