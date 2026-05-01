import requests
import re
import time
import logging
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
import hashlib

logger = logging.getLogger("Pipeline")

# Função para limpar tags XML do texto
def limpar_xml(texto):
    if not texto: return ""
    return " ".join(re.sub(r"<[^>]+>", " ", texto).split())

# Extrai artigos do Crossref com base na query e nos identificadores existentes
def extrair_crossref(query, existing_identifiers):
    theme_formatted = query.title()
    cursor = '*'
    count_da_query = 0
    novos_artigos = []

    logger.info(f"API Iniciada: Crossref | Tema: {theme_formatted}")

    headers = {"User-Agent": settings.USER_AGENT}

    while cursor:
        # Filtro de data incluído (from-pub-date)
        url = f"https://api.crossref.org/works?query.title={query.replace(' ', '+')}&filter=type:journal-article,from-pub-date:{settings.DATA_LIMITE}&rows={settings.ROWS_CROSSREF}&cursor={cursor}"

        try:
            time.sleep(1)
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 429:
                logger.warning("Bloqueio de IP (429) na Crossref. Abortando extracao atual.")
                break
            if resp.status_code != 200:
                break

            data = resp.json().get("message", {})
            items = data.get("items", [])
            if not items:
                break

            for art in items:
                title = art.get("title", [""])[0]
                autores = [f"{a.get('given', '')} {a.get('family', '')}".strip() for a in art.get("author", [])]
                autores = [a for a in autores if a]

                title_norm = title.lower().strip()
                authors_norm = "".join(sorted([a.lower().strip() for a in autores]))

                if (title_norm, authors_norm) in existing_identifiers:
                    continue

                dp = art.get("published-print", art.get("published-online", {})).get("date-parts", [[None]])
                article_url = art.get("URL", "")

                sha256_id = hashlib.sha256(article_url.encode('utf-8')).hexdigest()

                artigo = {
                    "id": sha256_id,
                    "id_api": "crossref_api",
                    "url": article_url,
                    "website": art.get("publisher", "Crossref"),
                    "theme": theme_formatted,
                    "title": title,
                    "abstract": limpar_xml(art.get("abstract", "")),
                    "authors": autores,
                    "year_publicacion": str(dp[0][0]) if dp[0][0] else "N/A",
                    "cites": art.get("is-referenced-by-count", 0),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                novos_artigos.append(artigo)
                existing_identifiers.add((title_norm, authors_norm))
                count_da_query += 1

                logger.info(f"Artigo adicionado. API: Crossref | Tema: {theme_formatted} | URL: {article_url}")

            cursor = data.get("next-cursor")

        except Exception as e:
            logger.error(f"Erro na Crossref | Tema: {theme_formatted} | Detalhe: {e}")
            break


    return novos_artigos