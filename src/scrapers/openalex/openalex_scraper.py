import requests
import time
import logging
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings
import hashlib

logger = logging.getLogger("Pipeline")

# Extrai artigos do OpenAlex com base na query e nos identificadores existentes
def extrair_openalex(query, existing_identifiers):
    theme_formatted = query.title()
    cursor = '*'
    count_da_query = 0
    novos_artigos = []

    logger.info(f"API Iniciada: OpenAlex | Tema: {theme_formatted}")

    headers = {'User-Agent': settings.USER_AGENT}

    while cursor:
        url = f"https://api.openalex.org/works?search={query}&filter=type:article,from_publication_date:{settings.DATA_LIMITE}&per_page={settings.PER_PAGE_OPENALEX}&cursor={cursor}"

        try:
            time.sleep(2)  # Proteção contra bloqueio
            response = requests.get(url, headers=headers, timeout=60)

            if response.status_code == 429:
                logger.warning("Bloqueio de IP (429) no OpenAlex. Abortando extracao atual.")
                break

            response.raise_for_status()
            data = response.json()

        except Exception as e:
            logger.error(f"Erro no OpenAlex | Tema: {theme_formatted} | Detalhe: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            title = work.get("display_name", "")
            if not title: continue

            authors = [a.get("author", {}).get("display_name") for a in work.get("authorships", []) if
                       a.get("author", {}).get("display_name")]

            title_norm = title.lower().strip()
            authors_norm = "".join(sorted([a.lower().strip() for a in authors]))

            if (title_norm, authors_norm) in existing_identifiers:
                continue

            article_url = work.get("doi") or work.get("id")

            url_para_hash = article_url if article_url else ""
            sha256_id = hashlib.sha256(url_para_hash.encode('utf-8')).hexdigest()

            artigo = {
                "id": sha256_id,
                "id_api": "openalex_api",
                "url": article_url,
                "website": "OpenAlex",
                "theme": theme_formatted,
                "title": title,
                "authors": authors,
                "year_publicacion": str(work.get("publication_year", "")),
                "cites": work.get("cited_by_count", 0),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            novos_artigos.append(artigo)
            existing_identifiers.add((title_norm, authors_norm))
            count_da_query += 1

            logger.info(f"Artigo adicionado. API: OpenAlex | Tema: {theme_formatted} | URL: {article_url}")

        cursor = data.get("meta", {}).get("next_cursor")

    return novos_artigos