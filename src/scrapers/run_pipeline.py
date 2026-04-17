import json
import os
import logging
import sys
import time


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from src.scrapers.openalex.openalex_scraper import extrair_openalex
from src.scrapers.crossref.crossref_scraper import extrair_crossref


os.makedirs(os.path.dirname(settings.LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(settings.DATA_PATH), exist_ok=True)


logger = logging.getLogger("Pipeline")
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(settings.LOG_PATH, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def guardar_dados(novos_artigos):
    if not novos_artigos:
        return

    dados_finais = {"articles": []}

    if os.path.exists(settings.DATA_PATH):
        with open(settings.DATA_PATH, "r", encoding="utf-8") as f:
            try:
                dados_finais = json.load(f)
            except json.JSONDecodeError:
                dados_finais = {"articles": []}

    dados_finais["articles"].extend(novos_artigos)

    with open(settings.DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(dados_finais, f, ensure_ascii=False, indent=2)


def executar_pipeline():

    tempo_inicio = time.time()
    logger.info("Processo de extracao iniciado.")

    existing_ids = set()
    if os.path.exists(settings.DATA_PATH):
        with open(settings.DATA_PATH, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                for art in data.get("articles", []):
                    t_norm = art["title"].lower().strip()
                    a_norm = "".join(sorted([a.lower().strip() for a in art["authors"]]))
                    existing_ids.add((t_norm, a_norm))
            except Exception as e:
                logger.error(f"Erro ao ler base local: {e}")

    total_artigos_sessao = 0

    for query in settings.QUERIES:
        artigos_oa = extrair_openalex(query, existing_ids)
        if artigos_oa:
            guardar_dados(artigos_oa)
            total_artigos_sessao += len(artigos_oa)

    for query in settings.QUERIES:
        artigos_cr = extrair_crossref(query, existing_ids)
        if artigos_cr:
            guardar_dados(artigos_cr)
            total_artigos_sessao += len(artigos_cr)

    tempo_fim = time.time()
    duracao_segundos = tempo_fim - tempo_inicio

    minutos, segundos = divmod(duracao_segundos, 60)
    tempo_formatado = f"{int(minutos)}m {int(segundos)}s"

    logger.info(
        f"Processo concluido em {tempo_formatado}. Total de artigos extraidos nesta sessão: {total_artigos_sessao}")

if __name__ == "__main__":
    executar_pipeline()