import json
import os
import logging
import sys
import time
import random
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from src.scrapers.openalex.openalex_scraper import extrair_openalex
from src.scrapers.crossref.crossref_scraper import extrair_crossref
from src.scrapers.ucirvine.ucirvine_scraper import extrair_ucirvine

os.makedirs(os.path.dirname(settings.LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(settings.DATA_PATH), exist_ok=True)

# Configuração de Logging
logger = logging.getLogger("Pipeline")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(settings.LOG_PATH, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Função para guardar dados no arquivo JSON
def guardar_dados(novos_itens, chave_raiz):
    if not novos_itens:
        return

    conteudo_atual = {"articles": [], "datasets": []}

    if os.path.exists(settings.DATA_PATH):
        with open(settings.DATA_PATH, "r", encoding="utf-8") as f:
            try:
                conteudo_atual = json.load(f)
            except json.JSONDecodeError:
                pass

    if chave_raiz not in conteudo_atual:
        conteudo_atual[chave_raiz] = []

    conteudo_atual[chave_raiz].extend(novos_itens)

    with open(settings.DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(conteudo_atual, f, ensure_ascii=False, indent=2)

# Função principal para executar o pipeline completo
def executar_pipeline():
    tempo_inicio = time.time()
    logger.info("Processo de extracao iniciado.")

    existing_titles = set()
    if os.path.exists(settings.DATA_PATH):
        try:
            with open(settings.DATA_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                for categoria in ["articles", "datasets"]:
                    for item in data.get(categoria, []):
                        existing_titles.add(item["title"].lower().strip())
        except Exception as e:
            logger.error(f"Erro ao carregar base local: {e}")

    total_novos_sessao = 0

    # UCIrvine (Web Scraping)
    try:
        uci_config = {
            'subjects': settings.UCI_SUBJECTS,
            'max_datasets_per_subject': settings.UCI_MAX_PER_SUBJECT
        }

        novos_datasets = extrair_ucirvine(uci_config, existing_titles)
        if novos_datasets:
            guardar_dados(novos_datasets, "datasets")
            total_novos_sessao += len(novos_datasets)
            # Log de finalização da fonte seguindo o padrão
            logger.info(f"Finalizado. Fonte: UCIrvine | Datasets extraidos: {len(novos_datasets)}")
    except Exception as e:
        logger.error(f"Falha critica no scraper UCIrvine: {e}")

    # OpenAlex (API)
    for query in settings.QUERIES:
        try:
            artigos_oa = extrair_openalex(query, existing_titles)
            if artigos_oa:
                guardar_dados(artigos_oa, "articles")
                total_novos_sessao += len(artigos_oa)
                logger.info(f"Finalizado. API: OpenAlex | Tema: {query} | Artigos extraidos: {len(artigos_oa)}")
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            logger.error(f"Erro na query '{query}' do OpenAlex: {e}")

    # Crossref (API)
    for query in settings.QUERIES:
        try:

            artigos_cr = extrair_crossref(query, existing_titles)
            if artigos_cr:
                guardar_dados(artigos_cr, "articles")
                total_novos_sessao += len(artigos_cr)
                logger.info(f"Finalizado. API: Crossref | Tema: {query} | Artigos extraidos: {len(artigos_cr)}")
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            logger.error(f"Erro na query '{query}' do Crossref: {e}")

    tempo_fim = time.time()
    duracao_segundos = tempo_fim - tempo_inicio
    minutos, segundos = divmod(duracao_segundos, 60)


    logger.info(f"Processo concluido em {int(minutos)}m {int(segundos)}s. Total de artigos extraidos nesta sessão: {total_novos_sessao}")


if __name__ == "__main__":
    executar_pipeline()