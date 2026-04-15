import json
import logging
import os
import re
import time
from datetime import datetime

import requests


#################################################################################
#                                                                               #
#                             CONFIGURAÇÃO DE LOGS                              #
#                                                                               #
#################################################################################
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.CRITICAL:
            return f"\n{'#' * 70}\n# {record.getMessage():^66} #\n{'#' * 70}\n"
        return super().format(record)


logger = logging.getLogger("CrossrefScraper")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    CustomFormatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)
logger.addHandler(console_handler)

file_handler = logging.FileHandler("src/logs/crossref_scraper.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

FICHEIRO_SAIDA = "src/data/crossref/dataset_crossref_ai.json"
HEADERS = {
    "User-Agent": "ProjetoUBI_LargeScaleData/1.0 (mailto:tomas.silva.gomes@ubi.pt)"
}


def limpar_texto_xml(texto: str) -> str:
    if not isinstance(texto, str):
        return ""
    return " ".join(re.sub(r"<[^>]+>", " ", texto).split())


def carregar_dataset_existente():
    if not os.path.exists(FICHEIRO_SAIDA):
        return {}
    with open(FICHEIRO_SAIDA, "r", encoding="utf-8") as f:
        try:
            dados = json.load(f)
            # Indexa pelo URL (Chave Única)
            return {
                art["url"]: art for art in dados.get("articles", []) if "url" in art
            }
        except json.JSONDecodeError:
            return {}


def guardar_dataset(dataset_indexado: dict):
    formato_final = {"articles": list(dataset_indexado.values())}
    with open(FICHEIRO_SAIDA, "w", encoding="utf-8") as f:
        json.dump(formato_final, f, ensure_ascii=False, indent=4)


#################################################################################
#                                                                               #
#                           EXTRAÇÃO POR TEMA                                   #
#                                                                               #
#################################################################################
def extrair_tema(tema, dataset, limite=500):
    logger.info(f"A investigar domínio: {tema.replace('+', ' ')}")
    cursor_atual = "*"
    extraidos_agora = 0
    tamanho_pagina = 100
    novos, atualizados = 0, 0

    while extraidos_agora < limite:
        url = (
            f"https://api.crossref.org/works?"
            f"query.title={tema}"
            f"&select=DOI,title,author,abstract,is-referenced-by-count,URL,published-print,published-online,publisher"
            f"&sort=deposited&order=desc"
            f"&rows={tamanho_pagina}&cursor={cursor_atual}"
        )

        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                break

            items = resp.json().get("message", {}).get("items", [])
            if not items:
                break

            for art in items:
                art_url = art.get("URL")
                if not art_url:
                    continue  # Ignora se não houver link

                dp = art.get("published-print", art.get("published-online", {})).get(
                    "date-parts", [[None]]
                )
                ano_pub = str(dp[0][0]) if dp[0][0] else "N/A"

                # Lista real de autores
                autores = [
                    f"{a.get('given', '')} {a.get('family', '')}".strip()
                    for a in art.get("author", [])
                ]
                autores = [a for a in autores if a]  # Remove vazios

                citacoes = art.get("is-referenced-by-count", 0)
                abs_bruto = art.get("abstract", "")

                registo = {
                    "id_api": "crossref_api",
                    "url": art_url,
                    "website": art.get("publisher", "Crossref"),
                    "theme": "",  ###### A preencher no dataset_crossref.py
                    "title": art.get("title", [""])[0],
                    "abstract": limpar_texto_xml(abs_bruto),
                    "authors": autores,
                    "year_publicacion": ano_pub,
                    "cites": citacoes,
                    "timestamp": datetime.now().isoformat(),
                }

                if art_url in dataset:
                    if dataset[art_url].get("cites") != citacoes:
                        dataset[art_url]["cites"] = citacoes
                        dataset[art_url]["timestamp"] = registo["timestamp"]
                        atualizados += 1
                else:
                    dataset[art_url] = registo
                    novos += 1

                extraidos_agora += 1
                if extraidos_agora >= limite:
                    break

            novo_cursor = resp.json().get("message", {}).get("next-cursor")
            if not novo_cursor or novo_cursor == cursor_atual:
                break
            cursor_atual = novo_cursor
            time.sleep(1)

        except Exception as e:
            logger.error(f"Erro na extração: {e}")
            break

    return novos, atualizados


#################################################################################
#                                                                               #
#                               ORQUESTRADOR                                    #
#                                                                               #
#################################################################################
def iniciar_servico_continuo():
    logger.critical(
        f"Serviço Iniciado: {datetime.now().strftime('%d/%m/%Y - %H:%M:%S')}"
    )
    temas_alvo = [
        "Artificial+Intelligence",
        "Machine+Learning",
        "Retrieval+Augmented+Generation",
        "Autonomous+Agents",
        "Information+Retrieval",
    ]
    ciclo_num = 1

    while True:
        dataset = carregar_dataset_existente()
        logger.critical(
            f"A iniciar CICLO #{ciclo_num} | Dataset atual: {len(dataset)} registos"
        )
        total_novos, total_atualizados = 0, 0

        for tema in temas_alvo:
            n, a = extrair_tema(tema, dataset, limite=500)
            total_novos += n
            total_atualizados += a
            guardar_dataset(dataset)
            logger.info(f"Tema concluído. +{n} novos. Pausa de 5 segundos...")
            time.sleep(5)

        logger.critical(
            f"Fim do CICLO #{ciclo_num}. Total JSON: {len(dataset)} (+{total_novos} novos)"
        )
        minutos_espera = 30
        logger.info(
            f"Modo de repouso. Próxima extração em {minutos_espera} min. (CTRL+C para parar)."
        )
        time.sleep(minutos_espera * 60)
        ciclo_num += 1


if __name__ == "__main__":
    try:
        iniciar_servico_continuo()
    except KeyboardInterrupt:
        logger.critical("Serviço interrompido (CTRL+C).")
