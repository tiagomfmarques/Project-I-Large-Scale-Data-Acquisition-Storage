import json
import os
import sys
from elasticsearch import Elasticsearch, helpers

# Garante que o Python encontra as pastas do projeto
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import settings

# 1. Conexão com o Elasticsearch
# Adiciona o parâmetro compatibility_mode=True
es = Elasticsearch(
    "http://localhost:9200",
    request_timeout=60,
    max_retries=10,
    retry_on_timeout=True
)
INDEX_NAME = "artigos_cientificos"

def carregar_dados_no_elastic():
    if not os.path.exists(settings.DATA_PATH):
        print(f"Erro: Ficheiro {settings.DATA_PATH} nao encontrado.")
        return

    print(f"Ficheiro: {settings.DATA_PATH}")
    with open(settings.DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
        artigos = data.get("articles", [])

    print(f"{len(artigos)} artigos para indexacao")

    # 2. Gerador para Bulk Indexing (Eficiencia de memoria)
    def gerar_acoes():
        for artigo in artigos:
            yield {
                "_index": INDEX_NAME,
                "_source": artigo
            }

    # 3. Enviar para o Elasticsearch
    try:
        success, failed = helpers.bulk(es, gerar_acoes())
        print(f"Sucesso: {success} artigos indexados.")
        print(f"Falhas: {failed}")
    except Exception as e:
        print(f"Erro durante a indexacao: {e}")

if __name__ == "__main__":
    carregar_dados_no_elastic()