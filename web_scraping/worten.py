"""
Scraper da Worten — Componentes de PC
======================================
Arquitectura de duas fases optimizada:

  Fase 1 (curl_cffi, puro HTTP):
    → POST /worten-api/search-products  — já contém nome, preço, rating, imagem e URL
    → Nenhum browser envolvido. Muito rápido.

  Fase 2 (Playwright, selectivo):
    → Apenas para produtos com ratings.cnt > 0 (têm reviews reais)
    → Extrai os textos dos comentários via Bazaarvoice
    → Salta produtos sem reviews — poupa 80-90% das chamadas ao browser

  Scheduler:
    → Corre automaticamente a cada hora (enunciado Tarefa 1)
    → Acumula dados entre corridas, deduplica por URL
    → Regista tudo em worten_scraper.log
"""

import asyncio
import json
import logging
import random
import time
from datetime import datetime
from pathlib import Path

from curl_cffi.requests import AsyncSession
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

FICHEIRO_SAIDA = "dataset_worten_componentes_completo.json"
FICHEIRO_LOG = "worten_scraper.log"
CONCORRENCIA_PLAYWRIGHT = 8
HORAS_ENTRE_EXECUCOES = 1

# URL base das imagens (confirmado via relations.refs no JSON da API)
WORTEN_IMG_BASE = "https://www.worten.pt/i/"

# Context ID da categoria Componentes de PC
# Encontrado em categoriesCanonicalsData → url = /informatica-e-acessorios/componentes
WORTEN_CONTEXT_ID = "01G2Z02T5MMJ4DQV6PBN77A4YZ"

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING — consola + ficheiro (exigido pelo enunciado)
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(FICHEIRO_LOG, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────────────────


def construir_url_imagem(transforms: dict) -> str | None:
    """
    A Worten usa dois formatos no mesmo JSON:
      Hash moderno:  "0e9de1b47cf519a7670fcaef7463216dcd1929ca"
      ID legado:     "1216946225_default"
    Ambos usam o mesmo padrão base: https://www.worten.pt/i/{valor}
    """
    valor = transforms.get("default", "")
    return f"{WORTEN_IMG_BASE}{valor}" if valor else None


def carregar_dataset_existente(caminho: str) -> dict:
    """Lê JSON existente e indexa por url_produto para deduplicação entre corridas."""
    if not Path(caminho).exists():
        return {}
    try:
        with open(caminho, encoding="utf-8") as f:
            dados = json.load(f)
        return {p["url_produto"]: p for p in dados if "url_produto" in p}
    except Exception as e:
        logging.warning(f"Não foi possível ler dataset existente: {e}")
        return {}


def guardar_dataset(caminho: str, dados: dict) -> None:
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(list(dados.values()), f, ensure_ascii=False, indent=4)
    logging.info(f"Dataset guardado: {caminho} ({len(dados)} produtos)")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1 — Extracção completa via API (sem browser)
# ─────────────────────────────────────────────────────────────────────────────


def extrair_produtos_da_pagina(dados: dict) -> tuple[list[dict], bool]:
    """
    Extrai nome, preço, rating, imagem e URL do JSON de uma página da API.

    A resposta da API vem embrulhada em "detailsResponse":
      { "detailsResponse": { "productsData": {...}, "offersData": {...}, ... } }

    O JSON contém três sub-estruturas que precisam de join:
      productsData            → dados principais (nome, marca, rating, imagem)
      offersData              → preços  (join: produto.woffer.offer_id)
      productsCanonicalsData  → URLs    (join: produto.meta.refs.webitem_id)
    """
    # Desembrulhar a chave raiz "detailsResponse" se presente
    detalhe = dados.get("detailsResponse", dados)

    # Índice de preços: offer_id → {final, original}
    indice_ofertas = {
        o["offer_id"]: o.get("price", {})
        for o in detalhe.get("offersData", {}).get("offers", [])
    }

    # Índice de URLs: webitem_id → "/produtos/..."
    indice_urls = {
        w["id"]: w["url"]
        for w in detalhe.get("productsCanonicalsData", {}).get("web_items", [])
    }

    produtos_extraidos = []

    for produto in detalhe.get("productsData", {}).get("products", []):
        try:
            refs = produto.get("meta", {}).get("refs", {})
            webitem_id = refs.get("webitem_id", "")
            offer_id = produto.get("woffer", {}).get("offer_id", "")
            texto = produto.get("properties", {}).get("text", {})
            ratings = produto.get("ratings", {})
            transforms = produto.get("image", {}).get("transforms", {})

            # Preço — join com offersData
            preco_dados = indice_ofertas.get(offer_id, {})
            preco_final = preco_dados.get("final")
            preco_orig = preco_dados.get("original")

            # URL — join com productsCanonicalsData
            url_path = indice_urls.get(webitem_id, "")
            url_produto = f"https://www.worten.pt{url_path}" if url_path else None

            produtos_extraidos.append(
                {
                    "nome_produto": texto.get("name", "N/A"),
                    "marca": texto.get("brand_name", "N/A"),
                    "preco_atual": float(preco_final)
                    if preco_final is not None
                    else None,
                    "preco_original": float(preco_orig) if preco_orig else None,
                    "rating_valor": ratings.get("val"),  # float, ex: 4.48
                    "rating_contagem": ratings.get("cnt", 0),  # int,   ex: 31
                    "comentarios": [],  # preenchido na Fase 2
                    "url_produto": url_produto,
                    "url_imagem": construir_url_imagem(transforms),
                    "sku": refs.get("sku", "N/A"),
                    "data_extracao": datetime.now().isoformat(),
                }
            )

        except Exception as e:
            logging.debug(f"Erro ao processar produto {produto.get('id', '?')}: {e}")

    # hasNextPage está dentro de detailsResponse.searchResponse
    tem_proxima = detalhe.get("searchResponse", {}).get("hasNextPage", False)

    return produtos_extraidos, tem_proxima


async def fase1_api_completa(session: AsyncSession) -> list[dict]:
    """
    Varre todas as páginas via API.
    Devolve lista completa com nome/preço/rating/imagem/URL já preenchidos.
    """
    url_api = "https://www.worten.pt/worten-api/search-products"
    todos = []
    pagina = 1

    logging.info("─── Fase 1: Extracção via API interna (sem browser) ───")

    # Bootstrap de sessão para obter cookies e reduzir bloqueios WAF
    try:
        await session.get(
            "https://www.worten.pt/informatica-e-acessorios/componentes",
            impersonate="chrome120",
        )
        await asyncio.sleep(random.uniform(1.0, 2.0))
    except Exception as e:
        logging.warning(f"Bootstrap falhou (continuando): {e}")

    while True:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.worten.pt",
            "Referer": f"https://www.worten.pt/informatica-e-acessorios/componentes?page={pagina}",
        }
        payload = {
            "contexts": [WORTEN_CONTEXT_ID],
            "params": {
                "pageNumber": pagina,
                "pageSize": 48,
                "filters": [],
                "sort": {"field": "rank1", "order": "ASC"},
                "collapse": True,
            },
            "hasVariants": True,
        }

        try:
            resp = await session.post(
                url_api, headers=headers, json=payload, impersonate="chrome120"
            )

            if resp.status_code == 403:
                logging.error(
                    f"WAF bloqueou na página {pagina}. Parcial: {len(todos)} produtos."
                )
                break
            if resp.status_code != 200:
                logging.warning(
                    f"Status inesperado {resp.status_code} na página {pagina}. A parar."
                )
                break

            dados = resp.json()
            novos, tem_proxima = extrair_produtos_da_pagina(dados)

            if not novos:
                logging.info("Resposta vazia — fim da paginação.")
                break

            todos.extend(novos)
            logging.info(
                f"Página {pagina}: +{len(novos)} produtos | Total: {len(todos)}"
            )

            if not tem_proxima:
                break

            pagina += 1
            await asyncio.sleep(random.uniform(1.2, 2.5))

        except Exception as e:
            logging.critical(f"Erro fatal na Fase 1, página {pagina}: {e}")
            break

    logging.info(f"Fase 1 concluída: {len(todos)} produtos extraídos.")
    return todos


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2 — Comentários via Playwright (selectivo)
# ─────────────────────────────────────────────────────────────────────────────


async def extrair_comentarios(
    browser, semaforo: asyncio.Semaphore, produto: dict
) -> dict:
    """
    Visita a página do produto APENAS para os textos dos comentários.
    Todos os outros campos já estão preenchidos da Fase 1.
    """
    async with semaforo:
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            await page.goto(
                produto["url_produto"], wait_until="domcontentloaded", timeout=25000
            )

            # Scroll para activar o módulo Bazaarvoice (lazy-loaded)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2.0)

            container = page.locator(".bv-content-list")
            if await container.is_visible(timeout=4000):
                textos = await page.locator(
                    ".bv-content-summary-body-text"
                ).all_inner_texts()
                produto["comentarios"] = [t.strip() for t in textos if t.strip()]

        except Exception as e:
            logging.debug(f"Erro ao obter comentários de {produto['url_produto']}: {e}")
        finally:
            await context.close()

        return produto


async def fase2_comentarios(produtos: list[dict]) -> list[dict]:
    """
    Usa Playwright apenas para produtos confirmados com reviews (rating_contagem > 0).
    Produtos sem reviews ficam com comentarios=[] e não são visitados — poupa ~80% das chamadas.
    """
    com_reviews = [
        p for p in produtos if p.get("rating_contagem", 0) > 0 and p.get("url_produto")
    ]
    sem_reviews = [p for p in produtos if p not in com_reviews]

    logging.info(
        f"─── Fase 2: {len(com_reviews)} produtos com reviews | "
        f"{len(sem_reviews)} sem reviews (ignorados) ───"
    )

    if not com_reviews:
        logging.info("Nenhum produto com reviews. Fase 2 ignorada.")
        return produtos

    semaforo = asyncio.Semaphore(CONCORRENCIA_PLAYWRIGHT)
    concluidos = 0
    total = len(com_reviews)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        tarefas = [extrair_comentarios(browser, semaforo, prod) for prod in com_reviews]

        for tarefa in asyncio.as_completed(tarefas):
            await tarefa
            concluidos += 1
            if concluidos % 10 == 0 or concluidos == total:
                logging.info(f"Comentários: {concluidos}/{total} páginas processadas.")

        await browser.close()

    logging.info("Fase 2 concluída.")
    return com_reviews + sem_reviews


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────


async def executar_pipeline() -> None:
    logging.info("═══════════════════════════════════════════════════")
    logging.info(f"  NOVA EXECUÇÃO — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("═══════════════════════════════════════════════════")

    dataset_indexado = carregar_dataset_existente(FICHEIRO_SAIDA)
    logging.info(f"Produtos já no dataset: {len(dataset_indexado)}")

    # Fase 1 — API pura (nome, preço, rating, imagem, URL)
    async with AsyncSession(impersonate="chrome120") as session:
        produtos = await fase1_api_completa(session)

    if not produtos:
        logging.error("Nenhum produto extraído. Execução abortada.")
        return

    # ── Para testes rápidos, descomenta a linha abaixo ──
    # produtos = produtos[:10]

    # Fase 2 — Comentários via Playwright (selectivo)
    produtos = await fase2_comentarios(produtos)

    # Merge com dataset existente — deduplicação por URL
    novos = sum(
        1
        for p in produtos
        if p.get("url_produto") and p["url_produto"] not in dataset_indexado
    )
    for p in produtos:
        if p.get("url_produto"):
            dataset_indexado[p["url_produto"]] = p

    logging.info(f"Novos produtos adicionados: {novos}")
    guardar_dataset(FICHEIRO_SAIDA, dataset_indexado)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER — corre a cada hora (Tarefa 1 do enunciado)
# ─────────────────────────────────────────────────────────────────────────────


def main():
    logging.getLogger("curl_cffi").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)

    logging.info(f"Scheduler iniciado. Execução a cada {HORAS_ENTRE_EXECUCOES}h.")

    while True:
        try:
            asyncio.run(executar_pipeline())
        except Exception as e:
            logging.critical(f"Erro inesperado no pipeline: {e}")

        logging.info(f"Próxima execução em {HORAS_ENTRE_EXECUCOES}h. A aguardar...")
        time.sleep(HORAS_ENTRE_EXECUCOES * 3600)


if __name__ == "__main__":
    main()
