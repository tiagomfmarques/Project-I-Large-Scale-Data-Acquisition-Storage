from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "artigos_cientificos"

# Função para formatar e exibir os resultados de forma legível
def formatar_resultado(hit):

    s = hit['_source']
    print("\n" + "=" * 50 + "\n")
    print(f"Título:  {s.get('title', 'N/A')}")
    print(f"Tema:   {s.get('theme', 'N/A')}")
    print(f"Ano:     {s.get('year_publicacion', 'N/A')}")
    print(f"Autores: {', '.join(s.get('authors', []))}")
    print(f"Resumo:  {s.get('abstract') or "Indisponível"}")
    print(f"Website/URL:  {s.get('website', 'N/A')} ({s.get('url', 'N/A')})")
    print(f"Relevância (Score): {hit['_score']}")

# Função para realizar uma pesquisa geral com fuzziness em múltiplos campos
def pesquisa_geral(termo):

    query = {
        "query": {
            "multi_match": {
                "query": termo,
                "fields": ["title", "authors", "abstract", "website", "theme"],
                "fuzziness": "AUTO"
            }
        }
    }
    return es.search(index=INDEX_NAME, body=query, size=10)

# Função para realizar uma pesquisa específica em um campo, usando "term" para ano e "match" para os outros campos
def pesquisa_especifica(campo, valor):

    tipo_query = "term" if campo == "year_publicacion" else "match"

    query = {
        "query": {
            tipo_query: {campo: valor}
        }
    }
    return es.search(index=INDEX_NAME, body=query, size=10)

# Menu interativo para o usuário escolher o tipo de pesquisa e inserir os termos de busca
def menu():
    while True:
        print("\nSistema de Pesquisa de Artigos Científicos\n")
        print("1. Pesquisa Geral")
        print("2. Pesquisar por Título")
        print("3. Pesquisar por Ano")
        print("4. Pesquisar por Autor")
        print("5. Pesquisar por Website/URL")
        print("0. Sair")

        opcao = input("\nEscolha uma opção: ")

        if opcao == "0": break

        resultados = None

        if opcao == "1":
            busca = input("Pesquisa:")
            resultados = pesquisa_geral(busca)
        elif opcao == "2":
            busca = input("Introduza o Título: ")
            resultados = pesquisa_especifica("title", busca)
        elif opcao == "3":
            busca = input("Introduza o Ano: ")
            resultados = pesquisa_especifica("year_publicacion", busca)
        elif opcao == "4":
            busca = input("Introduza o Nome do autor: ")
            resultados = pesquisa_especifica("authors", busca)
        elif opcao == "5":
            busca = input("Introduza o Webiste: ")
            resultados = pesquisa_especifica("website", busca)
        else:
            print("Opção inválida.")
            continue

        if resultados:
            total = resultados['hits']['total']['value']
            print(f"\nEncontrados {total} resultados. Mostrando os 10 melhores:")
            for hit in resultados['hits']['hits']:
                formatar_resultado(hit)

# Ponto de entrada do programa, verificando a conexão com o Elasticsearch antes de iniciar o menu
if __name__ == "__main__":
    if es.ping():
        menu()
    else:
        print("Erro: O Docker/Elasticsearch não está ligado")