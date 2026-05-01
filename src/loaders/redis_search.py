import redis
import json

# Configurar cliente Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


# Função para pesquisar dados no Redis de forma estruturada
def pesquisar_estruturado(termo):
    print(f"A pesquisar resultados para: '{termo}'")

    resultados_finais = []

    ordem_artigos = ["id", "id_api", "url", "website", "theme", "title", "authors", "year_publicacion", "cites",
                     "timestamp"]
    ordem_datasets = ["id", "id_webscarping", "url", "subject_Area", "title", "dataset_information", "creatores",
                      "year_publicacion", "keywords", "dataset_characteristics", "associated_tasks",
                      "feature_type", "n_instances", "n_features", "cites", "size", "timestamp"]

    for chave in r.scan_iter("*"):
        if len(resultados_finais) >= 5:
            break

        dados = r.hgetall(chave)

        titulo = dados.get("title", "").lower()
        tema = dados.get("theme", "").lower()
        area = dados.get("subject_Area", "").lower()

        if termo.lower() in titulo or termo.lower() in tema or termo.lower() in area:

            e_artigo = chave.startswith("article")
            ordem_campos = ordem_artigos if e_artigo else ordem_datasets

            item_ordenado = {}

            # 1. Mantém exatamente a tua estrutura e ordem
            for campo in ordem_campos:
                if campo in dados:
                    item_ordenado[campo] = dados[campo]

            # 2. ADIÇÃO: Vai buscar TUDO o resto que não estava na lista (ex: abstract)
            for campo_extra in dados:
                if campo_extra not in item_ordenado:
                    item_ordenado[campo_extra] = dados[campo_extra]

            resultados_finais.append(item_ordenado)

    return resultados_finais


if __name__ == "__main__":
    query = input("Pesquisa: ")

    try:
        saida_json = pesquisar_estruturado(query)

        if saida_json:
            print("\nResultados Encontrados:")

            print(json.dumps(saida_json, indent=4, ensure_ascii=False))
        else:
            print("\nNenhum resultado encontrado.")

    except Exception as e:
        print(f"Erro: {e}")