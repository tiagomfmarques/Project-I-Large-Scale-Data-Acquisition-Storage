import json
import redis
import hashlib
import os

# Configurar cliente Redis
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
except Exception as e:
    print(f"Erro ao configurar cliente Redis: {e}")

# Função para indexar dados do JSON no Redis
def indexar_dados(caminho_json):
    if not os.path.exists(caminho_json):
        print(f"Ficheiro não encontrado: {caminho_json}")
        return

    with open(caminho_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for categoria in ["articles", "datasets"]:
        if categoria in data:
            print(f"Processar {len(data[categoria])} {categoria}")
            for item in data[categoria]:

                url = item.get("url", "")

                item_id = hashlib.sha256(url.encode('utf-8')).hexdigest()

                documento = {}
                for k, v in item.items():
                    if isinstance(v, list):
                        documento[k] = ", ".join(map(str, v))
                    else:
                        documento[k] = v

                documento["id"] = item_id

                chave_redis = f"{categoria}:{item_id}"
                r.hset(chave_redis, mapping=documento)

    print("Indexação concluída")

if __name__ == "__main__":
    indexar_dados("../../data/raw/scientific_research_data.json")