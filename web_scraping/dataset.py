import pandas as pd
from IPython.display import display

# ─────────────────────────────────────────────────────────────────────────────
# 1. Carregamento dos dados
# ─────────────────────────────────────────────────────────────────────────────

df = pd.read_json("dataset_worten_componentes_completo.json")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Inspecção básica (útil para o notebook do projecto)
# ─────────────────────────────────────────────────────────────────────────────

print(f"Total de produtos no dataset: {len(df)}")
print(f"Colunas disponíveis: {list(df.columns)}\n")

# Percorrer 5 registos conforme pedido na Tarefa 1, ponto 2 do enunciado
print("─── Amostra de 5 registos ───")
for _, row in df.head(5).iterrows():
    print(f"  Nome    : {row['nome_produto']}")
    print(f" Marca   : {row['marca']}")
    print(f"  Preço   : {row['preco_atual']} €  (original: {row['preco_original']} €)")
    print(
        f"  Rating  : {row['rating_valor']} estrelas com {row['rating_contagem']} avaliações"
    )
    print(f"  URL     : {row['url_produto']}")
    print(f"  Imagem  : {row['url_imagem']}")
    print(f" Data de extração:  {row['data_extracao']}")

    print()

# ─────────────────────────────────────────────────────────────────────────────
# 3. Funções de formatação visual para o display HTML
# ─────────────────────────────────────────────────────────────────────────────


def formatar_imagem(url_img):
    if pd.isna(url_img) or not url_img:
        return "Sem imagem"
    return f'<img src="{url_img}" style="max-height:120px; border-radius:4px;">'


def formatar_link(url):
    return f'<a target="_blank" href="{url}" style="font-weight:bold; color:#1a0dab;">Ver Produto</a>'


def formatar_preco(valor):
    if pd.isna(valor):
        return "N/D"
    return f"{valor:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Top 10 produtos mais baratos (ordenação numérica correcta — preco_atual é float)
# ─────────────────────────────────────────────────────────────────────────────

df_apresentacao = (
    df[["nome_produto", "preco_atual", "url_produto", "url_imagem"]]
    .dropna(subset=["preco_atual"])  # Remove linhas sem preço extraído
    .sort_values(by="preco_atual")  # Ordena numericamente (não como string)
    .head(10)
    .copy()
)

# ─────────────────────────────────────────────────────────────────────────────
# 5. Renderização da tabela rica em HTML
# ─────────────────────────────────────────────────────────────────────────────

tabela_rica = (
    df_apresentacao.style.format(
        {
            "preco_atual": formatar_preco,
            "url_imagem": formatar_imagem,
            "url_produto": formatar_link,
        }
    )
    .set_properties(**{"text-align": "left"})
    .set_table_styles(
        [
            {
                "selector": "th",
                "props": [("background-color", "#f2f2f2"), ("font-weight", "bold")],
            },
            {
                "selector": "td",
                "props": [("vertical-align", "middle"), ("padding", "8px")],
            },
        ]
    )
    .hide(axis="index")
)

display(tabela_rica)
