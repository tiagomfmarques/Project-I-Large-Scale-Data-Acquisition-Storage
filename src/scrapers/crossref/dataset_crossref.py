import json
import os
import re
import webbrowser

import pandas as pd

json_path = "../../../data/crossref/dataset_crossref_ai.json"
if not os.path.exists(json_path):
    print(f"Erro: O ficheiro {json_path} não foi encontrado.")
    exit()

print("A processar base de dados (Novo Schema JSON)...")

with open(json_path, "r", encoding="utf-8") as f:
    dados_brutos = json.load(f)

df = pd.DataFrame(dados_brutos.get("articles", []))
df_view = df.copy()

################################################################################
#                                                                              #
#                   Classificação e Frequência de Temas                        #
#                                                                              #
################################################################################
regras_temas = {
    "RAG": r"\bretrieval.?augmented\b|\brag\b",
    "Agents": r"\bautonomous agent|\bagent.?based\b|\bmulti.?agent\b",
    "Information Retrieval": r"\binformation retrieval\b|\bir\b",
    "Machine Learning": r"\bmachine learning\b|\bdeep learning\b|\bneural network",
    "Artificial Intelligence": r"\bartificial intelligence\b|\bai\b",
}


def classificar_temas(texto):
    if pd.isna(texto):
        return "Desconhecido"
    texto_lower = str(texto).lower()
    temas = [
        tema for tema, padrao in regras_temas.items() if re.search(padrao, texto_lower)
    ]
    return ", ".join(temas) if temas else "Multidisciplinar"


df_view["tema_bruto"] = (
    df_view["title"].fillna("") + " " + df_view["abstract"].fillna("")
).apply(classificar_temas)
df["theme"] = df_view["tema_bruto"]

contagem_temas = df_view["tema_bruto"].value_counts().to_dict()
df_view["frequencia_tema"] = df_view["tema_bruto"].map(contagem_temas)


#################################################################################
#                                                                               #
#               Formatação HTML (Mantemos o Teu Layout Intacto)                 #
#                                                                               #
#################################################################################
def formatar_titulo_link(url, titulo):
    if pd.isna(url) or not url:
        return titulo
    return f'<a target="_blank" href="{url}" style="font-weight:600; color:#0969da; text-decoration:none;">{titulo}</a>'


def formatar_abstract(texto):
    if pd.isna(texto) or str(texto).strip() == "":
        return '<span style="background-color:#ffebe9; color:#cf222e; padding:3px 8px; border-radius:12px; font-weight:600; font-size:11px;">Sem Abstract</span>'
    badge_ok = '<span style="background-color:#dafbe1; color:#1a7f37; padding:3px 8px; border-radius:12px; font-weight:600; font-size:11px; display:inline-block; margin-bottom:6px;">Abstract Disponível</span><br>'
    if len(texto) > 300:
        return f"{badge_ok}<span style='color:#57606a; line-height:1.5;'>{texto[:300]}...</span>"
    return f"{badge_ok}<span style='color:#57606a; line-height:1.5;'>{texto}</span>"


def destacar_impacto(n):
    if pd.isna(n) or n == 0:
        return '<span style="color:#8c959f;">0</span>'
    if int(n) > 50:
        return f'<span style="color:#cf222e; font-weight:bold; font-size:14px;">🔥 {int(n)}</span>'
    return f'<span style="font-weight:600;">{int(n)}</span>'


def formatar_tags_tema(temas_str):
    if temas_str == "Multidisciplinar":
        return '<span style="background-color:#f6f8fa; color:#57606a; border:1px solid #d0d7de; padding:2px 8px; border-radius:12px; font-size:11px; display:inline-block;">Multidisciplinar</span>'
    cores = {
        "Artificial Intelligence": ("#f3f4f6", "#4b5563", "#d1d5db"),
        "Machine Learning": ("#fef3c7", "#b45309", "#fde68a"),
        "RAG": ("#e0e7ff", "#4338ca", "#c7d2fe"),
        "Agents": ("#dbeafe", "#1d4ed8", "#bfdbfe"),
        "Information Retrieval": ("#fce7f3", "#be185d", "#fbcfe8"),
    }
    tags_html = []
    for t in temas_str.split(", "):
        bg, text, border = cores.get(t, ("#f6f8fa", "#24292f", "#d0d7de"))
        tags_html.append(
            f'<span style="background-color:{bg}; color:{text}; border:1px solid {border}; padding:3px 8px; border-radius:12px; font-size:11px; font-weight:600; display:inline-block; margin:2px;">{t}</span>'
        )
    return (
        "<div style='display:flex; flex-wrap:wrap; gap:4px;'>"
        + "".join(tags_html)
        + "</div>"
    )


# Processar as colunas para visualização
resumo = df_view["tema_bruto"].str.split(", ").explode().value_counts()
resumo_html = "".join(
    f"<li><strong>{tema}:</strong> {contagem} artigos</li>"
    for tema, contagem in resumo.items()
)

df_view["Título"] = df_view.apply(
    lambda x: formatar_titulo_link(x["url"], x["title"]), axis=1
)
df_view["Abstract"] = df_view["abstract"].apply(formatar_abstract)
df_view["Tema"] = df_view["tema_bruto"].apply(formatar_tags_tema)
df_view["Citações"] = df_view["cites"].apply(destacar_impacto)
df_view["Ano de Publicação"] = df_view["year_publicacion"]
# A coluna authors agora é uma lista []. Convertemos para string apenas no visualizador:
df_view["Autores"] = df_view["authors"].apply(
    lambda lst: ", ".join(lst) if isinstance(lst, list) and lst else "N/A"
)

# Filtrar: apenas registos com autores E abstract
df_view = df_view[
    (df_view["authors"].apply(lambda lst: isinstance(lst, list) and len(lst) > 0))
    & (df_view["abstract"].notna())
    & (df_view["abstract"].apply(lambda x: str(x).strip() != ""))
]

# Ordenar: por citações (Maior -> Menor), depois por ano de publicação (Mais recente -> mais antigo)
df_view = df_view.sort_values(
    by=["cites", "year_publicacion"], ascending=[False, False], na_position="last"
)

colunas_finais = [
    "Título",
    "Tema",
    "Autores",
    "Abstract",
    "Citações",
    "Ano de Publicação",
]
tabela_html = df_view[colunas_finais].to_html(escape=False, index=False)

# ─────────────────────────────────────────────────────────────────────────────
# Geração de HTML e Exportação (Schema Respeitado)
# ─────────────────────────────────────────────────────────────────────────────
html_template = f"""
<!DOCTYPE html>
<html lang="pt-PT">
<head>
    <meta charset="UTF-8">
    <title>Dataset Científico: Inteligência Artificial</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; background-color: #f6f8fa; margin: 0; padding: 20px; color: #24292f; }}
        .container {{ width: 98%; margin: 0 auto; }}
        .header {{ margin-bottom: 24px; background: white; padding: 20px; border: 1px solid #d0d7de; border-radius: 6px; }}
        h1 {{ margin: 0 0 10px 0; font-size: 22px; }}
        .summary {{ font-size: 13px; color: #57606a; columns: 2; -webkit-columns: 2; -moz-columns: 2; }}
        table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #d0d7de; border-radius: 6px; table-layout: fixed; }}
        th {{ background-color: #f6f8fa; border-bottom: 1px solid #d0d7de; padding: 12px 16px; text-align: left; font-size: 13px; font-weight: 600; color: #57606a; }}
        td {{ padding: 16px; border-bottom: 1px solid #d0d7de; vertical-align: top; font-size: 14px; word-wrap: break-word; line-height: 1.4; }}
        tr:hover {{ background-color: #f3f4f6; }}
        th:nth-child(1) {{ width: 20%; }} th:nth-child(2) {{ width: 11%; }} th:nth-child(3) {{ width: 15%; }} th:nth-child(4) {{ width: 40%; }} th:nth-child(5) {{ width: 7%; }} th:nth-child(6) {{ width: 7%; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Catálogo de Investigação: {len(df)} artigos</h1>
            <p><strong>Distribuição dos Dados Recolhidos:</strong></p>
            <ul class="summary">{resumo_html}</ul>
        </div>
        {tabela_html}
    </div>
</body>
</html>
"""

ficheiro_html = "../../data/crossref/dataset_crossref_visualizacao.html"
with open(ficheiro_html, "w", encoding="utf-8") as f:
    f.write(html_template)


# Exportar o dataset filtrado e classificado para CSV e JSON
df_filtrado = df[
    (df["authors"].apply(lambda lst: isinstance(lst, list) and len(lst) > 0))
    & (df["abstract"].notna())
    & (df["abstract"].apply(lambda x: str(x).strip() != ""))
]

# Aplicar a mesma ordenação
df_filtrado = df_filtrado.sort_values(
    by=["cites", "year_publicacion"], ascending=[False, False], na_position="last"
)

df_filtrado.to_csv(
    "src/data/crossref/dataset_crossref_ai_.csv", index=False, encoding="utf-8"
)

formato_json_final = {"articles": df_filtrado.to_dict(orient="records")}
with open("src/data/crossref/dataset_crossref_ai.json", "w", encoding="utf-8") as f:
    json.dump(formato_json_final, f, ensure_ascii=False, indent=4)

print("✓ Sucesso. Ficheiro estruturado exportado.")
webbrowser.open("file://" + os.path.abspath(ficheiro_html))
