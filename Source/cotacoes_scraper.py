import requests
from bs4 import BeautifulSoup
import pandas as pd

def coletar_cotacoes(data='2022-12-31', base='USD'):
    url = f"https://www.x-rates.com/historical/?from={base}&amount=1&date={data}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    resposta = requests.get(url, headers=headers)
    if resposta.status_code != 200:
        raise Exception(f"Erro ao acessar página: {resposta.status_code}")

    soup = BeautifulSoup(resposta.content, 'html.parser')

    tabela = soup.select_one(
        "#content > div:nth-child(1) > div > div.col2.pull-right.module.bottomMargin > div.moduleContent > table.tablesorter.ratesTable"
    )

    if tabela is None:
        raise Exception("Tabela de câmbio não encontrada no HTML.")

    dados = []
    linhas = tabela.find_all("tr")[1:]  # Ignora cabeçalho
    for linha in linhas:
        colunas = linha.find_all("td")
        if len(colunas) >= 2:
            moeda = colunas[0].get_text(strip=True)
            taxa = colunas[1].get_text(strip=True)
            dados.append({
                "Data": data,
                "Base": base,
                "Moeda": moeda,
                "Taxa": float(taxa)
            })

    return pd.DataFrame(dados)

# Leitura do arquivo Excel direto do GitHub
url_excel = "https://raw.githubusercontent.com/Paulo972021/SpaceY/main/Source/CASE%20-%20Base%20de%20Dados.xlsx"

df_datas = pd.read_excel(url_excel)

# Garante que a coluna de datas está no formato correto
df_datas['Data'] = pd.to_datetime(df_datas['Data']).dt.strftime('%Y-%m-%d')

# Raspagem para todas as datas únicas da coluna
cotacoes = pd.concat(
    [coletar_cotacoes(data) for data in df_datas['Data'].unique()],
    ignore_index=True
)


