import wbdata
import pandas as pd
import time

# Anos de interesse
anos = [2022, 2023]

# Indicadores ativos
indicadores = {
    'NY.GDP.PCAP.CD': 'PIB per capita (USD)',
    'FP.CPI.TOTL.ZG': 'Inflação Média (%)',
    'NY.GDP.MKTP.KD.ZG': 'Crescimento do PIB (%)',
    'SL.UEM.TOTL.ZS': 'Taxa de Desemprego (%)',
    'IT.NET.BBND.P2': 'Banda Larga Fixa por 100 hab'
}

# Armazenar resultados por ano
dados_anos = {}

for ano in anos:
    dfs = []
    print(f"\n📅 Coletando dados para {ano}")
    for codigo, nome in indicadores.items():
        try:
            print(f"🔎 Buscando: {nome}")
            df = wbdata.get_dataframe({codigo: nome})
            df = df.reset_index()

            # Corrigir tipo da coluna de data
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Filtrar pelo ano desejado
            df = df[df['date'].dt.year == ano]

            # Manter país + valor
            df = df[['country', nome]].dropna()

            dfs.append(df)
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Erro ao buscar {nome}: {e}")

    # Combinar os indicadores desse ano
    if dfs:
        df_final_ano = dfs[0]
        for df_next in dfs[1:]:
            df_final_ano = pd.merge(df_final_ano, df_next, on='country', how='outer')
        df_final_ano['Ano'] = ano
        dados_anos[ano] = df_final_ano

# Concatenar todos os anos
df_final = pd.concat(dados_anos.values(), ignore_index=True)
