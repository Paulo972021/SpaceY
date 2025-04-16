import pycountry
import pandas as pd


# Lista de países disponíveis em pycountry
lista_paises = list(pycountry.countries)

# Construir DataFrame com país, nome da moeda e código ISO da moeda
dados = []

for pais in lista_paises:
    nome_pais = pais.name
    alpha_2 = pais.alpha_2
    moeda = None
    codigo = None

    try:
        moedas = list(pycountry.currencies)
        for m in moedas:
            if hasattr(m, 'countries') and alpha_2 in m.countries:
                moeda = m.name
                codigo = m.alpha_3
                break
    except:
        pass

    dados.append({
        "País (en)": nome_pais,
        "Código ISO": alpha_2,
        "Moeda": moeda,
        "Código Moeda": codigo
    })

# Criar DataFrame
df_paises_moedas = pd.DataFrame(dados)



# Usar countryinfo para preencher moedas com base nos nomes dos países
from countryinfo import CountryInfo

# Copiar DataFrame original
df_moedas = df_paises_moedas.copy()

# Funções auxiliares para obter moeda e código
def obter_moeda(pais):
    try:
        info = CountryInfo(pais)
        moedas = info.info().get('currencies')
        return moedas[0] if moedas else None
    except:
        return None

# Aplicar a função apenas onde a moeda está faltando
df_moedas['Código Moeda'] = df_moedas.apply(
    lambda row: obter_moeda(row['País (en)']) if pd.isna(row['Código Moeda']) else row['Código Moeda'],
    axis=1
)

# Agora buscar o nome da moeda a partir do código ISO
def nome_moeda_por_codigo(codigo):
    try:
        moeda = pycountry.currencies.get(alpha_3=codigo)
        return moeda.name if moeda else None
    except:
        return None

df_moedas['Moeda'] = df_moedas['Código Moeda'].apply(nome_moeda_por_codigo)
moedas = df_moedas

