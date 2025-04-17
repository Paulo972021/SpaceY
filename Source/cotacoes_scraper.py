import requests
import pandas as pd
from tqdm import tqdm

def gerar_datas_mensais():
    return pd.date_range("2022-01-01", "2023-12-31", freq="MS").strftime('%Y-%m-%d').tolist()

def coletar_taxas_currencylayer(access_key):
    datas = gerar_datas_mensais()
    resultado = []

    for data in tqdm(datas, desc="Consultando meses"):
        url = f"https://api.exchangerate.host/historical"
        params = {
            "access_key": access_key,
            "date": data
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                json_data = resp.json()
                base = json_data.get("source", "USD")
                quotes = json_data.get("quotes", {})
                for par, taxa in quotes.items():
                    moeda_destino = par.replace(base, "")
                    resultado.append({
                        "Data": data,
                        "Base": base,
                        "Moeda": moeda_destino,
                        "Taxa": taxa
                    })
            else:
                print(f"[⚠️ ERRO {resp.status_code}] {data}")
        except Exception as e:
            print(f"[❌ ERRO] {data}: {e}")

    return pd.DataFrame(resultado)
