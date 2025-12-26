import requests
import pandas as pd
import os
import time

# Endpoint do IBGE para municípios do ES (UF 32)
url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/32/municipios"

response = requests.get(url)
dados = response.json()

cidades = []
for cidade in dados:
    cidades.append({'nome': cidade['nome'], 'id': cidade['id']})
    url = f"https://info.dengue.mat.br/api/alertcity?geocode={cidade['id']}&disease=dengue&format=json&ew_start=01&ey_start=2020&ew_end=49&ey_end=2025"
    response = requests.get(url)
    dados_dengue = response.json()
    
    if not dados_dengue:
        print(f"Não há dados de dengue para {cidade['nome']}. Pulando.")
        continue
    
    dataframe_dengue = pd.DataFrame(dados_dengue)
    os.makedirs(f"data/bronze/dengue_estado/municipio_id={cidade['id']}", exist_ok=True)
    dataframe_dengue.to_parquet(f"data/bronze/dengue_estado/municipio_id={cidade['id']}/dados.parquet", index=False)
    print(f"Dados de dengue de {cidade['nome']} salvos.")
    time.sleep(5)