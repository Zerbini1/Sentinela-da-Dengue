import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import os
import time


# URL de um dataset público confiável com lat/lon de todos os municípios
url_coords = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"

# Lendo o arquivo direto da internet
df_brasil = pd.read_csv(url_coords)

# Filtrando apenas para o Espírito Santo (Código UF 32)
# Selecionamos apenas o que importa: código IBGE, nome, latitude e longitude
df_es_coords = df_brasil[df_brasil['codigo_uf'] == 32][['codigo_ibge', 'nome', 'latitude', 'longitude']]



# Setup da API
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
url = "https://archive-api.open-meteo.com/v1/archive"

# 1. LISTA VAZIA PARA ACUMULAR OS RESULTADOS
lista_dfs_semanais = []

print("Iniciando coleta...")

for ciades in df_es_coords.itertuples():
    print(f"Coletando: {ciades.nome}")
    
    params = {
        "latitude": ciades.latitude,
        "longitude": ciades.longitude,
        "start_date": "2019-12-29",
        "end_date": "2025-12-31",
        "daily": ["temperature_2m_mean", "precipitation_sum"],
        "timezone": "America/Sao_Paulo",
    }
    
    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Processamento Diário (Igual ao seu)
        daily = response.Daily()
        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}
        daily_data["temperature_2m_mean"] = daily.Variables(0).ValuesAsNumpy()
        daily_data["precipitation_sum"] = daily.Variables(1).ValuesAsNumpy()
        
        daily_dataframe = pd.DataFrame(data = daily_data)
        
        # Salvando dados diários individuais por município
        os.makedirs(f"data/bronze/clima_estado/municipio_id={ciades.codigo_ibge}", exist_ok=True)
        daily_dataframe.to_parquet(f"data/bronze/clima_estado/municipio_id={ciades.codigo_ibge}/clima_output.parquet", index = False)
        
        # Transformação Semanal
        daily_dataframe['date'] = pd.to_datetime(daily_dataframe['date'])
        daily_dataframe.set_index('date', inplace=True)
        
        daily_dataframe_semanal = daily_dataframe.resample('W-SUN').agg({
            'temperature_2m_mean': 'mean',
            'precipitation_sum': 'sum'
        }).reset_index()
        
        # 2. IDENTIFICANDO A CIDADE
        daily_dataframe_semanal['municipio_id'] = ciades.codigo_ibge
        
        # 3. GUARDANDO NA LISTA
        lista_dfs_semanais.append(daily_dataframe_semanal)

    except Exception as e:
        print(f"Erro ao processar {ciades.nome}: {e}")
        
    time.sleep(5)    

# 4. CONCATENANDO TUDO NO FINAL
print("Unificando dados...")
clima_es_total = pd.concat(lista_dfs_semanais, ignore_index=True)

print("Finalizado!")
print(clima_es_total.head())
print(f"Total de registros: {len(clima_es_total)}")
