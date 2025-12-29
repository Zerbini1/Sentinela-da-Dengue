import pandas as pd

# 1. Lendo os dados diários que já baixamos (Bronze)
df_diario = pd.read_parquet("data/bronze/clima_estado")

# Garantindo que é data
df_diario['date'] = pd.to_datetime(df_diario['date'])

# 2. Transformando em Semanal por Município (Silver)
# Agrupamos por Município E Data (semana)
df_semanal_municipios = df_diario.set_index('date').groupby('municipio_id').resample('W-SUN').agg({
    'temperature_2m_mean': 'mean',  # Média da semana
    'precipitation_sum': 'sum'      # Total de chuva da semana
}).reset_index()

# 3. Criando o Dataset do Estado (Média de todas as cidades)
df_semanal_estado = df_semanal_municipios.groupby('date').agg({
    'temperature_2m_mean': 'mean',
    'precipitation_sum': 'mean'
}).reset_index()

# Renomeando a coluna
df_semanal_estado = df_semanal_estado.rename(columns={'date': 'data_iniSE'})
df_semanal_municipios = df_semanal_municipios.rename(columns={'date': 'data_iniSE'})

# 4. Salvando as versões finais (Silver)
df_semanal_municipios.to_parquet("data/silver/clima_municipios.parquet", index=False)
df_semanal_estado.to_parquet("data/silver/clima_estado.parquet", index=False)

print("Dados semanais gerados e salvos na pasta 'data/silver'!")
print("\n--- Estado (Cabeçalho) ---")
print(df_semanal_estado.head())
# print(df_diario.columns)