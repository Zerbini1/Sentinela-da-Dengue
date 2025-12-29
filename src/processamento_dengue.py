import pandas as pd

# Lendo os dados diários que já baixamos (Bronze)
df_dengue = pd.read_parquet("data/bronze/dengue_estado")

# Garantindo que é data
df_dengue['data_iniSE'] = pd.to_datetime(df_dengue['data_iniSE'], unit='ms')

#Selecionando apenas as colunas necessárias
df_dengue = df_dengue[['data_iniSE', 'municipio_id', 'casos_est', 'casos']]

df_municipios = df_dengue.copy()

df_estado = df_dengue.groupby('data_iniSE').agg({
    'casos_est': 'sum',
    'casos': 'sum'
}).reset_index()

# Ordenando os dados por data
df_municipios = df_municipios.sort_values(['municipio_id', 'data_iniSE'])
df_estado = df_estado.sort_values('data_iniSE')

# Salvando as versões finais (Silver)
df_municipios.to_parquet("data/silver/dengue_municipios.parquet", index=False)
df_estado.to_parquet("data/silver/dengue_estado.parquet", index=False)

print("Dados gerados e salvos na pasta 'data/silver'!")