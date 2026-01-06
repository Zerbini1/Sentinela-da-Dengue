import pandas as pd

clima_municipios = pd.read_parquet("data/silver/clima_municipios.parquet")
clima_estado = pd.read_parquet("data/silver/clima_estado.parquet")
dengue_municipios = pd.read_parquet("data/silver/dengue_municipios.parquet")
dengue_estado = pd.read_parquet("data/silver/dengue_estado.parquet")

df_gold_municipios = pd.merge(clima_municipios, dengue_municipios,
                                 on=['data_iniSE', 'municipio_id'],
                                 how='inner')

df_gold_estado = pd.merge(clima_estado, dengue_estado,
                              on=['data_iniSE'],
                              how='inner')

df_gold_municipios = df_gold_municipios.rename(columns={
    'temperature_2m_mean': 'temp_media_semana',
    'precipitation_sum': 'precipitacao_total_semana',
    'casos_est': 'casos_dengue_estimativa',
    'casos': 'casos_dengue_confirmados'
})

df_gold_estado = df_gold_estado.rename(columns={
    'temperature_2m_mean': 'temp_media_semana',
    'precipitation_sum': 'precipitacao_total_semana',
    'casos_est': 'casos_dengue_estimativa',
    'casos': 'casos_dengue_confirmados'
})

df_gold_municipios.to_parquet("data/gold/dengue_clima_municipios.parquet", index=False)
df_gold_estado.to_parquet("data/gold/dengue_clima_estado.parquet", index=False)

print(f"Arquivo salvo: data/gold/dengue_clima_municipios.parquet ({len(df_gold_municipios)} registros)")
print(f"Arquivo salvo: data/gold/dengue_clima_estado.parquet ({len(df_gold_estado)} registros)")
print("\nAmostra (Estado):")
print(df_gold_estado.head())