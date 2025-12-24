import pandas as pd

print("Iniciando transformação...")

# 1. Carregar os dados
df_clima = pd.read_parquet('data/bronze/clima_output.parquet')
df_dengue = pd.read_parquet('data/bronze/dengue_output.parquet')

# 2. Tratamento do Clima
df_clima['date'] = pd.to_datetime(df_clima['date'])
df_clima.set_index('date', inplace=True)

# Resample semanal (Média de temp e Soma de chuva)
df_clima_semanal = df_clima.resample('W-SUN').agg({
    'temperature_2m_mean': 'mean',
    'precipitation_sum': 'sum'
})

# 3. Tratamento da Dengue
df_dengue_silver = df_dengue[['data_iniSE', 'casos', 'SE']].copy()

df_dengue_silver['data_iniSE'] = pd.to_datetime(df_dengue_silver['data_iniSE'], unit='ms').dt.tz_localize('UTC')

# Ordenar é obrigatório para o merge_asof funcionar
df_dengue_silver = df_dengue_silver.sort_values('data_iniSE')
df_clima_semanal = df_clima_semanal.sort_index()

# 4. Join entre Dengue e Clima
df_final = pd.merge_asof(
    df_dengue_silver, 
    df_clima_semanal, 
    left_on='data_iniSE', 
    right_index=True,
    direction='nearest',
    tolerance=pd.Timedelta(days=7)
)

# 5. Salvar na Camada Gold (Pronto para análise)
df_final.to_parquet('data/gold/final_output.parquet')

print("\n=== AMOSTRA DOS DADOS UNIFICADOS ===")
print(df_final.head())
print(f"\nArquivo salvo com sucesso: {df_final.shape[0]} linhas.")