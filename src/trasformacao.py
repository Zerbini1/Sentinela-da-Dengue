import pandas as pd

print("Iniciando transformação...")

# 1. Carregar os dados
df_clima = pd.read_parquet('daily_data.parquet')
df_dengue = pd.read_parquet('dengue_data.parquet')

# 2. Tratamento do Clima (Bronze -> Silver)
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

# 4. O Grande Encontro (Join)
# merge_asof procura a data mais próxima (tolerância de 7 dias)
df_final = pd.merge_asof(
    df_dengue_silver, 
    df_clima_semanal, 
    left_on='data_iniSE', 
    right_index=True,
    direction='nearest',
    tolerance=pd.Timedelta(days=7)
)

# 5. Salvar na Camada Gold (Pronto para análise)
df_final.to_parquet('dengue_clima_final.parquet')

print("\n=== AMOSTRA DOS DADOS UNIFICADOS ===")
print(df_final.head())
print(f"\nArquivo salvo com sucesso: {df_final.shape[0]} linhas.")