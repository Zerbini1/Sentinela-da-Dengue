import pandas as pd
import matplotlib.pyplot as plt

dados = pd.read_parquet("data/bronze/dengue_estado")

# Correção: nome da coluna 'data_iniSE' e uso de unit='ms'
dados['data_iniSE'] = pd.to_datetime(dados['data_iniSE'], unit='ms')

print(f"Total de registros: {len(dados)}")
# print(dados.head())

# Agrupa por data e soma os casos de todos os municípios naquela semana
casos_por_semana = dados.groupby('data_iniSE')['casos_est'].sum().reset_index()

# Ordena para ver do mais antigo para o mais recente
casos_por_semana = casos_por_semana.sort_values('data_iniSE')

# print(casos_por_semana.head())


# Gerando o gráfico de linha
# x = data_iniSE (Tempo)
# y = casos_est (Quantidade)
casos_por_semana.plot(x='data_iniSE', y='casos_est', kind='line', figsize=(12, 6))

# Melhorando a visualização
plt.title('Evolução Temporal dos Casos de Dengue (ES)')
plt.xlabel('Data')
plt.ylabel('Casos Estimados')
plt.grid(True, linestyle='--', alpha=0.5) # Adiciona linhas de grade suaves

# Salvando o gráfico
plt.savefig('grafico_dengue_linha.png')
print("Gráfico salvo como 'grafico_dengue_linha.png'")