import pandas as pd
import matplotlib.pyplot as plt
import os

# Configuração de estilo
plt.style.use('ggplot') 

print("Iniciando geração de gráficos...")

# 0. Garantir que a pasta de resultados existe
os.makedirs('results', exist_ok=True)

# 1. Carregar os dados da camada GOLD
# Ajustei o caminho conforme sua estrutura atual
arquivo_entrada = 'data/gold/final_output.parquet'

if not os.path.exists(arquivo_entrada):
    print(f"ERRO: Arquivo não encontrado em {arquivo_entrada}")
    exit()

df = pd.read_parquet(arquivo_entrada)

# Garantir ordem temporal
df = df.sort_values('data_iniSE')

# === GRÁFICO 1: Evolução da Dengue (Linha) ===
plt.figure(figsize=(12, 6))
plt.plot(df['data_iniSE'], df['casos'], color='darkred', linewidth=2, label='Casos Notificados')
plt.title('Evolução dos Casos de Dengue (2020-2025) - Vitória/ES')
plt.xlabel('Data')
plt.ylabel('Número de Casos')
plt.legend()
plt.grid(True)
plt.tight_layout()

# Salvando
plt.savefig('results/01_evolucao_dengue.png')
print("✅ Gráfico 1 salvo em: results/01_evolucao_dengue.png")
plt.close()


# === GRÁFICO 2: Chuva vs Tempo (Barras) ===
plt.figure(figsize=(12, 6))
plt.bar(df['data_iniSE'], df['precipitation_sum'], color='blue', alpha=0.6, width=5, label='Chuva (mm)')
plt.title('Histórico de Precipitação Semanal')
plt.xlabel('Data')
plt.ylabel('Chuva (mm)')
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()

# Salvando
plt.savefig('results/02_volume_chuva.png')
print("✅ Gráfico 2 salvo em: results/02_volume_chuva.png")
plt.close()

print("\nProcesso concluído! Confira a pasta 'results'.")