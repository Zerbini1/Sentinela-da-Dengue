import pandas as pd
import matplotlib.pyplot as plt
import os

# Configuração simples
plt.style.use('bmh') # Estilo limpo e profissional
plt.rcParams['figure.figsize'] = [10, 6]

os.makedirs("results/graficos", exist_ok=True)

# 1. Carregar dados
print("Carregando dados Gold...")
df = pd.read_parquet("data/gold/dengue_clima_estado.parquet")

# ---------------------------------------------------------
# GRÁFICO 1: Curva de Epidemia (Linha Simples)
# ---------------------------------------------------------
print("Gerando gráfico 1: Curva Epidêmica...")
plt.figure()
plt.plot(df['data_iniSE'], df['casos_dengue_confirmados'], color='#d62728', linewidth=2)
plt.title('Curva de Casos de Dengue - Espírito Santo (2020-2025)')
plt.ylabel('Número de Casos')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('results/graficos/1_curva_epidemia_simples.png')
plt.close()

# ---------------------------------------------------------
# GRÁFICO 2: Comparação Chuva vs Dengue (Eixo Duplo Combinado)
# ---------------------------------------------------------
print("Gerando gráfico 2: Chuva e Dengue Combinados...")
fig, ax1 = plt.subplots(figsize=(12, 6))

# Eixo Esquerdo (Barras de Chuva) - Azul Clarinho no Fundo
color_rain = '#1f77b4'
ax1.bar(df['data_iniSE'], df['precipitacao_total_semana'], color=color_rain, alpha=0.3, label='Chuva (mm)')
ax1.set_ylabel('Chuva (mm)', color=color_rain, fontweight='bold')
ax1.tick_params(axis='y', labelcolor=color_rain)
ax1.set_title('Correlação Visual: Chuva (Barras) vs Dengue (Linha) - Espírito Santo (2020-2025)')
ax1.grid(False) # Desliga grade para não poluir

# Eixo Direito (Linha de Dengue) - Vermelho Forte na Frente
ax2 = ax1.twinx()
color_dengue = '#d62728'
ax2.plot(df['data_iniSE'], df['casos_dengue_confirmados'], color=color_dengue, linewidth=2.5, label='Casos Dengue')
ax2.set_ylabel('Casos de Dengue', color=color_dengue, fontweight='bold')
ax2.tick_params(axis='y', labelcolor=color_dengue)
ax2.grid(True, linestyle='--', alpha=0.3)

# Legenda Combinada (Truque para juntar legendas de eixos diferentes)
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

plt.tight_layout()
plt.savefig('results/graficos/2_chuva_vs_dengue_combinado.png')
plt.close()

# ---------------------------------------------------------
# GRÁFICO 3: Dispersão Limpa (Temperatura x Casos)
# ---------------------------------------------------------
print("Gerando gráfico 3: Dispersão Temperatura...")
plt.figure()
plt.scatter(df['temp_media_semana'], df['casos_dengue_confirmados'], alpha=0.5, color='purple')
plt.title('Relação: Temperatura vs Casos')
plt.xlabel('Temperatura Média Semanal (°C)')
plt.ylabel('Casos de Dengue')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('results/graficos/3_dispersao_temperatura_limpa.png')
plt.close()

print("✅ Gráficos simplificados gerados em 'results/graficos'!")
