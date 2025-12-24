# Sentinela da Dengue

![Status](https://img.shields.io/badge/Status-Em_Desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)

## Sobre o Projeto

O **Sentinela da Dengue** é um projeto de Engenharia de Dados que visa criar um pipeline para analisar a correlação entre dados climáticos e focos de dengue em cidades brasileiras.

O objetivo é extrair dados de fontes públicas, tratá-los e armazená-los em uma arquitetura de Lakehouse (Medallion Architecture) para análises futuras.

## Arquitetura

O projeto segue a arquitetura Medalhão:
- **Bronze:** Dados brutos extraídos das APIs e salvos em formato Parquet (Histórico).
- **Silver:** Dados limpos, deduplicados e com tipagem correta (Em breve).
- **Gold:** Dados agregados prontos para BI e Analytics (Em breve).

## Tecnologias Utilizadas

- **Linguagem:** Python
- **Bibliotecas:** Pandas, Requests, PyArrow
- **Formatos:** Parquet
- **APIs:** Open-Meteo (Clima), InfoDengue (Epidemiologia)

## Contato
Desenvolvido por Felipe Zerbini | [LinkedIn](https://www.linkedin.com/in/felipe-zerbini/)
