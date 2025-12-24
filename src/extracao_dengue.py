import pandas as pd
import requests

url = "https://info.dengue.mat.br/api/alertcity/?geocode=3205309&disease=dengue&format=json&ew_start=01&ey_start=2020&ew_end=49&ey_end=2025"

response = requests.get(url)
data = response.json()
dataframe = pd.DataFrame(data)
dataframe.to_parquet("dengue_data.parquet", index = False)
print(dataframe.head())