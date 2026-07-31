import pandas as pd
from pathlib import Path

df = pd.read_csv(Path("data/clean/qualite_air.csv"))
cities = ["Amsterdam", "Antananarivo", "Beijing", "London", "Paris"]


for city in cities:
    df_filtered = df[df['ville'] == city] 
    filename = f"{city.lower()}_qa.csv"
    df_filtered.to_csv(filename, index=False)