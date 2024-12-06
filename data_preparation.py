# Importing all the necessary libraries
import pandas as pd
import os
import json
import re
from collections import Counter

import torch

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")
if device.type == 'cuda':
    print(f"GPU Name: {torch.cuda.get_device_name(0)}")



# Folder containing the JSON files
folder_path = "jobs"

# List to store data
data_list = []

# Iterate over each JSON file in the extracted folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.json'):  # Check if it's a JSON file
        file_path = os.path.join(folder_path, file_name)
        try:
            # Read the JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                df_temp = pd.DataFrame(data)
                data_list.append(df_temp)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")

# Combine all DataFrames into one named df
df = pd.concat(data_list, ignore_index=True)




# Drop rows where 'HTML_Text' is None
df = df.dropna(subset=['HTML_Text'])

# Calculate word counts for each row in the 'HTML_Text' column
df['word_count'] = df['HTML_Text'].apply(lambda x: len(x.split()))

# Filter out rows with less than 400 words in 'HTML_Text'
df = df[df['word_count'] > 400].copy()

# Clean city area names
def clean_trailing_indicators(area):
    if pd.isna(area):
        return None
    area = re.sub(r'\s+[a-zA-ZÆØÅæøå]+$', '', area.strip())
    return area

df['Area'] = df['Area'].apply(clean_trailing_indicators)

# Simplify area names to keep the first place
def keep_first_place_simple(area):
    if pd.isna(area):
        return None
    delimiters = [',', '/', ' og', ' eller', ' or', ' and']
    for delim in delimiters:
        if delim in area:
            area = area.split(delim)[0].strip()
            break
    return area if len(area) > 2 else None

df['Area'] = df['Area'].apply(keep_first_place_simple)

# Map Danish city names to English equivalents
city_name_mapping = {
    'København': 'Copenhagen',
    'Århus': 'Aarhus',
    'Helsingør': 'Elsinore'
}
df['Area'] = df['Area'].replace(city_name_mapping)

# Filter for 30 biggest cities in Denmark
cities = ["Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg", "Randers", "Kolding", "Horsens", "Vejle", "Roskilde",
          "Herning", "Hørsholm", "Silkeborg", "Næstved", "Fredericia", "Viborg", "Køge", "Holstebro", "Taastrup", "Slagelse",
          "Hillerød", "Sønderborg", "Svendborg", "Hjørring", "Holbæk", "Frederikshavn", "Nørresundby", "Ringsted", "Haderslev",
          "Skive", "Ølstykke-Stenløse", "Nykøbing Falster", "Greve Strand", "Kalundborg", "Ballerup", "Rødovre", "Lyngby",
          "Albertslund", "Hvidovre", "Glostrup", "Ishøj", "Birkerød", "Farum", "Frederikssund", "Brøndby Strand",
          "Skanderborg", "Hedensted", "Frederiksværk", "Lillerød", "Solrød Strand"]
df = df[df['Area'].isin(cities)]

# Save the cleaned DataFrame to a CSV file
df.to_csv("dataf.csv", index=False, encoding="utf-8")
