
import pandas as pd
import re

# Load and clean the data
# Assuming the original data is loaded into a DataFrame `df`

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
