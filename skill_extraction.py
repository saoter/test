
import pandas as pd
import requests
import json

# Load the cleaned data
df = pd.read_csv("dataf.csv")

# Initialize the DataFrame with additional columns for Primary and Secondary Skills
df['Primary_Skills'] = None
df['Secondary_Skills'] = None

# Skill extraction logic
SYSTEM_PROMPT = """Extract relevant skills from a job listing..."""  # Adjust prompt as needed

def extract_skills(html_text, row_id):
    prompt = f"Extract skills from: {html_text}"  # Simplified for demo
    # Replace with your request logic
    return {"skills": {"primary": [], "secondary": []}}

# Iterate over rows and process skill extraction
for i, row in df.iterrows():
    try:
        extracted_skills = extract_skills(row['HTML_Text'], row_id=i + 1)
        df.at[i, 'Primary_Skills'] = ", ".join(extracted_skills['skills']['primary'])
        df.at[i, 'Secondary_Skills'] = ", ".join(extracted_skills['skills']['secondary'])
    except Exception as e:
        print(f"Error processing row {i + 1}: {e}")

# Save the results
df.to_csv("updated_data_with_skills.csv", index=False, encoding="utf-8")
