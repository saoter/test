
import pandas as pd
import requests
import json

# Load the cleaned data
df = pd.read_csv("dataf.csv")


import os
import subprocess
import threading

# Function to start the Ollama server with GPU configurations
def start_ollama():
    # Ensure environment variables are set for GPU usage
    os.environ['OLLAMA_HOST'] = '0.0.0.0:11434'
    os.environ['OLLAMA_ORIGINS'] = '*'
    
    # Start the Ollama server
    subprocess.Popen(["ollama", "serve", "--device", "gpu"])

# Start the server in a separate thread
ollama_thread = threading.Thread(target=start_ollama)
ollama_thread.start()

##########################

SYSTEM_PROMPT = """

Extract relevant skills from a job listing, separating them into **primary IT-related technical skills** and **secondary (soft/general) skills**.

### Primary Skills
Focus on IT-specific technical skills such as:
- Programming languages, frameworks, tools, platforms, and technologies.
- Examples: Python, React, Docker, AWS, SQL, Kubernetes.

### Secondary Skills
Focus on non-technical competencies, including:
- Communication, leadership, problem-solving, teamwork, time management.

---

### Rules:
- Categorize skills into **primary** (IT-related) and **secondary** (soft skills).
- Split compound terms into separate skills (e.g., "Python and Java" → ["Python", "Java"]).
- Exclude vague, repetitive, or non-actionable terms.
- Translate all extracted skills into **English** if the input text is in Danish.
- Ensure outputs are clear, specific, and concise.


---

### Output Format:
```json
{
  "skills": {
    "primary": ["Skill 1", "Skill 2", "..."],
    "secondary": ["Skill A", "Skill B", "..."]
  }
}

---

Example:

Job: "Looking for Python, Docker, AWS, and strong communication skills."

Output:
{
    "skills": {
    "primary": ["Python", "Docker", "AWS"],
    "secondary": ["communication"]
  }
}

"""

###############################################################


# Function to extract skills (primary and secondary) from job listings
def extract_skills(html_text, row_id):
    prompt = f"""
    Extract the relevant skills from the following job listing, categorizing them into **primary (IT-related technical skills)** and **secondary (soft/general skills)**.

    ### Skills Categories:
    1. **Primary Skills:** IT-related technical skills (e.g., Python, AWS, Kubernetes, Docker, SQL, React, etc.).
    2. **Secondary Skills:** Soft skills and traits (e.g., Communication, Teamwork, Problem-Solving, Leadership).

    ### Rules:
    - Split combined skills into distinct terms (e.g., "Python and Java" → ["Python", "Java"]).
    - Focus on actionable, specific, and clearly stated skills.
    - Outputs must be in **clear and concise English only**, regardless of the input language.
    - If no skills are present, return empty lists for both categories.
    - Translate all extracted skills into **English** if the input text is in Danish.


    Provide the results in this JSON format:
    {{
      "skills": {{
        "primary": ["Skill 1", "Skill 2", "..."],
        "secondary": ["Skill A", "Skill B", "..."]
      }}
    }}

    Job Listing Text:
    {html_text}
    """
    try:
        response = ollama.chat(
            model='llama3.1:8b',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': prompt},
            ],
            format='json',
            options={"temperature": 0.1}
        )
        return json.loads(response['message']['content'])
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing JSON for Row {row_id}: {e}")
        return {"skills": {"primary": [], "secondary": []}}

# Function to refine extracted skills
def refine_skills(extracted_skills, row_id):
    refinement_prompt = f"""
    Refine the following extracted skills, ensuring accuracy and clear categorization:

    Extracted Skills:
    {json.dumps(extracted_skills, indent=2)}

    ### Refined Categories:
    1. **Primary Skills:** IT-related technical skills only.
    2. **Secondary Skills:** Clearly defined soft skills.

    ### Rules:
    - Keep skills actionable, specific, and IT-relevant for "primary."
    - Use concise and clear English.
    - Outputs must be in **clear and concise English.**
    - Translate all extracted skills into **English** if the input text is in Danish.

    Provide the results in this JSON format:
    {{
      "skills": {{
        "primary": ["Skill 1", "Skill 2", "..."],
        "secondary": ["Skill A", "Skill B", "..."]
      }}
    }}
    """
    try:
        response = ollama.chat(
            model='llama3.1:8b',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': refinement_prompt},
            ],
            format='json',
            options={"temperature": 0.1}
        )
        return json.loads(response['message']['content'])
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error refining skills for Row {row_id}: {e}")
        return extracted_skills

##############################################################


# Initialize the DataFrame with the columns for Primary_Skills and Secondary_Skills
df['Primary_Skills'] = None
df['Secondary_Skills'] = None

# Output file to store the results
output_file = 'updated_job_listings_with_skills.csv'
log_file = '[STEP5]LLM_for_project_progress_log.log'

# Create the CSV file with headers before the loop if it doesn't exist
if not os.path.exists(output_file):
    df[:0].to_csv(output_file, index=False, encoding='utf-8')  # Write only the headers initially

# Check the log file to find the last processed row
if os.path.exists(log_file):
    with open(log_file, 'r') as log:
        last_processed = int(log.read().strip())
else:
    last_processed = -1  # Start from the beginning if no log file exists

# Process job listings and add extracted skills to the DataFrame
for i, html_text in enumerate(df['HTML_Text']):
    # Skip rows that have already been processed
    if i <= last_processed:
        continue

    try:
        # Extract and refine skills
        extracted = extract_skills(html_text, row_id=i + 1)
        refined = refine_skills(extracted, row_id=i + 1)

        # Safely retrieve primary and secondary skills
        primary_skills = ", ".join(refined.get('skills', {}).get('primary', [])) or ""
        secondary_skills = ", ".join(refined.get('skills', {}).get('secondary', [])) or ""

        print(f"Primary Skills for Row {i + 1}: {primary_skills}")
        print(f"Secondary Skills for Row {i + 1}: {secondary_skills}")

        # Manually construct the row to save
        row_to_save = df.iloc[i].to_dict()  # Get the current row as a dictionary
        row_to_save['Primary_Skills'] = primary_skills
        row_to_save['Secondary_Skills'] = secondary_skills

        # Append the row to the CSV file
        pd.DataFrame([row_to_save]).to_csv(output_file, mode='a', index=False, header=False, encoding='utf-8')

        print(f"Row {i + 1} saved to {output_file}.")

        # Update the log file with the last processed row
        with open(log_file, 'w') as log:
            log.write(str(i))

    except Exception as e:
        print(f"Error processing Row {i + 1}: {e}")

        # Save the row with empty skills to the CSV
        row_to_save = df.iloc[i].to_dict()
        row_to_save['Primary_Skills'] = ""
        row_to_save['Secondary_Skills'] = ""
        pd.DataFrame([row_to_save]).to_csv(output_file, mode='a', index=False, header=False, encoding='utf-8')

        # Update the log file with the last processed row, even if it failed
        with open(log_file, 'w') as log:
            log.write(str(i))
