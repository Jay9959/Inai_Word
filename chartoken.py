import json
import string
import os

input_folder = r"D:\Token"            
output_file = "chartoken.json"

unique_words = set()

if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            existing_words = json.load(f)
            unique_words.update(existing_words)
    except json.JSONDecodeError:
        print(f"Warning: '{output_file}' is empty or invalid. Starting fresh.")

all_files = sorted([f for f in os.listdir(input_folder) if f.endswith(".json")])
selected_files = all_files[375:500]

print(f"Files to process: {len(selected_files)}")

for file_name in selected_files:
    file_path = os.path.join(input_folder, file_name)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

            entries = data if isinstance(data, list) else [data]

            for entry in entries:
                answer = entry.get("answer", "")
                if isinstance(answer, str):
                    clean_text = answer.translate(str.maketrans("", "", string.punctuation)).lower()
                    words = clean_text.split()
                    unique_words.update(words)

    except json.JSONDecodeError:
        print(f"Skipping invalid JSON file: {file_name}")
    except Exception as e:
        print(f"Error reading {file_name}: {e}")

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(sorted(unique_words), f, indent=4)

print(f"Total unique words saved: {len(unique_words)}")
print(f"Saved to: {output_file}")
