import json
import os
import re
import nltk
from nltk.corpus import stopwords

# Download NLTK stopwords if not already present
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Folder paths
input_folder = "falloutmods"
output_file = os.path.join("tokens", "Allvocab_clean.json")
common_words_file = os.path.join("diff", "Common_words.json")

# Get English stop words from NLTK
COMMON_WORDS = set(stopwords.words('english'))

# Add any additional words you want to consider as common
EXTRA_COMMON_WORDS = {
    'like', 'get', 'got', 'would', 'could', 'also', 
    'really', 'much', 'many', 'even', 'still', 'well'
}
COMMON_WORDS.update(EXTRA_COMMON_WORDS)

# Set to collect all unique words across files
all_words = set()
common_words = set()
content_words = set()

# List of files to skip
skip_files = {"vocab.json", "Allvocabs.json"}

# Loop through all JSON files in the folder
for filename in os.listdir(input_folder):
    if filename.endswith(".json") and filename not in skip_files:
        file_path = os.path.join(input_folder, filename)

        # Load each JSON file
        with open(file_path, "r", encoding="utf-8") as file:
            try:
                data = json.load(file)

                # If file contains a list of entries
                if isinstance(data, list):
                    entries = data
                # If file contains a single entry (dict), wrap it in a list
                elif isinstance(data, dict):
                    entries = [data]
                else:
                    continue  # Skip if unexpected structure

                # Extract and clean words from each entry's fields
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue  # Just in case

                    # Process all relevant fields
                    fields_to_process = ["question", "answer", "subject"]
                    for field in fields_to_process:
                        field_content = entry.get(field, "")
                        if isinstance(field_content, str):
                            clean_text = re.sub(r"[^a-zA-Z]", " ", field_content.lower())
                            words = clean_text.split()
                            all_words.update(words)

            except json.JSONDecodeError:
                print(f"❌ Skipping invalid JSON: {filename}")
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")

# Separate common words from content words
for word in all_words:
    if word in COMMON_WORDS:
        common_words.add(word)
    else:
        content_words.add(word)

# Sort alphabetically
sorted_vocab = sorted(content_words)
sorted_common = sorted(common_words)

# Save to JSON files
os.makedirs("tokens", exist_ok=True)
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(sorted_vocab, f, indent=2)

with open(common_words_file, "w", encoding="utf-8") as f:
    json.dump(sorted_common, f, indent=2)

print(f"✅ Total words processed: {len(all_words)}")
print(f"✅ Common words found: {len(common_words)}")
print(f"✅ Content words found: {len(content_words)}")
print(f"✅ Saved content words to {output_file}")
print(f"✅ Saved common words to {common_words_file}")