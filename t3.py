import json
import nltk
import re
from nltk.corpus import wordnet as wn

# === NLTK setup ===
nltk.download('wordnet')
nltk.download('omw-1.4')

# === CONFIG ===
input_file = "Allvocab_clean.json"
output_file = "vocab_cleaned.json"

# === CLEANING FUNCTIONS ===
def is_clean_word(word):
    return (
        3 <= len(word) <= 20 and
        word.isascii() and
        word.isalpha() and
        not re.search(r'(.)\1{2,}', word) and
        not re.search(r'http|www|\.com|\.org|\.net', word)
    )

def is_meaningful_noun(word):
    synsets = wn.synsets(word, pos=wn.NOUN)
    return any(syn.definition() for syn in synsets)

# === LOAD EXISTING NOUNS ===
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# It could be a dict (word → id), or a list of words
if isinstance(data, dict):
    all_words = list(data.keys())
elif isinstance(data, list):
    all_words = data
else:
    raise ValueError("Invalid format in input file.")

# === FILTER MEANINGFUL NOUNS ===
cleaned_nouns = sorted([
    word for word in all_words
    if is_clean_word(word) and is_meaningful_noun(word)
])

# === SAVE OUTPUT ===
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(cleaned_nouns, f, indent=2, ensure_ascii=False)

print(f"✅ Total meaningful nouns saved: {len(cleaned_nouns)}")
print(f"📁 Output saved to: {output_file}")
