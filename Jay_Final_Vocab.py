import json
import os
import nltk
from nltk.corpus import names

# 📥 Download first names dataset (if still needed)
nltk.download("names", quiet=True)
first_names = set(n.lower() for n in names.words())

# 📂 Config
input_file = "D:/Inai/cleaned/cleaned75.json"      # Your main vocab file
output_clean_file = "D:/Inai/Final/Final75.json"
output_removed_file = "D:/Inai/Final_removed/removed75.json"

def is_first_name(word):
    return word.lower() in first_names

def is_plural_simple(word):
    w = word.lower()
    return len(w) > 3 and (w.endswith("s") or w.endswith("es"))

# 📥 Load vocab list
if not os.path.exists(input_file):
    print(f"❌ File not found: {input_file}")
    exit()

with open(input_file, "r", encoding="utf-8") as f:
    vocab_list = json.load(f)

removed_words = []
cleaned_words = []

for word in vocab_list:
    w = word.strip().lower()

    if is_first_name(w):  # Only first name check
        removed_words.append(word)
    elif is_plural_simple(w):  # Simple plural check
        removed_words.append(word)
    else:
        cleaned_words.append(word)

# 💾 Save cleaned words
with open(output_clean_file, "w", encoding="utf-8") as f:
    json.dump(sorted(cleaned_words), f, ensure_ascii=False, indent=4)

# 💾 Save removed words
with open(output_removed_file, "w", encoding="utf-8") as f:
    json.dump(sorted(set(removed_words)), f, ensure_ascii=False, indent=4)

print(f"✅ Cleaned vocab saved: {output_clean_file} ({len(cleaned_words)} words)")
print(f"✅ Removed words saved: {output_removed_file} ({len(removed_words)} words)")
