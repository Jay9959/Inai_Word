import os
import json
import re
import nltk
from nltk.stem import WordNetLemmatizer

# 📥 Download required NLTK data
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('names')  # For name filtering

from nltk.corpus import names

# 📂 File paths
input_file = r"D:/Inai/MeaningFull/meaningful_meanings77.json"
output_dir = r"D:/Inai/plural"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "Plural_words77.json")

# 📥 Load dictionary
with open(input_file, "r", encoding="utf-8") as f:
    vocab_dict = json.load(f)

# ✂ Extract only words (keys)
word_list = list(vocab_dict.keys())

# 📝 Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

# 📜 Prepare name & surname set from NLTK names corpus
name_set = {n.lower() for n in names.words()}

# 🏛 Roman numeral regex
roman_pattern = re.compile(r"^(?=[MDCLXVI])M{0,4}(CM|CD|D?C{0,3})"
                           r"(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$", re.IGNORECASE)

# 🔄 Convert to singular, remove duplicates, and filter
filtered_words = set()
for word in word_list:
    w_lower = word.lower().strip()

    # ❌ Remove Roman numerals
    if roman_pattern.match(w_lower):
        continue

    # ❌ Remove names & surnames
    if w_lower in name_set:
        continue

    # ✅ Lemmatize to singular (noun)
    singular_form = lemmatizer.lemmatize(w_lower, pos='n')
    filtered_words.add(singular_form)

# 📊 Sort final words
singular_words = sorted(filtered_words)

# 💾 Save final cleaned list
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(singular_words, f, indent=2, ensure_ascii=False)

print(
    f"✅ Extracted {len(word_list)} words → {len(singular_words)} cleaned singular words saved to '{output_file}'"
)
