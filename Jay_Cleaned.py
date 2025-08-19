import json
import os
import re
import time
from nltk.stem import WordNetLemmatizer
import nltk
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

nltk.download("wordnet")
nltk.download("omw-1.4")

input_file = "D:/Inai/plural/Plural_words75.json"
output_clean_file = "D:/Inai/cleaned/cleaned75.json"
output_removed_file = "D:/Inai/cleaned_removed/removed75.json"

remove_lang_codes = {"de","ko","ja","fr","es","it","hi","ar","zh-cn","zh-tw","zh","ru"}

lemmatizer = WordNetLemmatizer()

def is_roman_word(word):
    if not re.fullmatch(r"[a-zA-Z]+", word):
        return False
    roman_pattern = r"^(M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))$"
    if re.fullmatch(roman_pattern, word.upper()):
        return False
    return True

def detect_language(word):
    try:
        if len(word) < 3:
            return "unknown"
        return detect(word)
    except:
        return "unknown"

def clean_vocab(word_list):
    cleaned_set = set()
    removed_list = []

    for word in word_list:
        word_lower = word.lower().strip()
        if word_lower in cleaned_set:
            removed_list.append(word)
            continue
        if not is_roman_word(word_lower):
            removed_list.append(word)
            continue
        lang = detect_language(word_lower)
        if lang in remove_lang_codes:
            removed_list.append(word)
            continue
        singular_word = lemmatizer.lemmatize(word_lower)
        if singular_word != word_lower:
            removed_list.append(word)
        cleaned_set.add(singular_word)

    return sorted(cleaned_set), sorted(set(removed_list))


# ---- MAIN ----
start_time = time.time()

if not os.path.exists(input_file):
    print(f"❌ File not found: {input_file}")
    exit()

with open(input_file, "r", encoding="utf-8") as f:
    vocab_list = json.load(f)

if not isinstance(vocab_list, list):
    print("❌ Input JSON is not a list of words.")
    exit()

cleaned_vocab, removed_vocab = clean_vocab(vocab_list)

with open(output_clean_file, "w", encoding="utf-8") as f:
    json.dump(cleaned_vocab, f, ensure_ascii=False, indent=4)

with open(output_removed_file, "w", encoding="utf-8") as f:
    json.dump(removed_vocab, f, ensure_ascii=False, indent=4)

end_time = time.time()
elapsed = end_time - start_time

# 🔹 Collect all outputs
outputs = [
    f"✅ Cleaned vocab saved to: {output_clean_file}",
    f"✅ Removed words saved to: {output_removed_file}",
    f"📦 Original size: {len(vocab_list)} words",
    f"📦 Cleaned size: {len(cleaned_vocab)} words",
    f"📦 Removed size: {len(removed_vocab)} words"
]

# 🔹 First output: time
print(f"🕒 Processing time: {elapsed:.2f} seconds")
# 🔹 Then print rest
for line in outputs:
    print(line)
