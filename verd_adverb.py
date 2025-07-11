import json
import nltk
import re
from nltk.corpus import wordnet as wn
from nltk import pos_tag, word_tokenize
from nltk.stem import WordNetLemmatizer

# 🔹 Download required resources
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')

# 🔹 Load the JSON word list
with open("D:/token/Allvocab_clean.json", "r", encoding="utf-8") as f:
    words = json.load(f)

# 🔹 NLTK Tools
lemmatizer = WordNetLemmatizer()

# 🔹 Check if a word is meaningful and valid
def is_valid_word(word):
    return (
        len(word) >= 3 and
        not re.fullmatch(r"[a-z]{1,2}", word) and
        not re.fullmatch(r"(.)\1{2,}", word) and
        re.search(r"[aeiou]", word) and
        wn.synsets(word)  # Must exist in WordNet
    )

# 🔹 Sets to collect unique cleaned verbs and adverbs
verbs = set()
adverbs = set()

for word in words:
    word = word.lower().strip()

    if not is_valid_word(word):
        continue

    # POS tagging
    tag = pos_tag([word])[0][1]

    # Verb: VB, VBD, VBG, etc.
    if tag.startswith("VB"):
        lemma = lemmatizer.lemmatize(word, 'v')  # Lemmatize as verb
        if is_valid_word(lemma):
            verbs.add(lemma)

    # Adverb: RB, RBR, RBS
    elif tag.startswith("RB"):
        lemma = lemmatizer.lemmatize(word, 'r')  # Lemmatize as adverb
        if is_valid_word(lemma):
            adverbs.add(lemma)

# 🔹 Convert sets to sorted lists
verbs_sorted = sorted(verbs)
adverbs_sorted = sorted(adverbs)

# 🔹 Save to JSON files
with open("D:/token/verbs_meaningful.json", "w", encoding="utf-8") as f:
    json.dump(verbs_sorted, f, indent=2, ensure_ascii=False)

with open("D:/token/adverbs_meaningful.json", "w", encoding="utf-8") as f:
    json.dump(adverbs_sorted, f, indent=2, ensure_ascii=False)

# 🔹 Print Summary
print(f"/n✅ Total Valid Verbs: {len(verbs_sorted)}")
print(f"✅ Total Valid Adverbs: {len(adverbs_sorted)}")
print("/n📁 Saved:")
print("  ✔ D:/token/verbs_meaningful.json")
print("  ✔ D:/token/adverbs_meaningful.json")
