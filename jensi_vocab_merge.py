# import json

# # File paths
# file1 = "harddisk_vocab2.json"
# file2 = "D:/token/final/english_only_vocab27.json"
# output_file = "harddisk_vocab3.json"

# # Load vocab files
# with open(file1, "r", encoding="utf-8") as f:
#     vocab1 = json.load(f)

# with open(file2, "r", encoding="utf-8") as f:
#     vocab2 = json.load(f)

# # Merge and keep only unique words
# merged_vocab = sorted(set(vocab1 + vocab2))

# # Save to JSON
# with open(output_file, "w", encoding="utf-8") as f:
#     json.dump(merged_vocab, f, ensure_ascii=False, indent=2)

# print(f"✅ Merged vocab saved to {output_file}")
# print(f"📦 Total unique words: {len(merged_vocab)}")





import json
import re

# ==== File paths ====
input_file = "merged_sorted8_139.json"
output_file_english = "merge_139.json"
output_file_removed = "removed_indian_words6.json"

# ==== Load vocab file ====
with open(input_file, "r", encoding="utf-8") as f:
    vocab = json.load(f)

# Remove duplicates
vocab_set = set(vocab)

# ==== Expanded Indian/Hindu culture words ====
indian_keywords = {
    # Common names
    "veer", "ram", "maharani", "raj", "arjun", "krishna", "radha", "shah",
    "mehta", "singh", "patel", "gupta", "yadav", "roy", "rao", "iyer", "reddy",
    "lal", "das", "maharaj", "pandit", "swami", "babu", "anil", "kaushik",
    "raja", "rani", "manoj", "sita", "laxmi", "gopal", "bharat", "vishnu",
    "shiv", "ganesh", "durga", "meenakshi", "suresh", "ajay", "amit", "baby",
    "amma", "appa", "bhai", "behan", "didi", "nana", "nani", "dada", "dadi",
    "guru", "maata", "pitaji", "bahu", "beta", "beti", "shankar", "babuji",
    "maharaja", "pavan",
    # Epics & texts
    "ramayan", "ramayana", "mahabharata", "mahabharat", "mahabharatam",
    "bhagavad", "gita", "puran", "purana", "upanishad", "vedas", "veda", "rigveda",
    "samveda", "yajurveda", "atharvaveda", "srimad", "bhagavata", "bharatham",
    # Gods & goddesses
    "shiva", "vishnu", "brahma", "parvati", "lakshmi", "saraswati", "kali",
    "hanuman", "kartikeya", "indra", "agni", "varuna", "yamraj", "yama",
    "chandra", "surya", "shani", "ganapati", "ganeshji", "gajanan", "murugan"
}

# ==== Regex for pure English words ====
english_word_pattern = re.compile(r"^[a-zA-Z-]+$")

# ==== Function to check if a word is Indian-related ====
def is_indian_word(word):
    w = word.lower()
    # Direct match
    if w in indian_keywords:
        return True
    # Partial match (e.g., mahabharatam contains mahabharata)
    for kw in indian_keywords:
        if kw in w:
            return True
    return False

# ==== Separate English/US and removed Indian words ====
english_us_vocab = sorted(
    word for word in vocab_set
    if english_word_pattern.match(word) and not is_indian_word(word)
)
removed_vocab = sorted(
    word for word in vocab_set
    if english_word_pattern.match(word) and is_indian_word(word)
)

# ==== Save results ====
with open(output_file_english, "w", encoding="utf-8") as f:
    json.dump(english_us_vocab, f, ensure_ascii=False, indent=2)

with open(output_file_removed, "w", encoding="utf-8") as f:
    json.dump(removed_vocab, f, ensure_ascii=False, indent=2)

print(f"✅ Cleaned vocab saved to {output_file_english}")
print(f"📦 Total English/US words: {len(english_us_vocab)}")
print(f"❌ Removed Indian-related words saved to {output_file_removed}")
print(f"📦 Total removed: {len(removed_vocab)}")
