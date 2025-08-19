import json
import nltk
from nltk.corpus import wordnet as wn
# :inbox_tray: Download WordNet if not already done
nltk.download('wordnet')
nltk.download('omw-1.4')
# :open_file_folder: Load words
with open("D:/Inai/JsonFile/Allvocab_ultimate77.json", "r", encoding="utf-8") as f:
    word_list = json.load(f)
# :mag: Get definitions for each word


def get_definition(word):
    synsets = wn.synsets(word)
    if not synsets:
        return None
    # Combine all unique definitions from all senses
    definitions = list(set([s.definition() for s in synsets]))
    return definitions


# :repeat: Build word: definitions dict
vocab_with_meanings = {}
for word in word_list:
    meaning = get_definition(word)
    if meaning:
        vocab_with_meanings[word] = meaning
# :floppy_disk: Save to new JSON file
with open("D:/Inai/MeaningFull/meaningful_meanings77.json", "w", encoding="utf-8") as f:
    json.dump(vocab_with_meanings, f, indent=2, ensure_ascii=False)
print(
    f":white_check_mark: Saved meanings for {len(vocab_with_meanings)} words to 'D:/Inai/MeaningFull/meaningful_meanings77.json'")