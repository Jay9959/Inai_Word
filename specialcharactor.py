import os
import json
import re
import unicodedata

# === CONFIG ===
input_folder = "data"
output_file = "special_character.json"
skip_files = {"vocab.json", "Allvocabs.json"}

special_chars = set()

# === HELPER: Check if character is printable and visible ===
def is_visible_symbol(char):
    # Exclude control characters, invisible, and undefined Unicode blocks
    return char.isprintable() and not unicodedata.category(char).startswith("C")

# === PROCESS FILES ===
for filename in os.listdir(input_folder):
    if filename.endswith(".json") and filename not in skip_files:
        file_path = os.path.join(input_folder, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)

                entries = [data] if isinstance(data, dict) else data
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue

                    for field in ["question", "answer", "subject"]:
                        text = entry.get(field, "")
                        if isinstance(text, str):
                            chars = re.findall(r"[^\w\s]", text)
                            special_chars.update(chars)

        except Exception as e:
            print(f"❌ Error in {filename}: {e}")

# === FILTER OUT NON-VISIBLE CHARACTERS ===
cleaned_chars = sorted({ch for ch in special_chars if is_visible_symbol(ch)})

# === SAVE CLEAN CHARACTERS ===
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(cleaned_chars, f, indent=2, ensure_ascii=False)

print(f"✅ Total visible special characters: {len(cleaned_chars)}")
print(f"📁 Saved to: {output_file}")
