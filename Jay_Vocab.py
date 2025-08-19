# import os
# import json
# import re

# # 📂 Folder paths
# input_folder = "D:/Inai/Anil_Data57"
# output_file = os.path.join("D:/Inai/JsonFile", "Allvocab_ultimate57.json")
# os.makedirs("D:/Inai/JsonFile", exist_ok=True)  # Ensure output folder exists

# vocab_set = set()

# # 🔍 Regex to match valid words (3+ alphabetic characters)
# word_pattern = re.compile(r'\b[a-zA-Z]{3,}\b')

# # 📄 Process each JSON file in the folder
# for filename in os.listdir(input_folder):
#     if filename.endswith(".json"):
#         file_path = os.path.join(input_folder, filename)
#         try:
#             with open(file_path, "r", encoding="utf-8") as f:
#                 data = json.load(f)

#                 # ✅ If JSON root is a list
#                 if isinstance(data, list):
#                     for item in data:
#                         if isinstance(item, dict):
#                             # Ignore 'title' & 'paragraphs', only take 'question', 'answer', 'content'
#                             content_text = item.get("content", "")

#                             vocab_set.update(word.lower() for word in word_pattern.findall(str(content_text)))

#                 # ✅ If JSON root is a dictionary
#                 elif isinstance(data, dict):
#                     content_text = data.get("content", "")

#                     vocab_set.update(word.lower() for word in word_pattern.findall(str(content_text)))

#         except Exception as e:
#             print(f"❌ Error reading {filename}: {e}")

# # 📋 Convert to sorted list
# vocab_list = sorted(vocab_set)

# # 💾 Save to JSON
# with open(output_file, "w", encoding="utf-8") as out_f:
#     json.dump(vocab_list, out_f, indent=2)

# print(f"✅ Total unique vocab words: {len(vocab_list)}")
# print(f"✅ Vocab saved to {output_file}")

# ====================================   # Take only 'Text' ===============================

import os
import json
import re

# 📂 Folder paths
input_folder = "D:/Inai/Anil_Data77"
output_dir = "tokens"
os.makedirs(output_dir, exist_ok=True)  # ✅ Ensure folder exists
output_file = os.path.join(
    output_dir, "D:/Inai/JsonFile/Allvocab_ultimate77.json")

# 🔍 Regex for valid words (3+ alphabetic characters)
word_pattern = re.compile(r'\b[a-zA-Z]{3,}\b')

vocab_set = set()

# 📄 Process each JSONL file
for filename in os.listdir(input_folder):
    if filename.endswith(".jsonl"):
        file_path = os.path.join(input_folder, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        text_content = data.get("text", "")
                        vocab_set.update(word.lower()
                                         for word in word_pattern.findall(text_content))
                    except json.JSONDecodeError as e:
                        print(f"⚠ JSON decode error in {filename}: {e}")
        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")

# 📋 Convert to sorted list
vocab_list = sorted(vocab_set)

# 💾 Save to JSON
with open(output_file, "w", encoding="utf-8") as out_f:
    json.dump(vocab_list, out_f, indent=2)

print(f"✅ Total unique vocab words: {len(vocab_list)}")
print(f"✅ Vocab saved to {output_file}")

# ====================================   # Take only 'title' and 'answer' ===============================

# import os
# import json
# import re

# # 📂 Folder paths
# input_folder = "D:/Inai/Anil_Data64"
# output_file = os.path.join("D:/Inai/JsonFile", "Allvocab_ultimate64.json")
# os.makedirs("D:/Inai/JsonFile", exist_ok=True)  # Ensure output folder exists

# vocab_set = set()

# # 🔍 Regex to match valid words (3+ alphabetic characters)
# word_pattern = re.compile(r'\b[a-zA-Z]{3,}\b')

# # 📄 Process each JSON file in the folder
# for filename in os.listdir(input_folder):
#     if filename.endswith(".json"):
#         file_path = os.path.join(input_folder, filename)
#         try:
#             with open(file_path, "r", encoding="utf-8") as f:
#                 data = json.load(f)

#                 # ✅ If JSON root is a list
#                 if isinstance(data, list):
#                     for item in data:
#                         if isinstance(item, dict):
#                             # Take only 'title' and 'answer'
#                             combined_text = " ".join([
#                                 str(item.get("title", "")),
#                                 str(item.get("answer", "")),
#                             ])
#                             vocab_set.update(word.lower() for word in word_pattern.findall(combined_text))

#                 # ✅ If JSON root is a dictionary
#                 elif isinstance(data, dict):
#                     combined_text = " ".join([
#                         str(data.get("title", "")),
#                         str(data.get("answer", "")),
#                     ])
#                     vocab_set.update(word.lower() for word in word_pattern.findall(combined_text))

#         except Exception as e:
#             print(f"❌ Error reading {filename}: {e}")

# # 📋 Convert to sorted list
# vocab_list = sorted(vocab_set)

# # 💾 Save to JSON
# with open(output_file, "w", encoding="utf-8") as out_f:
#     json.dump(vocab_list, out_f, indent=2)

# print(f"✅ Total unique vocab words: {len(vocab_list)}")
# print(f"✅ Vocab saved to {output_file}")
