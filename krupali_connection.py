#connection into json file

# import os
# import re
# import ujson as json
# import builtins
# from collections import defaultdict
# from hashlib import blake2b
# from tqdm import tqdm
# import gc
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import threading
# import random

# # --- Paths ---
# ROOT_FOLDER = "D:/download/batch_1/batch_1"# Folder with .json / .jsonl files
# OUTPUT_FOLDER = "D:/tokenid/cleanword"   # Prediction output folder
# VOCAB_FILE = "D:/download/english_only_vocab22.json"  # Vocabulary JSON file

# # Real-time printing
# print = lambda *args, **kwargs: builtins.print(*args, flush=True, **kwargs)

# # Thread lock for file operations
# file_lock = threading.Lock()

# # Regex for detecting scientific notation
# sci_notation_pattern = re.compile(r'^-?\d+(\.\d+)?e-?\d+$', re.IGNORECASE)
# replacement_map = {}

# # Global tensor ID mapping
# word_to_tensor_id = {}
# word_tensor_id_lock = threading.Lock()

# # --- Utility Functions ---
# def generate_global_tensor_id(word):
#     """Generate unique, consistent global tensor ID for a word."""
#     with word_tensor_id_lock:
#         if word in word_to_tensor_id:
#             return word_to_tensor_id[word]

#         unique_str = word.encode("utf-8")
#         h = blake2b(unique_str, digest_size=6).hexdigest()
#         int_val = int(h, 16)
#         tensor_id = round((int_val % 999999999999) / 1000000000000.0, 12)

#         val_str = str(tensor_id)
#         if sci_notation_pattern.match(val_str):
#             tensor_id = get_replacement(val_str)

#         word_to_tensor_id[word] = tensor_id
#         return tensor_id

# def is_repetitive(word):
#     """Skip repetitive or garbage words."""
#     return (
#         len(word) > 20 or
#         (len(word) > 4 and (all(c == word[0] for c in word) or len(set(word)) <= 2)) or
#         re.search(r'[<>:\"/\\|?*]', f"{word}_predictions.json")
#     )


# def get_replacement(original_value):
#     """Replacement for scientific notation values."""
#     if original_value not in replacement_map:
#         replacement_map[original_value] = round(random.uniform(0, 1), 8)
#     return replacement_map[original_value]

# # --- Local File Processing ---
# def process_single_file(file_path, vocab):
#     """Reads a local JSON/JSONL file and extracts word pairs."""
#     local_pairs = defaultdict(lambda: defaultdict(int))
#     try:
#         if file_path.lower().endswith(".jsonl"):
#             lines = []
#             with open(file_path, 'r', encoding='utf-8') as f:
#                 for line in f:
#                     line = line.strip()
#                     if line:
#                         try:
#                             lines.append(json.loads(line))
#                         except:
#                             continue
#             content = json.dumps(lines)
#         else:
#             with open(file_path, 'r', encoding='utf-8') as f:
#                 content = json.dumps(json.load(f))

#         words = re.findall(r'\b[a-z]+\b', content.lower())
#         for i in range(len(words) - 1):
#             w1, w2 = words[i], words[i + 1]
#             if w1 in vocab and not is_repetitive(w1):
#                 generate_global_tensor_id(w1)
#                 local_pairs[w1][w2] += 1
#     except Exception as e:
#         print(f"⚠ Error processing {file_path}: {e}")
#     return local_pairs

# def merge_word_pairs(main_dict, local_dict):
#     """Merge local word pairs into main dictionary."""
#     for word, next_words in local_dict.items():
#         for next_word, count in next_words.items():
#             main_dict[word][next_word] += count

# # --- Prediction Updates ---
# def process_word_batch(word_batch, word_pair_freq):
#     processed_count = 0
#     skipped_count = 0

#     for word in word_batch:
#         if word not in word_pair_freq:
#             continue

#         if is_repetitive(word):
#             continue

#         filename = f"{word}_predictions.json"
#         if len(filename) > 200:
#             continue

#         out_file = os.path.join(OUTPUT_FOLDER, filename)
#         next_words = word_pair_freq[word]

#         predictions = {}
#         if os.path.exists(out_file):
#             try:
#                 with open(out_file, 'r', encoding='utf-8') as f:
#                     predictions = json.load(f)
#             except:
#                 predictions = {}
#                 print(f"⚠ Could not load existing file for '{word}', starting fresh")

#         # Load existing tensor IDs into global mapping
#         with word_tensor_id_lock:
#             for next_w, tid in predictions.items():
#                 if next_w not in word_to_tensor_id:
#                     word_to_tensor_id[next_w] = tid

#         updated = False
#         new_connections = 0

#         for next_word in next_words:
#             tensor_id_for_next_word = generate_global_tensor_id(next_word)

#             if next_word in predictions:
#                 if predictions[next_word] != tensor_id_for_next_word:
#                     predictions[next_word] = tensor_id_for_next_word
#                     updated = True
#                 continue

#             predictions[next_word] = tensor_id_for_next_word
#             updated = True
#             new_connections += 1

#         if new_connections > 0:
#             print(f"✅ '{word}': Added {new_connections} new connections (Total: {len(predictions)})")

#         if updated:
#             sorted_predictions = {
#                 k: v for k, v in sorted(predictions.items(), key=lambda x: -x[1])
#                 if len(k) <= 20
#             }

#             try:
#                 with file_lock:
#                     with open(out_file, "w", encoding='utf-8') as f:
#                         json.dump(sorted_predictions, f, indent=2)
#                 processed_count += 1
#                 print(f"💾 Successfully updated '{word}_predictions.json'")
#             except Exception as e:
#                 print(f"⚠ Failed to save file for word '{word}': {e}")
#         else:
#             skipped_count += 1

#     return processed_count, skipped_count

# # --- Main Local Processing ---
# def process_local_json_files():
#     print("📥 Loading vocab from JSON...")
#     with open(VOCAB_FILE, 'r', encoding='utf-8') as f:
#         vocab = set(json.load(f))
#         for word in vocab:
#             generate_global_tensor_id(word)
#     print(f"📚 Loaded {len(vocab)} vocabulary words and assigned global IDs.")

#     json_files = []
#     for root, _, files in os.walk(ROOT_FOLDER):
#         for file in files:
#             if file.lower().endswith(('.json', '.jsonl')):
#                 json_files.append(os.path.join(root, file))
#     print(f"📄 Found {len(json_files)} input files.")

#     word_pair_freq = defaultdict(lambda: defaultdict(int))

#     print("🔄 Processing files to collect word pairs...")
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [executor.submit(process_single_file, path, vocab) for path in json_files]
#         for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
#             local_pairs = future.result()
#             merge_word_pairs(word_pair_freq, local_pairs)

#     print(f"✅ Frequency extraction complete. Found {len(word_pair_freq)} unique words.")

#     print("🧠 Updating predictions...")
#     words_to_process = list(word_pair_freq.keys())
#     batch_size = 100
#     total_processed = 0
#     total_skipped = 0

#     for i in tqdm(range(0, len(words_to_process), batch_size), desc="Processing word batches"):
#         word_batch = words_to_process[i:i + batch_size]
#         processed, skipped = process_word_batch(word_batch, word_pair_freq)
#         total_processed += processed
#         total_skipped += skipped

#         for word in word_batch:
#             if word in word_pair_freq:
#                 del word_pair_freq[word]

#         if i % (batch_size * 10) == 0:
#             gc.collect()
#             print(f"🎯 Processed so far: {total_processed} | Skipped: {total_skipped}")

#     print(f"\n✅ Finished processing. {total_processed} files updated, {total_skipped} skipped.")
#     print(f"📁 All predictions saved in: {OUTPUT_FOLDER}")

#     try:
#         with open(os.path.join(OUTPUT_FOLDER, "global_tensor_ids.json"), "w", encoding='utf-8') as f:
#             json.dump(word_to_tensor_id, f, indent=2)
#         print(f"💾 Global tensor IDs saved.")
#     except Exception as e:
#         print(f"⚠ Error saving global_tensor_ids.json: {e}")

# if __name__ == "__main__":
#     os.makedirs(OUTPUT_FOLDER, exist_ok=True)
#     try:
#         process_local_json_files()
#     except KeyboardInterrupt:
#         print("\n⚠ Process interrupted by user.")
#     except Exception as e:
#         print(f"\n❌ Fatal error: {e}")
#         import traceback
#         traceback.print_exc()

#connection into txt file
import os
import re
import ujson as json
import builtins
from collections import defaultdict
from hashlib import blake2b
from tqdm import tqdm
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random

# --- Paths ---
ROOT_FOLDER = "D:/download/batch_31/batch_31"  # 📂 Folder with .txt files
OUTPUT_FOLDER = "D:/tokenid/cleanword"       # 📂 Prediction output folder
VOCAB_FILE = "D:/download/english_only_vocab22.json"  # 📂 Vocabulary JSON file

# Real-time printing
print = lambda *args, **kwargs: builtins.print(*args, flush=True, **kwargs)

# Thread lock for file operations
file_lock = threading.Lock()

# Regex for detecting scientific notation
sci_notation_pattern = re.compile(r'^-?\d+(\.\d+)?e-?\d+$', re.IGNORECASE)
replacement_map = {}

# Global tensor ID mapping
word_to_tensor_id = {}
word_tensor_id_lock = threading.Lock() 
                                                                                                                                                                                                                                                                                                                                

# --- Utility Functions ---
def generate_global_tensor_id(word):
    """Generate unique, consistent global tensor ID for a word."""
    with word_tensor_id_lock:
        if word in word_to_tensor_id:
            return word_to_tensor_id[word]

        unique_str = word.encode("utf-8")
        h = blake2b(unique_str, digest_size=6).hexdigest()
        int_val = int(h, 16)
        tensor_id = round((int_val % 999999999999) / 1000000000000.0, 12)

        val_str = str(tensor_id)
        if sci_notation_pattern.match(val_str):
            tensor_id = get_replacement(val_str)

        word_to_tensor_id[word] = tensor_id
        return tensor_id

def is_repetitive(word):
    """Skip repetitive or garbage words."""
    return (
        len(word) > 20 or
        (len(word) > 4 and (all(c == word[0] for c in word) or len(set(word)) <= 2)) or
        re.search(r'[<>:\"/\\|?*]', f"{word}_predictions.json")
    )

def get_replacement(original_value):
    """Replacement for scientific notation values."""
    if original_value not in replacement_map:
        replacement_map[original_value] = round(random.uniform(0, 1), 8)
    return replacement_map[original_value]

# --- TXT File Processing ---
def process_single_file(file_path, vocab):
    """Reads a local TXT file and extracts word pairs."""
    local_pairs = defaultdict(lambda: defaultdict(int))
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        words = re.findall(r'\b[a-z]+\b', content.lower())
        for i in range(len(words) - 1):
            w1, w2 = words[i], words[i + 1]
            if w1 in vocab and not is_repetitive(w1):
                generate_global_tensor_id(w1)
                local_pairs[w1][w2] += 1
    except Exception as e:
        print(f"⚠ Error processing {file_path}: {e}")
    return local_pairs

def merge_word_pairs(main_dict, local_dict):
    """Merge local word pairs into main dictionary."""
    for word, next_words in local_dict.items():
        for next_word, count in next_words.items():
            main_dict[word][next_word] += count

# --- Prediction Updates (same as before) ---
def process_word_batch(word_batch, word_pair_freq):
    processed_count = 0
    skipped_count = 0

    for word in word_batch:
        if word not in word_pair_freq:
            continue

        if is_repetitive(word):
            continue

        filename = f"{word}_predictions.json"
        if len(filename) > 200:
            continue

        out_file = os.path.join(OUTPUT_FOLDER, filename)
        next_words = word_pair_freq[word]

        predictions = {}
        if os.path.exists(out_file):
            try:
                with open(out_file, 'r', encoding='utf-8') as f:
                    predictions = json.load(f)
            except:
                predictions = {}
                print(f"⚠ Could not load existing file for '{word}', starting fresh")

        # Load existing tensor IDs into global mapping
        with word_tensor_id_lock:
            for next_w, tid in predictions.items():
                if next_w not in word_to_tensor_id:
                    word_to_tensor_id[next_w] = tid

        updated = False
        new_connections = 0

        for next_word in next_words:
            tensor_id_for_next_word = generate_global_tensor_id(next_word)

            if next_word in predictions:
                if predictions[next_word] != tensor_id_for_next_word:
                    predictions[next_word] = tensor_id_for_next_word
                    updated = True
                continue

            predictions[next_word] = tensor_id_for_next_word
            updated = True
            new_connections += 1

        if new_connections > 0:
            print(f"✅ '{word}': Added {new_connections} new connections (Total: {len(predictions)})")

        if updated:
            sorted_predictions = {
                k: v for k, v in sorted(predictions.items(), key=lambda x: -x[1])
                if len(k) <= 20
            }

            try:
                with file_lock:
                    with open(out_file, "w", encoding='utf-8') as f:
                        json.dump(sorted_predictions, f, indent=2)
                processed_count += 1
                print(f"💾 Successfully updated '{word}_predictions.json'")
            except Exception as e:
                print(f"⚠ Failed to save file for word '{word}': {e}")
        else:
            skipped_count += 1

    return processed_count, skipped_count

# --- Main Processing for TXT ---
def process_local_txt_files():
    print("📥 Loading vocab from JSON...")
    with open(VOCAB_FILE, 'r', encoding='utf-8') as f:
        vocab = set(json.load(f))
        for word in vocab:
            generate_global_tensor_id(word)
    print(f"📚 Loaded {len(vocab)} vocabulary words and assigned global IDs.")

    txt_files = []
    for root, _, files in os.walk(ROOT_FOLDER):
        for file in files:
            if file.lower().endswith('.txt'):
                txt_files.append(os.path.join(root, file))
    print(f"📄 Found {len(txt_files)} input TXT files.")

    word_pair_freq = defaultdict(lambda: defaultdict(int))

    print("🔄 Processing TXT files to collect word pairs...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_single_file, path, vocab) for path in txt_files]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            local_pairs = future.result()
            merge_word_pairs(word_pair_freq, local_pairs)

    print(f"✅ Frequency extraction complete. Found {len(word_pair_freq)} unique words.")

    print("🧠 Updating predictions...")
    words_to_process = list(word_pair_freq.keys())
    batch_size = 100
    total_processed = 0
    total_skipped = 0

    for i in tqdm(range(0, len(words_to_process), batch_size), desc="Processing word batches"):
        word_batch = words_to_process[i:i + batch_size]
        processed, skipped = process_word_batch(word_batch, word_pair_freq)
        total_processed += processed
        total_skipped += skipped

        for word in word_batch:
            if word in word_pair_freq:
                del word_pair_freq[word]

        if i % (batch_size * 10) == 0:
            gc.collect()
            print(f"🎯 Processed so far: {total_processed} | Skipped: {total_skipped}")

    print(f"\n✅ Finished processing. {total_processed} files updated, {total_skipped} skipped.")
    print(f"📁 All predictions saved in: {OUTPUT_FOLDER}")

    try:
        with open(os.path.join(OUTPUT_FOLDER, "global_tensor_ids.json"), "w", encoding='utf-8') as f:
            json.dump(word_to_tensor_id, f, indent=2)
        print(f"💾 Global tensor IDs saved.")
    except Exception as e:
        print(f"⚠ Error saving global_tensor_ids.json: {e}")

if __name__ == "__main__":
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    try:
        process_local_txt_files()
    except KeyboardInterrupt:
        print("\n⚠ Process interrupted by user.")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
