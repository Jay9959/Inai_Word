import os
import re
import ujson as json
import builtins
from collections import defaultdict
from hashlib import blake2b
from tqdm import tqdm
import gc
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing as mp
import threading
from functools import lru_cache

# orjson વાપરો (જો હોય તો) – 3x ઝડપી JSON
try:
    import orjson
    # orjson સીધો બાઇનરી આઉટપુટ આપે છે, તેથી .decode() નહીં
    json.load = lambda f: orjson.loads(f.read())
    json.dump = lambda obj, f, indent=None: f.buffer.write(
        orjson.dumps(obj, option=orjson.OPT_INDENT_2) if indent else orjson.dumps(obj)
    )
    print("✅ orjson વાપરીને JSON સ્પીડ વધારી.")
except ImportError:
    print("⚠️ orjson નથી, ujson વાપરી રહ્યા છીએ.")

# SpaCy ટોકનાઇઝર (જો હોય તો)
try:
    from spacy.lang.en import English
    nlp = English()
    tokenizer = nlp.tokenizer
    print("✅ SpaCy ટોકનાઇઝર વાપરી રહ્યા છીએ.")
except ImportError:
    print("⚠️ SpaCy નથી. રેગેક્સ વાપરી રહ્યા છીએ. (pip install spacy કરો)")
    tokenizer = None

from datasets import load_dataset

# --- તમારા માર્ગ અને સેટિંગ્સ ---
OUTPUT_FOLDER = r"D:/data/2Connections"
VOCAB_FILE = r"D:\data\output.txt"
GLOBAL_TENSOR_IDS_FILE = os.path.join(OUTPUT_FOLDER, "global_tensor_ids.json")

# ✅ અહીં ફક્ત આ 3 લાઇન બદલો
# --- ડેટાસેટ સેટિંગ્સ ---
HF_DATASET_NAME = "rakshitdabral/gutenberg-tiny"
HF_DATASET_SPLITS = ["train"]  # મોટે ભાગે 'train' જ હોય છે
HF_DATASET_TEXT_COLUMNS = ["text"]  # <--- મુખ્ય કૉલમ "text" છે

# ફાઇલ ઓપરેશન માટે લૉક
file_lock = threading.Lock()

# સાઇન્ટિફિક નોટેશન માટે પેટર્ન
sci_notation_pattern = re.compile(r'^-?\d+(\.\d+)?e[+-]?\d+$', re.IGNORECASE)

# ગ્લોબલ ટેન્સર ID મેપિંગ
word_to_tensor_id = {}
word_tensor_id_lock = threading.Lock()

# બધી પ્રેડિક્શન ફાઇલ્સ RAM માં કેશ
loaded_predictions_cache = {}
predictions_cache_lock = threading.Lock()

# પ્રી-કમ્પાઇલ્ડ ટોકનાઇઝેશન પેટર્ન
TOKEN_REGEX = re.compile(r"\b[a-z']+\b")

# 🔁 ડિટરમિનિસ્ટિક ટેન્સર ID
def generate_global_tensor_id(word):
    with word_tensor_id_lock:
        if word in word_to_tensor_id:
            return word_to_tensor_id[word]
        unique_str = word.encode("utf-8")
        h = blake2b(unique_str, digest_size=6).hexdigest()
        int_val = int(h, 16)
        tensor_id = round((int_val % 99999999) / 100000000.0, 8)
        val_str = str(tensor_id)
        if sci_notation_pattern.match(val_str):
            salt = 0
            while True:
                h_salted = blake2b(unique_str + str(salt).encode(), digest_size=6).hexdigest()
                int_val_salted = int(h_salted, 16)
                candidate = round((int_val_salted % 99999999) / 100000000.0, 8)
                if not sci_notation_pattern.match(str(candidate)):
                    tensor_id = candidate
                    break
                salt += 1
        word_to_tensor_id[word] = tensor_id
        return tensor_id

# 🔍 રિપીટેટિવ શબ્દો ફિલ્ટર કરો – @lru_cache સાથે (ઝડપી)
@lru_cache(maxsize=50000)
def is_repetitive(word):
    """એક જ અક્ષર વારંવાર હોય તો ફિલ્ટર કરો."""
    n = len(word)
    if n > 40: return True
    if n <= 1: return False
    if word == word[0] * n: return True
    if len(set(word)) <= 2 and n > 5 and word.count(word[0]) > n * 0.8: return True
    if any(c in word for c in '<>:"/\\|?*'): return True
    return False

# FIX: Helper function for defaultdict, making it pickleable
def create_nested_defaultdict_int():
    return defaultdict(int)

# 🧹 ટોકનાઇઝ અને જોડીઓ બનાવો
def process_text_entries_batch(entries, vocab):
    local_pairs = defaultdict(create_nested_defaultdict_int)
    for entry in entries:
        try:
            text_parts = []
            for col in HF_DATASET_TEXT_COLUMNS:
                val = entry.get(col)
                if val and isinstance(val, str):
                    text_parts.append(val.lower())
                elif val and isinstance(val, dict) and 'text' in val:
                    text_parts.append(val['text'].lower())
            text_content = " ".join(text_parts)
            if not text_content:
                continue

            if tokenizer:
                words = [token.text for token in tokenizer(text_content) if token.is_alpha or "'" in token.text]
            else:
                words = TOKEN_REGEX.findall(text_content)
            
            for i in range(len(words) - 1):
                w1, w2 = words[i], words[i + 1]
                if w1 in vocab and not is_repetitive(w1) and not is_repetitive(w2):
                    local_pairs[w1][w2] += 1
        except Exception as e:
            continue
    return local_pairs

# 🔗 બે ડિક્શનરી જોડો
def merge_word_pairs(main, local):
    for w1, nexts in local.items():
        for w2, cnt in nexts.items():
            main[w1][w2] += cnt

# 📂 એક ફાઇલ લોડ કરો
def load_single_prediction_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        word = os.path.basename(filepath).replace("_predictions.json", "")
        return (word, data)
    except Exception as e:
        return None

# 📂 બધી પ્રેડિક્શન ફાઇલ્સ RAM માં લોડ કરો
def preload_all_predictions(output_folder):
    print("🔄 બધી પ્રેડિક્શન ફાઇલ્સ RAM માં સમાંતર લોડ કરી રહ્યા છીએ...")
    cache = {}
    file_paths = [os.path.join(output_folder, fname) for fname in os.listdir(output_folder) if fname.endswith("_predictions.json")]
    
    num_threads = min(32, os.cpu_count() * 2)
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(tqdm(executor.map(load_single_prediction_file, file_paths), 
                            total=len(file_paths), 
                            desc="📤 ફાઇલો લોડ કરી રહ્યા છીએ", miniters=100))
    
    count = 0
    for result in results:
        if result:
            word, data = result
            cache[word] = data
            count += 1
    print(f"✅ {count} ફાઇલ્સ લોડ થઈ.")
    return cache

# 💾 એક ફાઇલ સેવ કરો (બાઇનરી મોડમાં)
def save_one_file(args):
    word, predictions = args
    try:
        filename = f"{word}_predictions.json"
        out_file = os.path.join(OUTPUT_FOLDER, filename)
        with file_lock:
            with open(out_file, 'wb') as f:  # 'wb' → બાઇનરી મોડ
                json.dump(predictions, f, indent=2)
        return True
    except Exception as e:
        print(f"❌ {word} માટે ફાઇલ સેવ ના થઈ: {e}")
        return False

# --- મુખ્ય પ્રોસેસિંગ ---
def process_huggingface_dataset_splits():
    global loaded_predictions_cache, word_to_tensor_id

    # global_tensor_ids.json લોડ કરો
    print(f"🔄 {GLOBAL_TENSOR_IDS_FILE} માંથી ટેન્સર ID લોડ કરી રહ્યા છીએ...")
    try:
        if os.path.exists(GLOBAL_TENSOR_IDS_FILE):
            with open(GLOBAL_TENSOR_IDS_FILE, 'r', encoding='utf-8') as f:
                loaded_ids = json.load(f)
                word_to_tensor_id.update(loaded_ids)
            print(f"✅ {len(word_to_tensor_id)} હાલના ટેન્સર ID લોડ થયા.")
        else:
            print("ℹ️ global_tensor_ids.json મળી નથી, શરૂઆતથી બનાવી રહ્યા છીએ.")
    except Exception as e:
        print(f"❌ global_tensor_ids.json લોડ કરવામાં ભૂલ: {e}")
        word_to_tensor_id.clear()

    # vocab.txt લોડ કરો
    print("📚 vocab.txt લોડ કરીને ટેન્સર ID જનરેટ કરી રહ્યા છીએ...")
    vocab = set()
    try:
        with open(VOCAB_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                word = line.strip().lower()
                if word:
                    vocab.add(word)
                    generate_global_tensor_id(word)
        print(f"✅ {len(vocab)} શબ્દો લોડ થયા.")
    except FileNotFoundError:
        print(f"❌ {VOCAB_FILE} નથી મળતી.")
        return

    # પ્રેડિક્શન ફાઇલ્સ પ્રી-લોડ
    loaded_predictions_cache = preload_all_predictions(OUTPUT_FOLDER)

    discovered_splits = HF_DATASET_SPLITS
    print(f"📌 ડેટાસેટ: {HF_DATASET_NAME}, સ્પ્લિટ: {discovered_splits}")

    num_cpus = mp.cpu_count()
    data_workers = max(1, num_cpus - 1)
    write_workers = min(16, num_cpus * 2)

    for current_split in discovered_splits:
        print(f"\n--- 🚀 પ્રોસેસિંગ: {current_split} ---")
        try:
            dataset = load_dataset(HF_DATASET_NAME, split=current_split, streaming=True)
            print("✅ ડેટાસેટ સ્ટ્રીમિંગ મોડમાં લોડ થઈ.")
        except Exception as e:
            print(f"❌ લોડ ના થઈ: {e}")
            continue

        word_pair_freq = defaultdict(create_nested_defaultdict_int)
        total_entries = 0
        batch_size = 10000
        buffer = []

        with ProcessPoolExecutor(max_workers=data_workers) as executor:
            futures = []
            for entry in tqdm(dataset, desc="🔤 ડેટા એકત્રિત કરી રહ્યા છીએ", miniters=1000):
                buffer.append(entry)
                if len(buffer) >= batch_size * data_workers:  # chunksize સુધારો
                    chunked = [buffer[i:i + batch_size] for i in range(0, len(buffer), batch_size)]
                    for chunk in chunked:
                        futures.append(executor.submit(process_text_entries_batch, chunk, vocab))
                    buffer = []

                    # ભવિષ્ય પૂર્ણ થાય ત્યારે મર્જ કરો
                    if len(futures) >= data_workers * 3:
                        completed = as_completed(futures, timeout=None).__next__()
                        local = completed.result()
                        futures.remove(completed)
                        merge_word_pairs(word_pair_freq, local)
                        total_entries += batch_size

            # બાકીના બફર અને ફ્યુચર્સ
            if buffer:
                futures.append(executor.submit(process_text_entries_batch, buffer, vocab))
            for future in as_completed(futures):
                local = future.result()
                merge_word_pairs(word_pair_freq, local)
                total_entries += batch_size

            # ફક્ત જરૂર પડે ત્યારે જ gc
            if total_entries % 50000 == 0:
                gc.collect()

        print(f"📊 પેર ફ્રિક્વન્સી પૂરી. ~{total_entries} એન્ટ્રીઝ, {len(word_pair_freq)} શબ્દો.")

        # પ્રેડિક્શન અપડેટ કરો
        final_updates = {}
        words = list(word_pair_freq.keys())
        for word in tqdm(words, desc="🔄 પ્રેડિક્શન તૈયાર કરી રહ્યા છીએ"):
            if is_repetitive(word) or len(f"{word}_predictions.json") > 200:
                continue

            predictions = loaded_predictions_cache.get(word, {})
            updated = False

            for next_word in word_pair_freq[word]:
                tid = generate_global_tensor_id(next_word)
                if predictions.get(next_word) != tid:
                    predictions[next_word] = tid
                    updated = True

            if updated:
                sorted_preds = {
                    k: v for k, v in sorted(predictions.items(), key=lambda x: -x[1])
                    if len(k) <= 20
                }
                final_updates[word] = sorted_preds
                loaded_predictions_cache[word] = sorted_preds

        # ફાઇલ્સ સેવ કરો
        print(f"💾 {len(final_updates)} ફાઇલ્સ સેવ કરી રહ્યા છીએ...")
        with ThreadPoolExecutor(max_workers=write_workers) as writer:
            list(tqdm(
                writer.map(save_one_file, final_updates.items()),
                total=len(final_updates),
                desc="📤 ફાઇલ્સ સેવ કરી રહ્યા છીએ"
            ))

        print(f"✅ {current_split} માટે પ્રોસેસિંગ પૂરી.")

    # global_tensor_ids.json સેવ કરો
    try:
        with open(GLOBAL_TENSOR_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(word_to_tensor_id, f, indent=2)
        print("✅ global_tensor_ids.json સફળતાપૂર્વક અપડેટ થઈ.")
    except Exception as e:
        print(f"❌ global_tensor_ids.json સેવ કરવામાં ભૂલ: {e}")

# --- મુખ્ય ફંક્શન ---
if __name__ == "__main__":
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    try:
        process_huggingface_dataset_splits()
    except KeyboardInterrupt:
        print("\n🛑 યુઝરે બંધ કર્યું.")
    except Exception as e:
        print(f"\n💥 એરર: {e}")
        import traceback
        traceback.print_exc()