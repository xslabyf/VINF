import os
import re
import math
from collections import defaultdict
import csv   # <-- PRIDANÉ


def extract_driver_info(html_file):
    with open(html_file, "r", encoding="utf-8") as file:
        html = file.read()
    driver_info = {}
    name_match = re.search(r'<h1 class="hero__title sanomat">(.*?)</h1>', html, re.DOTALL)
    driver_info['name'] = name_match.group(1).strip() if name_match else ""

    birth_match = re.search(r'Born</div><div class="database-data__content">(.*?)</div>', html, re.DOTALL)
    if birth_match:
        birth = birth_match.group(1).strip().split(" ")
        driver_info['birth_date'] = " ".join(birth[:3])
    else:
        driver_info['birth_date'] = ""

    nationality_match = re.search(
        r'<div class="database-data__content database-data__icon">.*?<img.*?alt="(.*?)".*?</div>',
        html, re.DOTALL
    )
    driver_info['nationality'] = nationality_match.group(1).strip() if nationality_match else ""

    stats_regex = re.compile(
        r'<div class="database-data__head">(Starts|Wins|Poles|Podiums)</div>'
        r'<div class="database-data__content">(.*?)</div>',
        re.DOTALL
    )
    stat_matches = stats_regex.findall(html)
    for stat, value in stat_matches:
        driver_info[stat.lower()] = value.strip()

    return driver_info


def tokenize(text):
    return re.findall(r'\b[a-zA-Z0-9]+\b', text.lower())


def compute_tf(documents):
    index = defaultdict(lambda: defaultdict(int))
    for doc_id, text in documents.items():
        tokens = tokenize(text)
        for term in tokens:
            index[term][doc_id] += 1
    return index


def compute_idf_classic(tf_index, total_docs):
    idf = {}
    for term, docs in tf_index.items():
        df = len(docs)
        if df > 0:
            idf[term] = math.log(total_docs / df)
        else:
            idf[term] = 0
    return idf


def compute_idf_probabilistic(tf_index, total_docs):
    idf = {}
    for term, docs in tf_index.items():
        df = len(docs)
        if 0 < df < total_docs:
            idf[term] = math.log((total_docs - df) / df)
        else:
            idf[term] = 0
    return idf


def compute_tfidf(tf_index, idf):
    tfidf = defaultdict(lambda: defaultdict(float))
    for term, doc_counts in tf_index.items():
        for doc, tf in doc_counts.items():
            tfidf[term][doc] = tf * idf.get(term, 0)
    return tfidf


def query_tfidf(query, tfidf_index):
    query_terms = tokenize(query)
    scores = defaultdict(float)
    for term in query_terms:
        for doc, weight in tfidf_index.get(term, {}).items():
            scores[doc] += weight
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def build_index(folder_path, queries):
    documents = {}
    extracted = {}   # <-- bude obsahovať info pre CSV

    for filename in os.listdir(folder_path):
        if filename.endswith(".html"):
            path = os.path.join(folder_path, filename)
            driver_info = extract_driver_info(path)

            # uloženie pre TF-IDF
            documents[filename] = " ".join(driver_info.values())

            # uloženie pre CSV
            extracted[filename] = driver_info

    # --- GENERATE CSV ---
    with open("drivers.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["driver_id", "driver_name", "birth_date", "nationality", "starts", "wins", "poles", "podiums"])

        for file, info in extracted.items():
            writer.writerow([
                file,
                info.get("name", ""),
                info.get("birth_date", ""),
                info.get("nationality", ""),
                info.get("starts", ""),
                info.get("wins", ""),
                info.get("poles", ""),
                info.get("podiums", ""),
            ])

    print("Súbor drivers.csv bol vytvorený!")

    tf_index = compute_tf(documents)
    total_docs = len(documents)
    idf_basic = compute_idf_classic(tf_index, total_docs)
    idf_smooth = compute_idf_probabilistic(tf_index, total_docs)

    tfidf_basic = compute_tfidf(tf_index, idf_basic)
    tfidf_smooth = compute_tfidf(tf_index, idf_smooth)

    for q in queries:
        print(f"\n Query: '{q}'")
        print("\n Výsledky s IDF Classic:")
        results_basic = query_tfidf(q, tfidf_basic)
        for doc, score in results_basic[:5]:
            print(f"  {doc}: {score:.4f}")

        print("\n Výsledky s IDF Probabilistic:")
        results_smooth = query_tfidf(q, tfidf_smooth)
        for doc, score in results_smooth[:5]:
            print(f"  {doc}: {score:.4f}")


if __name__ == "__main__":
    folder = "data/html"
    queries = [
        "Patrick",
        "Italian",
        "1987"
    ]
    build_index(folder, queries)
