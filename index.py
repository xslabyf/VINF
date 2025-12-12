import os
import re
import math
from collections import defaultdict
import csv


def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def strip_tags(text):
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&amp;", "&").replace("&nbsp;", " ").replace("&quot;", '"').strip()
    return " ".join(text.split())

def extract_teams_from_section(html, section_id):
    section_pattern = rf'<section[^>]+id="{re.escape(section_id)}"[^>]*>(.*?)</section>'
    section_match = re.search(section_pattern, html, re.DOTALL | re.IGNORECASE)

    if not section_match:
        return []

    section_html = section_match.group(1)
    teams = []

    if section_id == "championship-seasons":
        rows = re.findall(r'<tr[^>]*class="database-table__row"[^>]*>(.*? )</tr>', section_html, re.DOTALL)

        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*? )</td>', row, re.DOTALL)

            if len(tds) >= 2:
                entrant_td = tds[1]
                link_match = re.search(r'<a[^>]*>(.*?)</a>', entrant_td, re.DOTALL)
                if link_match:
                    team_name = strip_tags(link_match.group(1))
                    if team_name and team_name not in teams:
                        teams.append(team_name)

    elif section_id == "non-championship-races":
        rows = re.findall(r'<tr[^>]*class="database-table__row"[^>]*>(.*?)</tr>', section_html, re.DOTALL)

        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

            if len(tds) >= 4:
                entrant_td = tds[3]

                link_match = re.search(r'<a[^>]*>(.*?)</a>', entrant_td, re.DOTALL)
                if link_match:
                    team_name = strip_tags(link_match.group(1))
                    if team_name and team_name not in teams:
                        teams.append(team_name)

    elif section_id == "teams":
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', section_html, re.DOTALL)

        for row in rows:
            if '<th' in row:
                continue

            link_match = re.search(r'<a[^>]*>(.*?)</a>', row, re.DOTALL)
            if link_match:
                team_name = strip_tags(link_match.group(1))
                if team_name and team_name not in teams:
                    teams.append(team_name)

    return teams


def extract_all_teams(html):
    all_teams = []

    for section in ["championship-seasons", "non-championship-races", "teams"]:
        section_teams = extract_teams_from_section(html, section)
        for team in section_teams:
            if team not in all_teams:
                all_teams.append(team)

    return all_teams


def extract_driver_info(html_file):
    html = read_file(html_file)
    driver_info = {}

    name_match = re.search(r'<h1[^>]*class="[^"]*hero__title[^"]*"[^>]*>(.*?)</h1>', html, re.DOTALL)
    if not name_match:
        name_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    driver_info['name'] = strip_tags(name_match.group(1)) if name_match else ""

    birth_match = re.search(
        r'<div[^>]*class="[^"]*database-data__head[^"]*"[^>]*>Born</div>\s*<div[^>]*class="[^"]*database-data__content[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if birth_match:
        birth_text = strip_tags(birth_match.group(1))
        parts = birth_text.split()
        driver_info['birth_date'] = " ".join(parts[:3]) if len(parts) >= 3 else birth_text
    else:
        driver_info['birth_date'] = ""

    nationality_match = re.search(
        r'<div[^>]*class="[^"]*database-data__content[^"]*database-data__icon[^"]*"[^>]*>.*?<img[^>]*alt="([^"]+)"[^>]*>',
        html, re.DOTALL
    )
    driver_info['nationality'] = nationality_match.group(1).strip() if nationality_match else ""

    for stat in ["starts", "wins", "poles", "podiums"]:
        driver_info[stat] = ""

    stats_pattern = r'<div[^>]*class="[^"]*database-data__head[^"]*"[^>]*>(Starts|Wins|Poles|Podiums)</div>\s*<div[^>]*class="[^"]*database-data__content[^"]*"[^>]*>(.*?)</div>'
    stat_matches = re.findall(stats_pattern, html, re.DOTALL | re.IGNORECASE)

    for stat_name, value in stat_matches:
        driver_info[stat_name.lower()] = strip_tags(value)

    teams_list = extract_all_teams(html)
    driver_info['teams'] = "; ".join(teams_list)

    return driver_info

def tokenize(text):
    return re.findall(r'\b[a-zA-Z0-9]+\b', (text or "").lower())


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
    extracted = {}

    print(f"Checking folder: {os.path.abspath(folder_path)}")

    if not os.path.exists(folder_path):
        print(f"ERROR: Folder does not exist!")
        return

    all_files = os.listdir(folder_path)
    html_files = [f for f in all_files if f.endswith(".html")]

    print(f"Total files: {len(all_files)}")
    print(f"HTML files: {len(html_files)}")

    if not html_files:
        print("ERROR: No HTML files found!")
        return

    print(f"First 3 HTML files: {html_files[:3]}\n")
    print(f"Processing HTML files.. .\n")

    for filename in html_files:
        path = os.path.join(folder_path, filename)
        try:
            info = extract_driver_info(path)

            documents[filename] = " ".join([
                info.get("name", ""),
                info.get("birth_date", ""),
                info.get("nationality", ""),
                info.get("teams", ""),
                info.get("starts", ""),
                info.get("wins", ""),
                info.get("poles", ""),
                info.get("podiums", ""),
            ])

            extracted[filename] = info
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    print(f"Processed {len(extracted)} drivers\n")

    csv_file = "drivers_teams.csv"
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "driver_id", "driver_name", "birth_date", "nationality",
            "teams", "starts", "wins", "poles", "podiums"
        ])

        for file, info in extracted.items():
            writer.writerow([
                file,
                info.get("name", ""),
                info.get("birth_date", ""),
                info.get("nationality", ""),
                info.get("teams", ""),
                info.get("starts", ""),
                info.get("wins", ""),
                info.get("poles", ""),
                info.get("podiums", ""),
            ])

    print(f"CSV file created: {csv_file}\n")

    tf_index = compute_tf(documents)
    total_docs = len(documents)
    idf_basic = compute_idf_classic(tf_index, total_docs)
    idf_smooth = compute_idf_probabilistic(tf_index, total_docs)

    tfidf_basic = compute_tfidf(tf_index, idf_basic)
    tfidf_smooth = compute_tfidf(tf_index, idf_smooth)

    for q in queries:
        print(f"\nQuery: '{q}'")
        print("\nIDF Classic:")
        results_basic = query_tfidf(q, tfidf_basic)
        for doc, score in results_basic[:5]:
            driver_name = extracted.get(doc, {}).get("name", doc)
            print(f"  {driver_name}: {score:.4f}")

        print("\nIDF Probabilistic:")
        results_smooth = query_tfidf(q, tfidf_smooth)
        for doc, score in results_smooth[:5]:
            driver_name = extracted.get(doc, {}).get("name", doc)
            print(f"  {driver_name}: {score:.4f}")



if __name__ == "__main__":
    folder = "data/html"
    queries = ["Ferrari driver named Peter", "Jaguar driver who is Belgian", "British driver", "1987"]
    build_index(folder, queries)