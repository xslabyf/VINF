import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
import pandas as pd

spark = SparkSession.builder.appName("WikiXMLprocessing").getOrCreate()

wiki_xml = "D:/enwiki-latest-pages-articles.xml"
wiki_df = spark.read. format("xml").option("rowTag", "page").load(wiki_xml) \
    .select(col("title"), col("revision.text._VALUE").alias("text"))

wiki_df = wiki_df.withColumn(
    "title_norm",
    lower(regexp_replace(col("title"), r"\s+\(.*\)$", ""))
)

drivers_df = spark.read.csv("drivers.csv", header=True, inferSchema=True) \
    .withColumn(
    "driver_name_norm",
    lower(regexp_replace(col("driver_name"), r"\s+\(.*\)$", ""))
)

joined_raw = drivers_df.join(
    wiki_df,
    drivers_df.driver_name_norm == wiki_df.title_norm,
    "inner"
)

joined_filtered = joined_raw. filter(
    col("text").isNotNull() &
    lower(col("text")).rlike("(racing driver|race car|formula one|nascar|indycar|motogp|wrc|karting)")
)

pdf = joined_filtered.toPandas()


def extract_infobox(text):
    if not isinstance(text, str):
        return ""

    m = re.search(r"\{\{Infobox", text, re.IGNORECASE)
    if not m:
        return ""

    start = m.start()

    sec = re.search(r"\n={2,}.*? ={2,}", text[start:])
    end_limit = start + sec.start() if sec else len(text)

    depth = 0
    i = start
    while i < end_limit:
        if text[i: i + 2] == "{{":
            depth += 1
            i += 2
        elif text[i:i + 2] == "}}":
            depth -= 1
            i += 2
            if depth == 0:
                return text[start: i]
        else:
            i += 1

    return text[start:end_limit]


print("Extracting infoboxes including nested templates...")
pdf["infobox_full"] = pdf["text"]. apply(extract_infobox)


def extract_all_fields(infobox_text):
    if not isinstance(infobox_text, str):
        return {}

    fields = {}

    pattern = r"^\s*\|\s*([A-Za-z0-9_()]+?)\s*=\s*(.*? )\s*$"

    for line in infobox_text. split("\n"):
        m = re.match(pattern, line. strip())
        if not m:
            continue
        key, value = m.groups()
        value = value.strip()
        if value == "":
            continue

        key_norm = key.strip().lower()
        fields[key_norm] = value

    return fields


pdf["fields"] = pdf["infobox_full"].apply(extract_all_fields)


def get_field(d, names):
    for n in names:
        n_norm = n. lower()
        if n_norm in d:
            return d[n_norm]
    return ""


pdf["teams_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["teams", "team", "current_team", "team(s)"])
)

pdf["birth_place_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["birth_place"])
)
pdf["nationality_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["nationality"])
)
pdf["birth_date_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["birth_date", "born"])
)

pdf["championships_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["championships"])
)
pdf["wins_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["wins", "win"])
)
pdf["entries_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["entries", "races", "starts"])
)
pdf["podiums_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["podiums", "podium finishes"])
)
pdf["car_number_wiki"] = pdf["fields"]. apply(
    lambda d: get_field(d, ["car_number", "car number"])
)
pdf["fastest_laps_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["fastest_laps", "fastest laps"])
)
pdf["pol_positions_wiki"] = pdf["fields"].apply(
    lambda d: get_field(d, ["pole positions", "poles"])
)


def clean_value(v):
    if not isinstance(v, str):
        return ""
    v = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", v)
    v = re.sub(r"\[\[([^\]]+)\]\]", r"\1", v)
    v = re.sub(r"\{\{[^{}]+\}\}", "", v)
    v = re.sub(r"<.*?>", "", v)
    v = " ".join(v.replace("|", " ").split())
    return v


for c in [
    "teams_wiki",
    "birth_place_wiki",
    "nationality_wiki",
    "wins_wiki",
    "entries_wiki",
    "podiums_wiki",
    "championships_wiki",
    "car_number_wiki",
    "fastest_laps_wiki",
    "pol_positions_wiki",
]:
    pdf[c] = pdf[c].apply(clean_value)


def clean_birth_date(v):
    if not isinstance(v, str):
        return ""
    m = re.search(r"(\d{4}).?(\d{1,2}).?(\d{1,2})", v)
    if m:
        y, mth, d = m.groups()
        return f"{y}-{mth}-{d}"
    return clean_value(v)


pdf["birth_date_wiki"] = pdf["birth_date_wiki"].apply(clean_birth_date)

num_cols = [
    "championships_wiki",
    "wins_wiki",
    "entries_wiki",
    "podiums_wiki",
    "car_number_wiki",
    "fastest_laps_wiki",
    "pol_positions_wiki",
]

for c in num_cols:
    pdf[c] = pdf[c].astype(str).str.extract(r"(\d+)", expand=False)


def extract_first_1000_words(text):
    if not isinstance(text, str):
        return ""

    cleaned = re.sub(r"\{\{Infobox.*?\}\}", "", text, flags=re. DOTALL | re.IGNORECASE)

    while re.search(r"\{\{[^{}]*\}\}", cleaned):
        cleaned = re.sub(r"\{\{[^{}]*\}\}", "", cleaned)

    cleaned = re.sub(r"<[^>]+>", "", cleaned)

    cleaned = re.sub(r"<ref[^>]*>.*?</ref>", "", cleaned, flags=re. DOTALL)
    cleaned = re.sub(r"<ref[^>]*/>", "", cleaned)

    cleaned = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", cleaned)
    cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", cleaned)

    cleaned = re.sub(r"\[https?://[^\]]+\]", "", cleaned)

    cleaned = re.sub(r"={2,}.*?={2,}", "", cleaned)

    cleaned = re.sub(r"\|[\w\s]*=", "", cleaned)
    cleaned = re.sub(r"\|-", "", cleaned)
    cleaned = re.sub(r"\|\+", "", cleaned)
    cleaned = re.sub(r"^\|", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"! !", "", cleaned)
    cleaned = re.sub(r"\bcolspan\b", "", cleaned)
    cleaned = re.sub(r"\browspan\b", "", cleaned)
    cleaned = re.sub(r"\bnowrap\b", "", cleaned)
    cleaned = re.sub(r"\bstyle\b", "", cleaned)
    cleaned = re.sub(r"\bbackground\b", "", cleaned)
    cleaned = re.sub(r"\bnbsp\b", "", cleaned)

    cleaned = re.sub(r"Category: .*", "", cleaned)

    cleaned = re.sub(r"\[\[File:.*?\]\]", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\[\[Image:.*?\]\]", "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\b(thumb|thumbnail|left|right|center|frame)\b", "", cleaned)

    cleaned = re.sub(r"'{2,}", "", cleaned)

    words = re.findall(r"\b[A-Za-z][A-Za-z0-9\-']*\b", cleaned)

    words = [w for w in words if len(w) >= 2]

    first_1000 = words[:1000]

    return " ".join(first_1000)


pdf["first_1000_words"] = pdf["text"].apply(extract_first_1000_words)

clean = pdf.drop(
    columns=["infobox_full", "fields", "text", "driver_name_norm", "title_norm"],
    errors="ignore",
)

clean.to_csv("final_gg.csv", index=False)
print("DONE - saved to final_gg.csv")

spark.stop()