import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_extract, regexp_replace, trim
import pandas as pd

spark = SparkSession.builder.appName("WikiXMLprocessing").getOrCreate()

# ---------- 1. LOAD XML ----------
wiki_xml = "enwiki-latest-pages-articles10.xml-p4045403p5399366/enwiki-latest-pages-articles10.xml-p4045403p5399366"
wiki_df = spark.read.format("xml").option("rowTag", "page").load(wiki_xml) \
    .select(col("title"), col("revision.text._VALUE").alias("text"))

wiki_df = wiki_df.withColumn("title_norm", lower(regexp_replace(col("title"), r"\s+\(.*\)$", "")))

# ---------- 3. LOAD drivers ----------
drivers_df = spark.read.csv("drivers.csv", header=True, inferSchema=True) \
    .withColumn("driver_name_norm", lower(regexp_replace(col("driver_name"), r"\s+\(.*\)$", "")))

# ---------- 4. JOIN ----------
joined_raw = drivers_df.join(wiki_df,
                             drivers_df.driver_name_norm == wiki_df.title_norm,
                             "inner")

# ---------- 5. FILTER ----------
joined_filtered = joined_raw.filter(
    col("text").isNotNull() &
    lower(col("text")).rlike("(racing driver|race car|formula one|nascar|indycar|motogp|wrc|karting)")
)

# ---------- 6. EXTRAKCIA INFOBOXU ----------
joined_infobox = joined_filtered.withColumn(
    "infobox_body",
    regexp_extract(col("text"), r"((?s)\{\{Infobox[^}]*\n((?:[^{}]|\{\{.*?\}\})*)\n\}\}", 0)
)

# ---------- 7. EXTRAKCIA POLÍ ----------
def extract(field):
    return f"(?mi)^\\|\\s*{field}\\s*=\\s*([^\\n]+)"

fields = {
    # FULL-LINE EXTRACT (DOBRE)
    "birth_date_wiki": r"(?mi)^\|\s*(?:birth_date|born)\s*=\s*(?!\s*$)(.+)",
    "teams_wiki": r"(?mi)^\|\s*(?:team|teams|current_team|teams(s))\s*=\s*(?!\s*$)(?!.*Championships)(.*?)\s*(?=\n\|)",
    "championships_wiki": r"(?mi)^\|\s*(?:championships)\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",
    "car_number_wiki": r"(?mi)^\|\s*(?:car_number|car number)\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",
    "entries_wiki": r"(?mi)^\|\s*(?:entries|starts|races)\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",
    "wins_wiki": r"(?mi)^\|\s*wins\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",
    "podiums_wiki": r"(?mi)^\|\s*(?:podiums|podium finishes)\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",
    "fastest_laps_wiki": r"(?mi)^\|\s*(?:fastest_laps|fastest laps)\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",
    "pol_positions_wiki": r"(?mi)^\|\s*(?:pole positions|poles)\s*=\s*(?!\s*$)(.*?)\s*(?=\n\|)",


    # Ostatné nechávame tak
    "birth_place_wiki": extract("birth_place"),
    "nationality_wiki": extract("nationality"),
    "former_teams_wiki": extract("(former_teams|former team)")
}

for colname, pattern in fields.items():
    joined_infobox = joined_infobox.withColumn(colname, regexp_extract(col("infobox_body"), pattern, 1))

# ---------- 8. Spark → Pandas ----------
pdf = joined_infobox.toPandas()

# ---------- 9. ČISTENIE birth_place + nationality ----------
def clean_value(v):
    if pd.isna(v):
        return ""
    v = str(v)
    v = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", v)
    v = re.sub(r"\[\[([^\]]+)\]\]", r"\1", v)
    v = re.sub(r"\{\{[^{}]+\}\}", "", v)
    v = re.sub(r"<.*?>", "", v)
    v = v.replace("|", " ")
    v = " ".join(v.split())
    return v

for c in ["birth_place_wiki", "nationality_wiki"]:
    pdf[c] = pdf[c].apply(clean_value)

# ---------- 9B. ČISTENIE teams ----------
def clean_teams(v):
    if pd.isna(v):
        return ""
    v = str(v)
    v = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", v)
    v = re.sub(r"\[\[([^\]]+)\]\]", r"\1", v)
    v = v.replace("<br>", ", ").replace("<br/>", ", ").replace("<br />", ", ")
    v = " ".join(v.split())
    return v

pdf["teams_wiki"] = pdf["teams_wiki"].apply(clean_teams)

# ---------- 9C. BIRTH_DATE ----------
def clean_birth_date(v):
    if pd.isna(v):
        return ""
    v = str(v)
    m = re.search(r"(\d{4}).?(\d{1,2}).?(\d{1,2})", v)
    if m:
        y, mth, d = m.groups()
        return f"{y}-{mth}-{d}"
    return re.sub(r"[\{\}\|]", " ", v)

pdf["birth_date_wiki"] = pdf["birth_date_wiki"].apply(clean_birth_date)

# ---------- 10. NUMERIC extraction ----------
num_cols = ["championships_wiki", "wins_wiki", "entries_wiki", "podiums_wiki", "car_number_wiki"]

for c in num_cols:
    pdf[c] = pdf[c].astype(str).str.extract(r"(\d+)")

# ---------- 11. DROP ----------
clean = pdf.drop(
    columns=["infobox", "infobox_body", "text", "driver_name_norm", "title_norm"],
    errors="ignore"
)

clean.to_csv("pls.csv", index=False)

print("DONE — uložené ako combined_clean.csv")
spark.stop()
