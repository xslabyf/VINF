import lucene

lucene.initVM()

import csv
from java.nio.file import Paths
from org.apache.lucene.store import MMapDirectory
from org.apache. lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene. analysis.miscellaneous import PerFieldAnalyzerWrapper
from org.apache.lucene.document import (
    Document,
    Field,
    TextField,
    StringField,
    StoredField,
    IntPoint,
    LongPoint,
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from java.util import HashMap
from datetime import datetime

CSV_FILE = "finalny_complete.csv"
INDEX_DIR = "index"

FULLTEXT_FIELDS_WITH_FALLBACK = {
    "birth_place_wiki":  "birth_place",
    "nationality_wiki": "nationality",
}

FULLTEXT_FIELDS = [
    "driver_name",
    "teams_wiki",
    "former_teams_wiki",
    "car_number_wiki",
    "birth_date_wiki",
    "birth_date",
    "biography",
]

STRING_FIELDS = [
    "driver_id",
]

NUMERIC_FIELDS_WITH_FALLBACK = {
    "wins":  "wins_wiki",
    "podiums": "podiums_wiki",
    "poles": "pol_positions_wiki",
    "starts": "entries_wiki",
    "fastest_laps": "fastest_laps_wiki",
}


def parse_birth_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return 0

    date_str = date_str.strip()

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y",
    ]

    for fmt in formats:
        try:
            dt = datetime. strptime(date_str. split()[0], fmt)
            return int(dt.timestamp())
        except:
            pass

    import re
    year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
    if year_match:
        try:
            year = int(year_match.group())
            dt = datetime(year, 1, 1)
            return int(dt.timestamp())
        except:
            pass

    return 0


def get_text_value(row, base_field, wiki_field):
    if wiki_field:
        value = row.get(wiki_field, "").strip()
        if value:
            return value

    value = row. get(base_field, "").strip()
    if value:
        return value

    return None


def get_numeric_value(row, base_field, wiki_field):
    if wiki_field:
        value = row.get(wiki_field, "").strip()
        if value:
            try:
                return int(float(value))
            except (ValueError, TypeError):
                pass

    value = row.get(base_field, "").strip()
    if value:
        try:
            return int(float(value))
        except (ValueError, TypeError):
            pass

    return 0


def create_index():
    standard_analyzer = StandardAnalyzer()
    whitespace_analyzer = WhitespaceAnalyzer()

    analyzer_map = HashMap()
    analyzer_map.put("birth_date_wiki", whitespace_analyzer)
    analyzer_map.put("birth_date", whitespace_analyzer)

    analyzer = PerFieldAnalyzerWrapper(standard_analyzer, analyzer_map)

    index_dir = MMapDirectory(Paths.get(INDEX_DIR))
    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(index_dir, config)

    indexed_count = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            doc = Document()

            for wiki_field, base_field in FULLTEXT_FIELDS_WITH_FALLBACK.items():
                value = get_text_value(row, base_field, wiki_field)
                if value:
                    doc.add(TextField(wiki_field, value, Field.Store.YES))

            for field in FULLTEXT_FIELDS:
                csv_field = "first_1000_words" if field == "biography" else field
                value = row.get(csv_field)
                if value and value.strip():
                    doc.add(TextField(field, value, Field.Store.YES))

            for field in STRING_FIELDS:
                value = row.get(field)
                if value and value.strip():
                    doc.add(StringField(field, value, Field.Store.YES))

            for base_field, wiki_field in NUMERIC_FIELDS_WITH_FALLBACK.items():
                num = get_numeric_value(row, base_field, wiki_field)
                doc.add(IntPoint(base_field, num))
                doc.add(StoredField(base_field, num))

            championships_value = row.get("championships_wiki", "").strip()
            if championships_value:
                try:
                    championships = int(float(championships_value))
                except (ValueError, TypeError):
                    championships = 0
            else:
                championships = 0

            doc. add(IntPoint("championships", championships))
            doc.add(StoredField("championships", championships))

            for base_field, wiki_field in NUMERIC_FIELDS_WITH_FALLBACK.items():
                num = get_numeric_value(row, base_field, wiki_field)
                if num > 0:
                    stats_text = f"{num} {base_field}"
                    field_name = f"{base_field}_text"
                    doc.add(TextField(field_name, stats_text, Field.Store.YES))

            if championships > 0:
                champ_text = f"{championships} championships"
                doc.add(TextField("championships_text", champ_text, Field.Store.YES))

            birth_date_str = row.get("birth_date_wiki") or row.get("birth_date")
            birth_timestamp = parse_birth_date(birth_date_str)
            doc.add(LongPoint("birth_timestamp", birth_timestamp))
            doc.add(StoredField("birth_timestamp", birth_timestamp))

            indexed_fields = (
                    list(FULLTEXT_FIELDS_WITH_FALLBACK.keys()) +
                    list(FULLTEXT_FIELDS_WITH_FALLBACK.values()) +
                    FULLTEXT_FIELDS +
                    STRING_FIELDS +
                    list(NUMERIC_FIELDS_WITH_FALLBACK.keys()) +
                    [v for v in NUMERIC_FIELDS_WITH_FALLBACK.values() if v] +
                    ["birth_timestamp", "championships", "championships_wiki", "first_1000_words",
                     "wins_text", "podiums_text", "poles_text", "starts_text", "fastest_laps_text",
                     "championships_text"]
            )

            for key, value in row.items():
                if key not in indexed_fields:
                    if value is not None and value != "":
                        doc.add(StoredField(key, value))

            writer.addDocument(doc)
            indexed_count += 1

    writer.close()
    print(f"Index created successfully!  Indexed {indexed_count} drivers.")


if __name__ == "__main__":
    create_index()