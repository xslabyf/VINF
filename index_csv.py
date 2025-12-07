import lucene

lucene.initVM()

import csv
from java.nio.file import Paths
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
from org.apache.lucene.document import (
    Document,
    Field,
    TextField,
    StringField,
    StoredField,
    IntPoint,
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from java.util import HashMap

CSV_FILE = "finalny_merged.csv"
INDEX_DIR = "index"


FULLTEXT_FIELDS_WITH_FALLBACK = {
    "birth_place_wiki": "birth_place",
    "nationality_wiki": "nationality",
}


FULLTEXT_FIELDS = [
    "driver_name",
    "teams_wiki",
    "former_teams_wiki",
    "car_number_wiki",
    "birth_date_wiki",
    "birth_date",
]


STRING_FIELDS = [
    "driver_id",
]


NUMERIC_FIELDS_WITH_FALLBACK = {
    "wins": "wins_wiki",
    "podiums": "podiums_wiki",
    "poles": None,
    "starts": "entries_wiki",
    "fastest_laps": "fastest_laps_wiki",
}


def get_text_value(row, base_field, wiki_field):

    if wiki_field:
        value = row.get(wiki_field, "").strip()
        if value:
            return value


    value = row.get(base_field, "").strip()
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

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            doc = Document()

            for wiki_field, base_field in FULLTEXT_FIELDS_WITH_FALLBACK.items():
                value = get_text_value(row, base_field, wiki_field)
                if value:
                    doc.add(TextField(wiki_field, value, Field.Store.YES))

            for field in FULLTEXT_FIELDS:
                value = row.get(field)
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

            indexed_fields = (
                    list(FULLTEXT_FIELDS_WITH_FALLBACK.keys()) +
                    list(FULLTEXT_FIELDS_WITH_FALLBACK.values()) +
                    FULLTEXT_FIELDS +
                    STRING_FIELDS +
                    list(NUMERIC_FIELDS_WITH_FALLBACK.keys()) +
                    [v for v in NUMERIC_FIELDS_WITH_FALLBACK.values() if v]
            )

            for key, value in row.items():
                if key not in indexed_fields:
                    if value is not None and value != "":
                        doc.add(StoredField(key, value))

            writer.addDocument(doc)

    writer.close()
    print("Index created successfully.")


if __name__ == "__main__":
    create_index()