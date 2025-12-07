import lucene

lucene.initVM()

import re
from java.nio.file import Paths
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher, Sort, SortField, BooleanQuery, BooleanClause, BoostQuery
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
from org.apache.lucene.document import IntPoint
from java.util import HashMap

INDEX_DIR = "index"


class F1SearchCLI:
    def __init__(self):
        index_dir = MMapDirectory(Paths.get(INDEX_DIR))
        self.reader = DirectoryReader.open(index_dir)
        self.searcher = IndexSearcher(self.reader)

        standard_analyzer = StandardAnalyzer()

        whitespace_analyzer = WhitespaceAnalyzer()

        analyzer_map = HashMap()
        analyzer_map.put("birth_date_wiki", whitespace_analyzer)
        analyzer_map.put("birth_date", whitespace_analyzer)

        self.analyzer = PerFieldAnalyzerWrapper(standard_analyzer, analyzer_map)

        self.text_fields = [
            "driver_name",
            "nationality_wiki",
            "teams_wiki",
            "former_teams_wiki",
            "birth_place_wiki",
            "car_number_wiki",
            "birth_date_wiki",
            "birth_date",
        ]

        self.field_weights = {
            "driver_name": 3.0,
            "teams_wiki": 2.0,
        }

        self.field_aliases = {
            "name": "driver_name",
            "nationality": "nationality_wiki",
            "teams": "teams_wiki",
            "former_teams": "former_teams_wiki",
            "birth_place": "birth_place_wiki",
            "car_number": "car_number_wiki",
            "birth_date": "birth_date_wiki",
        }

        self.numeric_fields = ["wins", "podiums", "starts", "poles", "fastest_laps"]

    def resolve_field_alias(self, query_string):
        for alias, real_field in self.field_aliases.items():
            # Look for pattern: alias:value
            pattern = rf'\b{alias}:'
            if re.search(pattern, query_string):
                query_string = re.sub(pattern, f'{real_field}:', query_string)

        return query_string

    def parse_numeric_query(self, query_string):
        match = re.match(r'(\w+):\[(\d+)\s+TO\s+(\d+|\*)\]', query_string, re.IGNORECASE)
        if match:
            field, min_val, max_val = match.groups()
            min_val = int(min_val)
            max_val = 999999 if max_val == '*' else int(max_val)
            return field, min_val, max_val

        match = re.match(r'(\w+):\[(\d+)-(\d+|\*)\]', query_string)
        if match:
            field, min_val, max_val = match.groups()
            min_val = int(min_val)
            max_val = 999999 if max_val == '*' else int(max_val)
            return field, min_val, max_val

        match = re.match(r'(\w+):\[(\d+)\s+TO\s+\*\]', query_string, re.IGNORECASE)
        if match:
            field, min_val = match.groups()
            return field, int(min_val), 999999

        match = re.match(r'(\w+):\[(\d+)-\*\]', query_string)
        if match:
            field, min_val = match.groups()
            return field, int(min_val), 999999

        return None

    def search(self, query_string, max_results=100, sort_by=None):
        try:
            query_string = self.resolve_field_alias(query_string)

            numeric_result = self.parse_numeric_query(query_string)

            if numeric_result:
                field, min_val, max_val = numeric_result

                if field not in self.numeric_fields:
                    print(f"\nERROR: '{field}' is not a numeric field!")
                    print(f"Available: {', '.join(self.numeric_fields)}")
                    return

                query = IntPoint.newRangeQuery(field, min_val, max_val)

            elif ":" in query_string:
                parser = QueryParser("driver_name", self.analyzer)
                parser.setAllowLeadingWildcard(True)
                query = parser.parse(query_string)

            else:
                builder = BooleanQuery.Builder()

                for field in self.text_fields:
                    parser = QueryParser(field, self.analyzer)
                    parser.setAllowLeadingWildcard(True)
                    try:
                        field_query = parser.parse(query_string)

                        weight = self.field_weights.get(field, 1.0)
                        boosted_query = BoostQuery(field_query, weight)

                        builder.add(boosted_query, BooleanClause.Occur.SHOULD)
                    except:
                        pass

                query = builder.build()

            if sort_by:
                sort = Sort(SortField(sort_by, SortField.Type.INT, True))
                hits = self.searcher.search(query, max_results, sort)
            else:
                hits = self.searcher.search(query, max_results)

            total = hits.totalHits.value()
            print(f"\nFound {total} results for query: '{query_string}'")

            if total == 0:
                print("\nNo results.    Try:")
                print("  - Michael")
                print("  - Ferrari")
                print("  - wins:[10-100]")
                print("  - teams:Mercedes")
                print("  - 1987")
                return

            print("=" * 100)

            stored_fields = self.reader.storedFields()
            for i, score_doc in enumerate(hits.scoreDocs, 1):
                doc = stored_fields.document(score_doc.doc)
                self.show_result(doc, score_doc.score, i)

        except Exception as e:
            print(f"\nERROR: {e}")
            import traceback
            traceback.print_exc()

    def show_result(self, doc, score, rank):
        print(f"\n{rank}.  Score: {score:.4f}")
        print(f"   Name: {doc.get('driver_name')}")
        print(f"   Birth: {doc.get('birth_date_wiki') or doc.get('birth_date') or 'unknown'}")
        print(f"   Place: {doc.get('birth_place_wiki') or 'unknown'}")
        print(f"   Nationality: {doc.get('nationality_wiki') or 'unknown'}")
        print(f"   Teams: {doc.get('teams_wiki') or doc.get('former_teams_wiki') or 'none'}")
        print(f"   Car number: {doc.get('car_number_wiki') or 'none'}")
        print(
            f"   Wins: {doc.get('wins') or '0'} | Podiums: {doc.get('podiums') or '0'}")
        print(f"   Pole positions: {doc.get('poles') or '0'} | Fastest laps: {doc.get('fastest_laps') or '0'}")
        print(f"   Championships: {doc.get('championships_wiki') or '0'}")
        print("-" * 100)

    def show_help(self):
        """Help"""
        print("\n" + "=" * 100)
        print("F1 FULL-TEXT SEARCH ENGINE")
        print("=" * 100)
        print("\nBASIC SEARCH (multi-field with boosting):")
        print("  Michael              - search 'Michael' in all fields")
        print("  Ferrari              - search 'Ferrari' in all fields")
        print("  1987                 - search birth year 1987")
        print("\nSPECIFIC FIELD (with aliases):")
        print("  name:Michael         - name only")
        print("  teams:Ferrari        - teams only")
        print("  nationality:British  - nationality only")
        print("  car_number:44        - car number only")
        print("  birth_place:Germany  - birth place only")
        print("  birth_date:1987      - birth year 1987")
        print("\nNUMERIC RANGES:")
        print("  wins:[10-100]        - 10-100 wins")
        print("  wins:[50-*]          - 50+ wins")
        print("  podiums:[20-50]      - 20-50 podiums")
        print("  starts:[100-*]       - 100+ starts")
        print("  fastest_laps:[10-*]  - 10+ fastest laps")
        print("\nCOMBINED QUERIES:")
        print("  name:Hamilton AND nationality:British")
        print("  teams:Ferrari AND wins:[10-*]")
        print("  birth_date:1987 AND nationality:German")
        print("\nWILDCARDS:")
        print("  Schum*               - starts with 'Schum'")
        print("  *son                 - ends with 'son'")
        print("  teams:*Mercedes*     - contains 'Mercedes'")
        print("\nCOMMANDS:")
        print("  help                 - show help")
        print("  exit                 - quit")
        print("  sort:wins [query]    - sort by wins")
        print("=" * 100)

    def close(self):
        self.reader.close()


def main():
    searcher = F1SearchCLI()

    print("\n" + "=" * 100)
    print("F1 FULL-TEXT SEARCH ENGINE - LUCENE (with Field Boosting)")
    print("=" * 100)
    print("\nExamples: 'Ferrari', 'wins:[10-100]', 'teams:Mercedes', '1987'")
    print("Type 'help' for help or 'exit' to quit\n")

    while True:
        try:
            query_input = input("Query: ").strip()

            if not query_input:
                continue

            if query_input.lower() in ["exit", "quit", "q"]:
                print("\nGoodbye!")
                break

            if query_input.lower() in ["help", "h", "?"]:
                searcher.show_help()
                continue

            sort_by = None
            if query_input.startswith("sort:"):
                parts = query_input.split(" ", 1)
                if len(parts) == 2:
                    sort_by = parts[0].replace("sort:", "")
                    query_input = parts[1]
                else:
                    print("ERROR: Usage: sort:wins Hamilton")
                    continue

            searcher.search(query_input, max_results=100, sort_by=sort_by)

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nERROR: {e}")

    searcher.close()


if __name__ == "__main__":
    main()