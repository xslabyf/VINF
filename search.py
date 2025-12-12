import lucene

lucene.initVM()

import re
from java.nio.file import Paths
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene. search import IndexSearcher, BooleanQuery, BooleanClause, BoostQuery, Sort, SortField, FuzzyQuery
from org. apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis. standard import StandardAnalyzer
from org.apache.lucene. document import IntPoint
from org.apache.lucene.index import Term

INDEX_DIR = "index"


class F1Search:
    def __init__(self):
        index_dir = MMapDirectory(Paths.get(INDEX_DIR))
        self.reader = DirectoryReader.open(index_dir)
        self.searcher = IndexSearcher(self.reader)
        self.analyzer = StandardAnalyzer()

        self.search_fields = {
            "driver_name": 5.0,
            "teams_wiki": 2.5,
            "biography": 0.2,
            "nationality_wiki":  5.0,
            "birth_place_wiki": 5.0,
            "birth_date_wiki": 1.5,
            "birth_date":  1.5,
            "car_number_wiki": 1.5,
            "wins_text": 3.0,
            "podiums_text": 3.0,
            "poles_text": 3.0,
            "starts_text": 3.0,
            "fastest_laps_text": 3.0,
            "championships_text": 3.0,
        }

        self.numeric_fields = ["wins", "podiums", "poles", "starts", "fastest_laps", "championships"]

        self.fuzzy_mode = True

    def parse_operator_queries(self, query_string):
        numeric_filters = []
        remaining = query_string

        for field in self.numeric_fields:
            match = re.search(rf'\b{field}\s*>=\s*(\d+)\b', remaining, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                numeric_filters.append((field, value, 999999))
                remaining = re.sub(rf'\b{field}\s*>=\s*\d+\b', '', remaining, flags=re.IGNORECASE)
                continue

            match = re.search(rf'\b{field}\s*>\s*(\d+)\b', remaining, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                numeric_filters. append((field, value + 1, 999999))
                remaining = re.sub(rf'\b{field}\s*>\s*\d+\b', '', remaining, flags=re. IGNORECASE)
                continue

            match = re.search(rf'\b{field}\s*<=\s*(\d+)\b', remaining, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                numeric_filters.append((field, 0, value))
                remaining = re.sub(rf'\b{field}\s*<=\s*\d+\b', '', remaining, flags=re.IGNORECASE)
                continue

            match = re.search(rf'\b{field}\s*<\s*(\d+)\b', remaining, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                numeric_filters.append((field, 0, value - 1))
                remaining = re.sub(rf'\b{field}\s*<\s*\d+\b', '', remaining, flags=re.IGNORECASE)
                continue

            match = re.search(rf'\b{field}\s*==\s*(\d+)\b', remaining, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                numeric_filters. append((field, value, value))
                remaining = re.sub(rf'\b{field}\s*==\s*\d+\b', '', remaining, flags=re.IGNORECASE)
                continue

            match = re.search(rf'\b{field}\s*=\s*(\d+)\b', remaining, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                numeric_filters.append((field, value, value))
                remaining = re.sub(rf'\b{field}\s*=\s*\d+\b', '', remaining, flags=re.IGNORECASE)
                continue

        remaining = re.sub(r'\s+', ' ', remaining).strip()

        return numeric_filters, remaining

    def create_fuzzy_query(self, word, field, boost):
        max_edits = 1 if len(word) < 5 else 2

        term = Term(field, word. lower())
        fuzzy_query = FuzzyQuery(term, max_edits)

        return BoostQuery(fuzzy_query, boost)

    def search(self, query_string, max_results=5, use_or=True, sort_by=None):
        try:
            print(f"\nSearching for:  '{query_string}'")
            if self.fuzzy_mode:
                print("   Fuzzy mode:  ON (tolerates typos)")
            else:
                print("   Fuzzy mode: OFF (exact matching)")

            numeric_filters, text_to_search = self.parse_operator_queries(query_string)

            query_parts = []

            if numeric_filters:
                for field, min_val, max_val in numeric_filters:
                    if min_val == max_val:
                        print(f"   Numeric filter: {field} == {min_val}")
                    else:
                        print(f"   Numeric filter: {field} in [{min_val}, {max_val}]")

                    numeric_query = IntPoint. newRangeQuery(field, min_val, max_val)
                    query_parts.append(numeric_query)

            text_to_search = text_to_search.strip()
            if text_to_search and text_to_search not in ["driver", "drivers", "racer", "racers", "with"]:
                print(f"   Text search: '{text_to_search}'")
                print(f"   Mode: {'OR' if use_or else 'AND'}")

                words = text_to_search.split()
                main_builder = BooleanQuery.Builder()

                for word in words:
                    if word.lower() in ["driver", "drivers", "racer", "racers", "with", "has", "have", "and", "the"]:
                        continue

                    word_builder = BooleanQuery.Builder()

                    for field, boost in self.search_fields.items():
                        try:
                            if self. fuzzy_mode:
                                boosted_query = self.create_fuzzy_query(word, field, boost)
                            else:
                                parser = QueryParser(field, self.analyzer)
                                parser.setAllowLeadingWildcard(True)
                                field_query = parser. parse(word)
                                boosted_query = BoostQuery(field_query, boost)

                            word_builder.add(boosted_query, BooleanClause.Occur.SHOULD)
                        except:
                            pass

                    word_query = word_builder.build()
                    occur = BooleanClause. Occur.SHOULD if use_or else BooleanClause. Occur.MUST
                    main_builder.add(word_query, occur)

                text_query = main_builder.build()
                query_parts.append(text_query)

            if len(query_parts) == 0:
                print("ERROR: No valid query")
                return
            elif len(query_parts) == 1:
                query = query_parts[0]
            else:
                combined_builder = BooleanQuery.Builder()
                for q in query_parts:
                    combined_builder.add(q, BooleanClause. Occur.MUST)
                query = combined_builder.build()

            if sort_by and sort_by in self.numeric_fields:
                print(f"   Sorting by:  {sort_by} (descending)")
                sort = Sort(SortField(sort_by, SortField.Type.INT, True))
                hits = self.searcher.search(query, max_results, sort)
            else:
                hits = self.searcher.search(query, max_results)

            total = hits.totalHits. value()
            print(f"\nFound {total} results\n")
            print("=" * 120)

            if total == 0:
                if not self. fuzzy_mode:
                    print("\nTry:")
                    print("   - Enable fuzzy mode:  'fuzzy on'")
                    print("   - Check spelling")
                else:
                    print("\nNo results even with fuzzy matching")
                return

            stored_fields = self.reader. storedFields()
            for i, score_doc in enumerate(hits.scoreDocs, 1):
                doc = stored_fields. document(score_doc.doc)
                self.show_result(doc, score_doc.score, i)

        except Exception as e:
            print(f"\nERROR: {e}")
            import traceback
            traceback.print_exc()

    def show_result(self, doc, score, rank):
        print(f"\n{rank}.  Score: {score:.4f}")
        print(f"   Name: {doc.get('driver_name') or 'Unknown'}")

        birth = doc.get('birth_date_wiki') or doc.get('birth_date') or 'unknown'
        place = doc.get('birth_place_wiki') or 'unknown'
        nationality = doc.get('nationality_wiki') or 'unknown'

        print(f"   Birth:  {birth} | {place}")
        print(f"   Nationality: {nationality}")

        teams = doc.get('teams_wiki') or doc.get('former_teams_wiki') or 'none'
        if len(teams) > 100:
            teams = teams[:100] + "..."
        print(f"   Teams: {teams}")

        wins = doc. get('wins') or '0'
        podiums = doc.get('podiums') or '0'
        championships = doc.get('championships') or '0'
        starts = doc.get('starts') or '0'
        poles = doc.get('poles') or '0'

        print(f"   Wins: {wins} | Podiums: {podiums} | Championships: {championships}")
        print(f"   Starts:  {starts} | Poles: {poles}")

    def show_help(self):
        print("\n" + "=" * 120)
        print("F1 INTELLIGENT SEARCH")
        print("=" * 120)
        print("\nTEXT SEARCH:")
        print("   Michael Schumacher")
        print("   Ferrari champion")
        print("   21 wins")
        print("\nOPERATOR-BASED NUMERIC QUERIES:")
        print("   wins>8")
        print("   championships>=5")
        print("   Ferrari wins>20")
        print("\nFUZZY SEARCH (typo tolerance, DEFAULT ON):")
        print("   fuzzy on          - enable fuzzy mode")
        print("   fuzzy off         - disable fuzzy mode")
        print("   Shumacher         - finds 'Schumacher' (with fuzzy on)")
        print("   Ferari            - finds 'Ferrari' (with fuzzy on)")
        print("\nCOMMANDS:")
        print("   help / exit / and / or")
        print("   fuzzy on / fuzzy off")
        print("   sort: wins [query]")
        print("=" * 120)

    def close(self):
        self.reader.close()


def main():
    searcher = F1Search()
    use_or = True

    print("\n" + "=" * 120)
    print("F1 INTELLIGENT SEARCH - Text + Numeric + Fuzzy (DEFAULT ON)")
    print("=" * 120)
    print("\nTry:  'wins>8', 'Ferrari', '21 wins', 'Shumacher' (fuzzy ON by default)")
    print("Type 'help' or 'exit'\n")

    while True:
        try:
            mode_str = f"{'OR' if use_or else 'AND'}"
            if searcher.fuzzy_mode:
                mode_str += " | FUZZY"

            query_input = input(f"Query ({mode_str}): ").strip()

            if not query_input:
                continue

            if query_input.lower() in ["exit", "quit", "q"]:
                print("\nGoodbye!")
                break

            if query_input.lower() in ["help", "h", "?"]:
                searcher.show_help()
                continue

            if query_input.lower() == "and":
                use_or = False
                print("AND mode")
                continue

            if query_input. lower() == "or":
                use_or = True
                print("OR mode")
                continue

            if query_input. lower() in ["fuzzy on", "fuzzy"]:
                searcher.fuzzy_mode = True
                print("Fuzzy mode ON (tolerates typos)")
                continue

            if query_input.lower() in ["fuzzy off", "exact"]:
                searcher.fuzzy_mode = False
                print("Fuzzy mode OFF (exact matching)")
                continue

            sort_by = None
            if query_input.startswith("sort:"):
                parts = query_input.split(" ", 1)
                if len(parts) == 2:
                    sort_by = parts[0].replace("sort:", "")
                    query_input = parts[1]

            searcher.search(query_input, max_results=5, use_or=use_or, sort_by=sort_by)

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nERROR: {e}")

    searcher.close()


if __name__ == "__main__":
    main()