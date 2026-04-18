from __future__ import annotations

import json

from mrjob.job import MRJob
from mrjob.step import MRStep

from data_intensive_computing.text import load_stopwords, tokenize_unigrams


def chi_square_value(
    term_in_category: int,
    term_not_in_category: int,
    no_term_in_category: int,
    no_term_not_in_category: int,
) -> float:
    """Compute the chi-square score for a 2x2 contingency table."""
    total_documents = (
        term_in_category
        + term_not_in_category
        + no_term_in_category
        + no_term_not_in_category
    )
    numerator = (
        total_documents
        * (
            (term_in_category * no_term_not_in_category)
            - (term_not_in_category * no_term_in_category)
        )
        ** 2
    )
    denominator = (
        (term_in_category + no_term_in_category)
        * (term_not_in_category + no_term_not_in_category)
        * (term_in_category + term_not_in_category)
        * (no_term_in_category + no_term_not_in_category)
    )
    return 0.0 if denominator == 0 else numerator / denominator


def keep_top_terms(
    current_top_terms: list[dict],
    candidate: dict,
    top_k: int,
) -> list[dict]:
    """Insert a candidate and keep only the best `top_k` scores."""
    updated = current_top_terms + [candidate]
    updated.sort(key=lambda item: (-item["chi_square"], item["term"]))
    return updated[:top_k]


class MRChiSquareTerms(MRJob):
    """Compute top chi-square unigram features per category."""

    def configure_args(self) -> None:
        super().configure_args()
        self.add_file_arg(
            "--stopwords",
            default="data/stopwords.txt",
            help="Path to the stopword list shipped to the job runner.",
        )
        self.add_passthru_arg(
            "--top-k",
            type=int,
            default=75,
            help="Number of highest-scoring terms to keep per category.",
        )

    def mapper_init(self) -> None:
        self.stopwords = load_stopwords(self.options.stopwords)

    def mapper(self, _, line: str):
        record = json.loads(line)
        category = record["category"]
        tokens = set(tokenize_unigrams(record.get("reviewText", ""), self.stopwords))

        yield ("__all__", "__documents__"), 1
        yield ("__category__", category), 1

        for token in tokens:
            yield ("__term__", token), 1
            yield ("__term_category__", token, category), 1

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer_collect_counts(self, key, counts):
        yield key, sum(counts)

    def mapper_prepare_statistics(self, key, count: int):
        kind = key[0]

        if kind == "__all__":
            yield "__global__", ("documents", count)
        elif kind == "__category__":
            _, category = key
            yield "__global__", ("category", category, count)
        elif kind == "__term__":
            _, term = key
            yield term, ("term_total", count)
        elif kind == "__term_category__":
            _, term, category = key
            yield term, ("term_category", category, count)

    def reducer_build_records(self, term, values):
        if term == "__global__":
            total_documents = 0
            category_totals: dict[str, int] = {}

            for value in values:
                if value[0] == "documents":
                    total_documents += value[1]
                else:
                    _, category, count = value
                    category_totals[category] = count

            for category, count in category_totals.items():
                yield (
                    f"0\t{category}",
                    {
                        "record_type": "category_total",
                        "category": category,
                        "documents_in_category": count,
                        "total_documents": total_documents,
                    },
                )
            return

        term_total = 0
        term_category_counts: dict[str, int] = {}

        for value in values:
            if value[0] == "term_total":
                term_total += value[1]
            else:
                _, category, count = value
                term_category_counts[category] = count

        yield (
            f"1\t{term}",
            {
                "record_type": "term_stats",
                "term": term,
                "term_total": term_total,
                "term_category_counts": term_category_counts,
            },
        )

    def reducer_select_top_terms_init(self) -> None:
        self.total_documents = 0
        self.category_totals: dict[str, int] = {}
        self.top_terms_by_category: dict[str, list[dict]] = {}

    def reducer_select_top_terms(self, sorted_key, values):
        _, name = sorted_key.split("\t", 1)

        for value in values:
            if value["record_type"] == "category_total":
                self.total_documents = value["total_documents"]
                self.category_totals[value["category"]] = value["documents_in_category"]
                self.top_terms_by_category.setdefault(value["category"], [])
                continue

            term = value["term"]
            term_total = value["term_total"]
            counts_by_category = value["term_category_counts"]

            for category, category_total in self.category_totals.items():
                n11 = counts_by_category.get(category, 0)
                n10 = term_total - n11
                n01 = category_total - n11
                n00 = self.total_documents - n11 - n10 - n01
                score = chi_square_value(n11, n10, n01, n00)

                candidate = {
                    "term": term,
                    "category": category,
                    "chi_square": score,
                    "n11": n11,
                    "n10": n10,
                    "n01": n01,
                    "n00": n00,
                    "term_documents": term_total,
                    "category_documents": category_total,
                    "total_documents": self.total_documents,
                }
                self.top_terms_by_category[category] = keep_top_terms(
                    self.top_terms_by_category[category],
                    candidate,
                    self.options.top_k,
                )

    def reducer_select_top_terms_final(self):
        for category in sorted(self.top_terms_by_category):
            ranked_terms = self.top_terms_by_category[category]
            for rank, record in enumerate(ranked_terms, start=1):
                yield (
                    category,
                    {
                        "rank": rank,
                        **record,
                    },
                )

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer_collect_counts,
            ),
            MRStep(
                mapper=self.mapper_prepare_statistics,
                reducer=self.reducer_build_records,
            ),
            MRStep(
                reducer_init=self.reducer_select_top_terms_init,
                reducer=self.reducer_select_top_terms,
                reducer_final=self.reducer_select_top_terms_final,
                jobconf={
                    "mapreduce.job.reduces": "1",
                    "mapred.reduce.tasks": "1",
                },
            ),
        ]


if __name__ == "__main__":
    MRChiSquareTerms.run()
