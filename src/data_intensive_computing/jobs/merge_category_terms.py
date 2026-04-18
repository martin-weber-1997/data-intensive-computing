from __future__ import annotations

import json

from mrjob.job import MRJob
from mrjob.step import MRStep


def parse_mrjob_output_line(line: str) -> tuple[str, dict]:
    """Parse one tab-separated `mrjob` output line."""
    key, value = line.rstrip("\n").split("\t", 1)
    return json.loads(key), json.loads(value)


class MRMergeCategoryTerms(MRJob):
    """Merge top-k category term lists into one deduplicated vocabulary."""

    def mapper(self, _, line: str):
        category, record = parse_mrjob_output_line(line)
        yield (
            record["term"],
            {
                "category": category,
                "chi_square": record["chi_square"],
                "rank": record["rank"],
            },
        )

    def reducer(self, term, values):
        categories = []
        best_record = None

        for value in values:
            categories.append(value["category"])
            if best_record is None or (
                value["chi_square"],
                -value["rank"],
                value["category"],
            ) > (
                best_record["chi_square"],
                -best_record["rank"],
                best_record["category"],
            ):
                best_record = value

        yield (
            term,
            {
                "best_category": best_record["category"],
                "best_chi_square": best_record["chi_square"],
                "categories": sorted(categories),
            },
        )

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == "__main__":
    MRMergeCategoryTerms.run()
