"""MRJob that counts reviews per category (``N_c``).

We use this so we can then load those values into our second step. This
should help to reduce read and shuffle overhead since we can then omit
another mapper in our chi square job.

Input
-----
Newline-delimited JSON reviews, one per line, with a ``category`` field.


Output
------
One ``(category, count)`` pair per category. 
"""

from __future__ import annotations

import json

from mrjob.job import MRJob


class MRDocCounts(MRJob):
    """Single-step MapReduce producing per-category doc counts."""

    def mapper(self, _, line: str):
        """Emit ``(category, 1)`` for every review.

        :param _: ignored input key supplied by the streaming framework.
        :param line: raw JSON line representing one review.
        :yields: ``(category, 1)`` pairs.
        """
        try:
            review = json.loads(line)
        except json.JSONDecodeError:
            return
        category = review.get("category")
        if not category:
            return
        yield category, 1

    def combiner(self, key, values):
        """Sum partial counts on the mapper side to cut network traffic.

        :param key: ``category`` (str).
        :param values: iterable of partial counts ``[1, 1, ...]`` from
            ``mapper`` for this category within one map task.
        :yields: ``(category, partial_sum)`` pairs.
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """Sum combiner output to produce the final ``(category, N_c)`` pair.

        :param key: ``category`` (str).
        :param values: iterable of partial sums from all combiners for this
            category.
        :yields: ``(category, N_c)`` — the total review count per category.
        """
        yield key, sum(values)


if __name__ == "__main__":
    MRDocCounts.run()
