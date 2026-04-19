"""Three-step MapReduce chi-square pipeline with top-N term selection.


``--stopwords`` and ``--stats`` are declared with ``add_file_arg`` so mrjob
uploads them to every task's working directory.

<key, value> design
-------------------
Step 1 (compute ``N_tc`` = #docs in category ``c`` that contain term ``t``):

* map     ``(None, raw JSON line)  ->  ((term, category), 1)`` for every
  unique term in the review after tokenization and stop-word filtering.
* combine ``((term, category), [1, 1, ...])  ->  ((term, category), partial)``.
* reduce  ``((term, category), [partial, ...])  ->  ((term, category), N_tc)``.

Step 2 (compute chi-square per ``(term, category)``):

* map    ``((term, category), N_tc)  ->  (term, (category, N_tc))``.
* reduce ``(term, [(category, N_tc), ...])  ->  (category, (chi^2, term))``.
  The reducer derives ``N_t`` by summing ``N_tc`` across all incoming
  categories and reads ``N`` and ``N_c`` from the side-input stats file.

Step 3 (keep the top N terms per category):

* reduce ``(category, [(chi^2, term), ...])  ->  (category, [(term, chi^2),...])``.
A bounded heap keeps the top-N records per category in one pass.
"""

from __future__ import annotations

import heapq
import json
import re

from mrjob.job import MRJob
from mrjob.step import MRStep

DELIMITER_PATTERN = re.compile(r"[\s\d()\[\]{}.!?,;:+=_\"'`~#@&*%€$§\\/-]+")


def _load_stopwords(path: str) -> set:
    """Read stop words (one per line) into a set."""
    with open(path, encoding="utf-8") as handle:
        return set(handle.read().splitlines())


def _tokenize_unigrams(text: str, stopwords: set) -> list:
    """Case-fold, split on :data:`DELIMITER_PATTERN`, drop one-char/stop tokens."""
    folded = text.casefold()
    tokens = []
    for token in DELIMITER_PATTERN.split(folded):
        if len(token) <= 1 or token in stopwords:
            continue
        tokens.append(token)
    return tokens


class MRChiSquare(MRJob):
    """Three-step chi-square MRJob with top-N per-category selection."""

    def configure_args(self) -> None:
        super().configure_args()
        self.add_file_arg(
            "--stopwords",
            help="Path to the stopword list (one word per line).",
        )
        self.add_file_arg(
            "--stats",
            help=(
                "Path to a JSON side-input file with {'<category>': N_c, ...} "
                "produced by MRDocCounts."
            ),
        )
        self.add_passthru_arg(
            "--top-n",
            type=int,
            default=75,
            help="Number of top-scoring terms to keep per category (default 75).",
        )
        # Per-step reducer counts. Steps 1 and 2 shuffle the heavy term-
        # partitioned stream and benefit from a high reducer count on the
        # cluster. Step 3 has at most one group per category (22), so capping
        # at that count avoids wasted containers.
        self.add_passthru_arg(
            "--reducers-s1",
            type=int,
            default=60,
            help="Reducers for step 1 (term x category doc freq). Default 60.",
        )
        self.add_passthru_arg(
            "--reducers-s2",
            type=int,
            default=60,
            help="Reducers for step 2 (chi^2 per term). Default 60.",
        )
        self.add_passthru_arg(
            "--reducers-s3",
            type=int,
            default=22,
            help="Reducers for step 3 (top-N per category). Default 22.",
        )

    # ------------------------------------------------------------------ step 1
    def mapper_init_s1(self) -> None:
        """Load the stopword list once per mapper task."""
        self.stopwords = _load_stopwords(self.options.stopwords)

    def mapper_s1(self, _, line: str):
        """Emit ``((term, category), 1)`` for every unique token in a review.

        :param _: ignored input key.
        :param line: one raw JSON review per line.
        :yields: ``((term, category), 1)`` pairs (duplicates within a review
            are removed via a set so we count *documents*, not term frequencies).
        """
        try:
            review = json.loads(line)
        except json.JSONDecodeError:
            return
        category = review.get("category")
        text = review.get("reviewText")
        if not category or not text:
            return
        for term in set(_tokenize_unigrams(text, self.stopwords)):
            yield (term, category), 1

    def combiner_s1(self, key, values):
        """Locally sum partial counts for the same ``(term, category)`` key."""
        yield key, sum(values)

    def reducer_s1(self, key, values):
        """Finalize ``N_tc`` = total docs in ``c`` containing ``t``."""
        yield key, sum(values)

    # ------------------------------------------------------------------ step 2
    def mapper_s2(self, key, value):
        """Re-key step-1 output by term so one reducer sees all categories.

        ``key`` arrives as the JSON-decoded list ``[term, category]``.
        """
        term, category = key
        yield term, (category, value)

    def reducer_init_s2(self) -> None:
        """Load ``N_c`` from the side-input stats JSON and derive ``N``."""
        with open(self.options.stats, encoding="utf-8") as handle:
            stats = json.load(handle)
        self.N_c = {str(k): int(v) for k, v in stats.items()}
        self.N = sum(self.N_c.values())

    def reducer_s2(self, term, values):
        """Compute chi^2 for every ``(term, category)`` incoming under ``term``.

        Uses the 2x2 contingency table

        =======  ================================  ===========================
                 contains term ``t``               does *not* contain ``t``
        =======  ================================  ===========================
        in ``c``  A = N_tc                          C = N_c - N_tc
        not ``c`` B = N_t - N_tc                    D = N - N_c - N_t + N_tc
        =======  ================================  ===========================

        chi^2 = N * (A*D - B*C)^2 / (N_t * (N - N_t) * N_c * (N - N_c)).
        """
        pairs = [(str(c), int(n)) for c, n in values]
        N_t = sum(n for _, n in pairs)
        if N_t == 0:
            return
        N = self.N
        for category, N_tc in pairs:
            N_c = self.N_c.get(category)
            if N_c is None:
                continue
            A = N_tc
            B = N_t - N_tc
            C = N_c - N_tc
            D = N - N_c - N_t + N_tc
            denom = N_t * (N - N_t) * N_c * (N - N_c)
            if denom == 0:
                continue
            chi2 = N * (A * D - B * C) ** 2 / denom
            yield category, (chi2, term)

    # ------------------------------------------------------------------ step 3
    def reducer_s3(self, category, values):
        """Select the top-N ``(chi^2, term)`` pairs for ``category``.

        The bounded heap keeps memory use at ``top_n`` items per category.
        """
        top = heapq.nlargest(
            self.options.top_n,
            ((float(chi2), str(term)) for chi2, term in values),
        )
        yield category, [(term, chi2) for chi2, term in top]

    # --------------------------------------------------------------- pipeline
    def steps(self):
        # Per-step reducer counts baked into each MRStep's jobconf so the
        # Hadoop runner picks them up via ``-D mapreduce.job.reduces=N``.
        return [
            MRStep(
                mapper_init=self.mapper_init_s1,
                mapper=self.mapper_s1,
                combiner=self.combiner_s1,
                reducer=self.reducer_s1,
                jobconf={"mapreduce.job.reduces": str(self.options.reducers_s1)},
            ),
            MRStep(
                mapper=self.mapper_s2,
                reducer_init=self.reducer_init_s2,
                reducer=self.reducer_s2,
                jobconf={"mapreduce.job.reduces": str(self.options.reducers_s2)},
            ),
            MRStep(
                reducer=self.reducer_s3,
                jobconf={"mapreduce.job.reduces": str(self.options.reducers_s3)},
            ),
        ]


if __name__ == "__main__":
    MRChiSquare.run()
