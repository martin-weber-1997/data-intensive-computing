from data_intensive_computing.jobs.chi_square_terms import (
    chi_square_value,
    keep_top_terms,
)
from data_intensive_computing.jobs.merge_category_terms import parse_mrjob_output_line


def test_chi_square_value_matches_known_example():
    assert round(chi_square_value(20, 5, 10, 15), 6) == 8.333333


def test_keep_top_terms_preserves_best_scores_and_order():
    top_terms = []
    top_terms = keep_top_terms(top_terms, {"term": "beta", "chi_square": 5.0}, top_k=2)
    top_terms = keep_top_terms(top_terms, {"term": "alpha", "chi_square": 5.0}, top_k=2)
    top_terms = keep_top_terms(top_terms, {"term": "gamma", "chi_square": 7.0}, top_k=2)

    assert top_terms == [
        {"term": "gamma", "chi_square": 7.0},
        {"term": "alpha", "chi_square": 5.0},
    ]


def test_parse_mrjob_output_line_reads_json_key_and_value():
    category, payload = parse_mrjob_output_line(
        '"Book"\t{"rank": 1, "term": "novel", "chi_square": 12.5}\n'
    )

    assert category == "Book"
    assert payload == {"rank": 1, "term": "novel", "chi_square": 12.5}
