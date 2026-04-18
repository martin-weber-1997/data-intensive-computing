from data_intensive_computing.jobs.format_output import (
    build_output_lines,
    format_chi_square_value,
    write_output_file,
)


def test_format_chi_square_value_trims_trailing_zeroes():
    assert format_chi_square_value(12.5) == "12.5"
    assert format_chi_square_value(4.0) == "4"
    assert format_chi_square_value(0.0) == "0"


def test_build_output_lines_formats_categories_and_merged_dictionary():
    terms_by_category = {
        "Book": [
            {"rank": 2, "term": "author", "chi_square": 12.0},
            {"rank": 1, "term": "novel", "chi_square": 14.25},
        ],
        "Electronics": [
            {"rank": 1, "term": "battery", "chi_square": 9.5},
            {"rank": 2, "term": "screen", "chi_square": 8.0},
        ],
    }

    lines = build_output_lines(terms_by_category, top_k=2)

    assert lines == [
        "Book novel:14.25 author:12",
        "Electronics battery:9.5 screen:8",
        "author battery novel screen",
    ]


def test_write_output_file_writes_assignment_layout(tmp_path):
    ranked_terms = tmp_path / "ranked.jsonl"
    ranked_terms.write_text(
        '"Book"\t{"rank": 1, "term": "novel", "chi_square": 14.25}\n'
        '"Book"\t{"rank": 2, "term": "author", "chi_square": 12.0}\n'
        '"Electronics"\t{"rank": 1, "term": "battery", "chi_square": 9.5}\n',
        encoding="utf-8",
    )
    output_file = tmp_path / "output.txt"

    write_output_file(ranked_terms, output_file, top_k=2)

    assert output_file.read_text(encoding="utf-8") == (
        "Book novel:14.25 author:12\n"
        "Electronics battery:9.5\n"
        "author battery novel\n"
    )
