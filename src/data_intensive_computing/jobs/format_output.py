from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def parse_mrjob_output_line(line: str) -> tuple[str, dict]:
    """Parse one tab-separated `mrjob` output line."""
    key, value = line.rstrip("\n").split("\t", 1)
    return json.loads(key), json.loads(value)


def format_chi_square_value(value: float) -> str:
    """Format chi-square values deterministically for `output.txt`."""
    formatted = f"{value:.6f}".rstrip("0").rstrip(".")
    return formatted or "0"


def load_ranked_terms(input_path: str | Path) -> dict[str, list[dict]]:
    """Load the ranked per-category terms emitted by the chi-square MRJob."""
    terms_by_category: dict[str, list[dict]] = defaultdict(list)

    with Path(input_path).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            category, record = parse_mrjob_output_line(line)
            terms_by_category[category].append(record)

    for category in terms_by_category:
        terms_by_category[category].sort(
            key=lambda record: (record["rank"], -record["chi_square"], record["term"])
        )

    return dict(terms_by_category)


def build_output_lines(
    terms_by_category: dict[str, list[dict]],
    top_k: int,
) -> list[str]:
    """Build the assignment-specific `output.txt` lines."""
    lines = []
    merged_terms: set[str] = set()

    for category in sorted(terms_by_category):
        ranked_terms = terms_by_category[category][:top_k]
        formatted_terms = []

        for record in ranked_terms:
            formatted_terms.append(
                f'{record["term"]}:{format_chi_square_value(record["chi_square"])}'
            )
            merged_terms.add(record["term"])

        category_line = " ".join([category, *formatted_terms])
        lines.append(category_line)

    lines.append(" ".join(sorted(merged_terms)))
    return lines


def write_output_file(
    ranked_terms_path: str | Path,
    output_path: str | Path,
    top_k: int = 75,
) -> None:
    """Write the final assignment output file from ranked MRJob results."""
    terms_by_category = load_ranked_terms(ranked_terms_path)
    output_lines = build_output_lines(terms_by_category, top_k=top_k)
    Path(output_path).write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Format chi-square MRJob output into the required output.txt file."
    )
    parser.add_argument(
        "ranked_terms_path",
        help="Path to the per-category ranked term output from chi_square_terms.",
    )
    parser.add_argument(
        "output_path",
        help="Path of the final formatted output file.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=75,
        help="Number of terms per category to keep in the formatted output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    write_output_file(
        ranked_terms_path=args.ranked_terms_path,
        output_path=args.output_path,
        top_k=args.top_k,
    )


if __name__ == "__main__":
    main()
