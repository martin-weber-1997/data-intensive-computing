from __future__ import annotations

import re
from pathlib import Path

DELIMITER_PATTERN = re.compile(r"[\s\t\d()\[\]{}.!?,;:+=_\"'`~#@&*%€$§\\/]+")


def load_stopwords(path: str | Path) -> set[str]:
    """Load one stop word per line, normalized with case folding."""
    stopwords_path = Path(path)
    with stopwords_path.open(encoding="utf-8") as handle:
        return {line.strip().casefold() for line in handle if line.strip()}


def tokenize_unigrams(text: str, stopwords: set[str]) -> list[str]:
    """Tokenize text according to the assignment rules.
    :text string that will get tokenized into unigrams
    :stopwordds list of stopwords that we will use to tokenize our text

    :returns list of tokenized strings
    """
    # I AM A Frog
    folded_text= text.casefold()
    #i am a frog
    list_of_words = DELIMITER_PATTERN.split(folded_text)
    # ["i","am","a","frog"]
    tokens = []
    for token in list_of_words:
        if len(token) <= 1:
            continue
        if token in stopwords:
            continue
        tokens.append(token)

    return tokens




if __name__ == "__main__":
    print(tokenize_unigrams("I AM A FROG", {"am"}))