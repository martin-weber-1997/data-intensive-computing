from data_intensive_computing.text import tokenize_unigrams


def test_tokenize_unigrams_applies_assignment_rules():
    stopwords = {"this", "and", "for", "he"}
    text = "This is 1 test, and HE said: Spark++ for {ML}! a b cd"

    assert tokenize_unigrams(text, stopwords) == [
        "is",
        "test",
        "said",
        "spark",
        "ml",
        "cd",
    ]
