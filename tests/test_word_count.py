from data_intensive_computing.jobs.word_count import count_words, run_job


def test_count_words_transforms_dataframe(spark):
    lines = spark.createDataFrame(
        [("Spark makes data fun",), ("Data jobs with Spark",)],
        ["line"],
    )

    rows = count_words(lines).collect()

    assert rows == [
        ("data", 2),
        ("spark", 2),
        ("fun", 1),
        ("jobs", 1),
        ("makes", 1),
        ("with", 1),
    ]


def test_run_job_reads_local_text_file(spark, tmp_path):
    input_file = tmp_path / "input.txt"
    input_file.write_text("hello spark\nhello tests\n", encoding="utf-8")

    rows = run_job(spark, str(input_file)).collect()

    assert rows == [
        ("hello", 2),
        ("spark", 1),
        ("tests", 1),
    ]
