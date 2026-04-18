from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "data-intensive-computing",
    master: str = "local[*]",
) -> SparkSession:
    """Create a Spark session tuned for local development and tests."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
