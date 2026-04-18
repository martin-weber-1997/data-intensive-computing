import pytest

from data_intensive_computing.spark import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(app_name="pytest-spark", master="local[2]")
    yield session
    session.stop()
