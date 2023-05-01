import pytest

from pytest_spark.config import SparkConfigBuilder
from pytest_spark.util import reduce_logging


# with regard to https://docs.pytest.org/en/stable/deprecations.html#calling-fixtures-directly
# it is necessary to copy some code from pytest_spark library


@pytest.fixture(scope="session", params=["pandas", "spark"])
def _pandas_or_spark_session(request):
    """Internal fixture for SparkSession instance.

    Yields SparkSession instance if it is supported by the pyspark
    version, otherwise yields None.

    Required to correctly initialize `spark_context` fixture after
    `spark_session` fixture.

    ..note::
        It is not possible to create SparkSession from the existing
        SparkContext.
    """
    if request.param == "pandas":
        yield None
        return

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        yield
    else:
        session = SparkSession.builder \
            .config(conf=SparkConfigBuilder().get()) \
            .getOrCreate()

        yield session
        session.stop()


@pytest.fixture(scope="session")
def pandas_or_spark_session(_pandas_or_spark_session):
    """Return a Hive enabled SparkSession instance with reduced logging
    (session scope).

    Available from Spark 2.0 onwards.
    """

    if _pandas_or_spark_session is None:
        yield None

    else:
        reduce_logging(_pandas_or_spark_session.sparkContext)
        yield _pandas_or_spark_session
