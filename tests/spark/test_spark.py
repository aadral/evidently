from typing import Optional

import pandas as pd
import pytest

from evidently.utils.spark import fixup_pandas_df_for_big_data


@pytest.mark.parametrize(
    "pandas_df",
    [
        pytest.param(pd.DataFrame({"feature": [1, 2, 3]}), id="all int"),
        pytest.param(pd.DataFrame({"feature": ["a", 1, 2]}), id="str then int"),
        pytest.param(pd.DataFrame({"feature": [1, 2, "a"]}), id="int then str"),
        pytest.param(pd.DataFrame({"feature": ["a", "b", "c"]}), id="all str"),
    ],
)
def test_spark_dataset_conversion_raise_no_error(pandas_df: pd.DataFrame, spark_session):
    # TODO(aadral): consider global configuration for optimization
    # spark_session.conf.set("spark.sql.execution.arrow.enabled", True)

    # TODO(aadral): consider usage of pandas schema directly, otherwise Spark will iterate
    # over each Row of RDD to infer schema
    # https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/types.html
    # see: #type-casting-between-pandas-and-pandas-api-on-spark

    fixup_pandas_df_for_big_data(pandas_df)
    spark_df = spark_session.createDataFrame(pandas_df, samplingRatio=1.0)
    assert spark_df is not None
