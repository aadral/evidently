import pandas as pd

from evidently.utils.spark import fixup_pandas_df_for_big_data


def convert_pandas_to_spark_df_if_necessary(dataset, maybe_spark_session):
    if maybe_spark_session is None:
        return dataset

    if dataset is None:
        return None

    fixup_pandas_df_for_big_data(dataset)
    spark_current_df = maybe_spark_session.createDataFrame(dataset)
    pandas_spark_current_df = spark_current_df.pandas_api()
    return pandas_spark_current_df
