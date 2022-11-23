import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

import const as ct


def merge(df1: DataFrame, df2: DataFrame, join_type: str, join_key: str, path: str):
    assert join_type in ct.JOIN_TYPES
    res_df= df1.join(df2, on=join_key, how=join_type)
    res_df.coalesce(1).write.csv(f'{path}/{ct.OUTPUT_MERGE_KEY}_{join_type}.csv', header='true')


if __name__ == "__main__":
    spark = SparkSession.builder.appName(ct.APP_NAME).getOrCreate()
    wd = os.path.join(os.environ[ct.HOME_ENV], ct.OUTPUT_PATH)

    df1 = spark.read.csv(f'{wd}/pyspark_demo_df1.csv', header=True)
    df2 = spark.read.csv(f'{wd}/pyspark_demo_df2.csv', header=True)

    merge_types = ["inner", "outer"]

    for merge_type in merge_types:
        merge(df1=df1, df2=df2, join_type=merge_type, join_key="id", path=wd)


