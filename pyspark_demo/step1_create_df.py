import os, sys
sys.path.append(os.path.dirname(__file__))
from pathlib import Path

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import DataFrame

import const as ct

np.random.seed(1)


class COL:
    def __init__(self, name: str, dtype, distr: str, size: int):
        self.name = name
        self.dtype = dtype
        if distr not in ct.DistriType._member_names_:
            raise Exception(f"distribution type {distr} not supported yet!")
        self.distr = distr.lower()
        self.size = size

    def gen_distribution(self, size: int):
        if self.distr.lower() == ct.DistriType.Gaussian.name.lower():
            return [float(i) for i in np.random.normal(size=size, loc=ct.Gaussian_loc, scale=ct.Gaussian_scale)]
        elif self.distr.lower() == ct.DistriType.Poisson.name.lower():
            return [int(i) for i in np.random.poisson(size=size, lam=ct.Poisson_iam)]
        elif self.distr.lower() == ct.DistriType.Index.name.lower():
            return range(size)
        else:
            raise Exception(f"this shouldn't happen")


class DF:
    spark = SparkSession.builder.appName(ct.APP_NAME).getOrCreate()

    def __init__(self, spark: SparkSession):
        pass

    @staticmethod
    def _create_cols_data(cols: list[COL]):
        cols_data = []
        for col in cols:
            cols_data.append(col.gen_distribution(col.size))

        return list(zip(*cols_data))

    @staticmethod
    def _enforce_type(cols: list[COL], df: DataFrame):
        for col in cols:
            df = df.withColumn(col.name, df[col.name].cast(col.dtype))
        return df

    def run(self, cols: list[COL], output_name: str):
        df = DF._create_df(cols=cols)
        df.coalesce(1).write.csv(output_name, header='true')

    @staticmethod
    def _create_df(cols: list[COL]):
        data = DF._create_cols_data(cols=cols)
        col_names = [col.name for col in cols]
        df = DF.spark.createDataFrame(data, col_names)
        df = DF._enforce_type(cols=cols, df=df)
        return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName(ct.APP_NAME).getOrCreate()
    wd = os.path.join(os.environ[ct.HOME_ENV], ct.OUTPUT_PATH)
    os.makedirs(wd, exist_ok=True)

    df1_schema = [COL(name=ct.ID_NAME, dtype=IntegerType(), distr=ct.DIST_INDEX, size=ct.UNIT_SIZE),
                  COL(name=ct.PRICE_NAME, dtype=DoubleType(), distr=ct.DIST_GAUSS, size=ct.UNIT_SIZE)]
    df2_schema = [COL(name=ct.ID_NAME, dtype=IntegerType(), distr=ct.DIST_INDEX, size=ct.UNIT_SIZE),
                  COL(name=ct.SALES_NAME, dtype=IntegerType(), distr=ct.DIST_POISSON, size=ct.UNIT_SIZE)]

    my_df = DF(spark=spark)

    run_configs = [{ct.COL_NAME: df1_schema, ct.OUTPUT_PATH:f"{wd}/pyspark_demo_df1.csv"},
                   {ct.COL_NAME: df2_schema, ct.OUTPUT_PATH:f"{wd}/pyspark_demo_df2.csv"}]

    for config in run_configs:
        my_df.run(cols=config[ct.COL_NAME], output_name=config[ct.OUTPUT_PATH])

