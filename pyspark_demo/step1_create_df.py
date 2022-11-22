import os, sys
sys.path.append(os.path.dirname(__file__))
from pathlib import Path

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import DataFrame

from const import Gaussian_loc, Gaussian_scale, Poisson_iam, DistriType

np.random.seed(1)


class COL:
    def __init__(self, name: str, dtype, distr: str, size: int):
        self.name = name
        self.dtype = dtype
        if distr not in DistriType._member_names_:
            raise Exception(f"distribution type {distr} not supported yet!")
        self.distr = distr
        self.size = size

    def gen_distribution(self, size: int):
        if self.distr.lower() == DistriType.gaussian.name:
            return [float(i) for i in np.random.normal(size=size, loc=Gaussian_loc, scale=Gaussian_scale)]
        elif self.distr == DistriType.poisson.name:
            return [int(i) for i in np.random.poisson(size=size, lam=Poisson_iam)]
        elif self.distr == DistriType.index.name:
            return range(size)
        else:
            raise Exception(f"this shouldn't happen")


class DF:
    def __init__(self, cols: list, spark: SparkSession, output_name: str):
        self.cols = cols
        self.spark = spark
        self.output_name = output_name

    def create_cols_data(self):
        cols_data = []
        for col in self.cols:
            cols_data.append(col.gen_distribution(col.size))

        return list(zip(*cols_data))

    def enforce_type(self, df: DataFrame):
        for col in self.cols:
            df = df.withColumn(col.name, df[col.name].cast(col.dtype))
        return df

    def run(self):
        df = self.create_df()
        df.coalesce(1).write.csv(self.output_name, header='true')

    def create_df(self):
        data = self.create_cols_data()
        col_names = [col.name for col in self.cols]
        df = spark.createDataFrame(data, col_names)
        df = self.enforce_type(df)
        return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName('pyspark_demo').getOrCreate()
    wd = os.path.join(os.environ['HOME'], "res")
    os.makedirs(wd, exist_ok=True)

    uni_size = 1000
    df1_schema = [COL(name="id", dtype=IntegerType(), distr="index", size=uni_size),
                  COL(name="price", dtype=DoubleType(), distr="gaussian", size=uni_size)]
    df2_schema = [COL(name="id", dtype=IntegerType(), distr="index", size=uni_size),
                  COL(name="sales", dtype=IntegerType(), distr="poisson", size=uni_size)]

    df1 = DF(cols=df1_schema, spark=spark, output_name=f"{wd}/pyspark_demo_df1.csv")
    df2 = DF(cols=df2_schema, spark=spark, output_name=f"{wd}/pyspark_demo_df2.csv")

    for df in [df1, df2]:
        df.run()
