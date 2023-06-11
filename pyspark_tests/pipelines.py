from pyspark.sql import SparkSession
from pyspark.sql.functions import col, DataFrame


def run(spark: SparkSession):

    # Bad example
    spark.read.parquet("huge_data")\
        .select(col('A'), col('B'))\
        .groupby('C').agg(sum('D'))\
        .write.parquet("more_huge_data")

    # Good example
    def transform1(df: DataFrame) -> DataFrame:
        return df.select(col('A'), col('B'))

    def transform2(df: DataFrame) -> DataFrame:
        return df.groupby('C').agg(sum('D'))

    def write_df(df: DataFrame):
        df.write.parquet("more_huge_data")

    input_df = spark.read.parquet("huge_data")
    transformed_df = input_df\
        .transform(transform1)\
        .transform(transform2)
    write_df(transformed_df)
