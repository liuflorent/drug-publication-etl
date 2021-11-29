from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def get_spark_session(app: str):
    return SparkSession.builder.appName(app).getOrCreate()


def read_csv(spark: SparkSession, filepath: str, schema: StructType, separator: str = ","):
    return (
        spark.read.schema(schema)
        .format("csv")
        .option("sep", separator)
        .option("header", "true")
        .load(filepath)
    )


def read_json(spark: SparkSession, filepath: str, schema: StructType):
    return spark.read.option("multiline", "true").schema(schema).json(filepath)


def write_json(filepath: str, df: DataFrame):
    df.coalesce(1).write.format("json").mode("overwrite").save(filepath)


def rename_columns(df: DataFrame, old_columns: list, new_columns: list):
    for old_col, new_col in zip(old_columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)
    return df
