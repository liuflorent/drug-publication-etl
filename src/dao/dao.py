from pyspark.sql import DataFrame, SparkSession

import src.utils.utils as utils
from src.schema import schemas


class DrugDao(object):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path
        self.schema = schemas.drug_schema

    def read(self):
        """
        Read csv input file.
        :return: pyspark.sql.DataFrame
        """
        return utils.read_csv(spark=self.spark, filepath=self.path, schema=self.schema)


class ClinicalTrialDao(object):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path
        self.schema = schemas.clinical_trial_schema

    def read(self):
        """
        Read csv input file.
        :return: pyspark.sql.DataFrame
        """
        return utils.read_csv(spark=self.spark, filepath=self.path, schema=self.schema)


class PubmedDao(object):
    def __init__(self, spark: SparkSession, csv_path: str, json_path: str):
        self.spark = spark
        self.csv_path = csv_path
        self.json_path = json_path
        self.schema = schemas.pubmed_schema

    def read(self):
        """
        Read csv and json input files.
        :return: pyspark.sql.DataFrame
        """
        return utils.read_csv(spark=self.spark, filepath=self.csv_path, schema=self.schema).union(
            utils.read_json(spark=self.spark, filepath=self.json_path, schema=self.schema)
        )


class PublicationDao(object):
    def __init__(self, path: str):
        self.path = path

    def write(self, df: DataFrame, destination: str):
        """
        Write a json file with DataFrame.
        :param df: pyspark.sql.DataFrame
        :param destination: the path to write the file
        """
        utils.write_json(filepath=f"{self.path}/{destination}", df=df)
