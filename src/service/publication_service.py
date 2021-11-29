from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.functions import col, count, expr, lit, regexp_replace, trim, upper, when

from src.dao import dao
from src.utils import constants, utils


class PublicationService(object):
    """
    Finds the name fo one or serveral newpapers from the json produced by the data pipeline
    that mention the most different drugs.
    :param pyspark.sql session
    :param drug inputfile path
    :param clinical trial inputfile path
    :param pubmed csv inputfile path
    :param pubmed json inputfile path
    :param output path
    """

    def __init__(
        self,
        spark: SparkSession,
        drug_path: str,
        clinical_trial_path: str,
        pubmed_csv_path: str,
        pubmed_json_path: str,
        output_path: str,
    ):
        self.spark = spark
        self.drug_path = drug_path
        self.clinical_trial_path = clinical_trial_path
        self.pubmec_csv_path = pubmed_csv_path
        self.pubmec_json_path = pubmed_json_path
        self.output_path = output_path
        self.drug_dao = dao.DrugDao(self.spark, self.drug_path)
        self.clinical_trial_dao = dao.ClinicalTrialDao(self.spark, self.clinical_trial_path)
        self.pubmed_dao = dao.PubmedDao(self.spark, self.pubmec_csv_path, self.pubmec_json_path)

    def execute(self):
        """
        - Finds the result which represents a link graph between the different drugs and their respective
        mentions in the different PubMed publications, the scientific publications and finally the newpapers
        with the date associated with each of these mentions.
        - Finds the name fo one or serveral newpapers from the json produced by the data pipeline
        that mention the most different drugs.
        - save the two results into output
        :return: the result of the execution
        :type: str
        """
        publication_df = self._find_publication_result()
        top_journals_df = self._find_top_journals_result(publication_df)

        publiation_dao = dao.PublicationDao(path=self.output_path)
        publiation_dao.write(df=publication_df, destination="publication")
        publiation_dao.write(df=top_journals_df, destination="top_journals")

        top_journals = top_journals_df.select(constants.JOURNAL).rdd.flatMap(lambda x: x).collect()

        result = (
            f"Le résultat de publication a été enregistré sur {self.output_path}/publication\n"
            "Le résultat des journax qui mentionnent le plus de médicaments différents:\n"
            f"- {top_journals}\n"
            f"Le résultat a été enregistré sur {self.output_path}/top_journals"
        )
        return result

    def _find_publication_result(self):
        """
        Finds the result which represents a link graph between the different drugs and their respective
        mentions in the different PubMed publications, the scientific publications and finally the newpapers
        with the date associated with each of these mentions.
        :return: pyspark.sql.DataFrame
        """
        drug_df = self._get_drug_df()
        clinical_trial_df = self._get_clinical_trial_df()
        pubmed_df = self._get_pubmed_df()

        columns = [
            constants.DRUG_NAME,
            constants.JOURNAL,
            constants.CLINICAL_TRIAL,
            constants.PUBMED,
            constants.CLINICAL_TRIAL_DATE,
        ]

        df1 = self._union(
            left_df=drug_df,
            right_df=clinical_trial_df,
            join_left_column=constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE,
            join_right_column=constants.DRUG_NAME,
            add_column=constants.PUBMED,
            drop_columns=(
                constants.DRUG_ATCCODE,
                constants.CLINICAL_TRIAL_ID,
            ),
            old_columns=[
                constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE,
                constants.CLINICAL_TRIAL_JOURNAL,
            ],
            new_columns=[constants.CLINICAL_TRIAL, constants.JOURNAL],
            select_columns=columns,
        )
        df2 = self._union(
            left_df=drug_df,
            right_df=pubmed_df,
            join_left_column=constants.PUBMED_TITLE,
            join_right_column=constants.DRUG_NAME,
            add_column=constants.CLINICAL_TRIAL,
            drop_columns=(
                constants.DRUG_ATCCODE,
                constants.PUBMED_ID,
            ),
            old_columns=[
                constants.PUBMED_TITLE,
                constants.PUBMED_JOURNAL,
            ],
            new_columns=[constants.PUBMED, constants.JOURNAL],
            select_columns=columns,
        )
        regex = r"\\x[a-f0-9]{2}"
        df = (
            df1.union(df2)
            .withColumn(
                constants.PUBMED,
                when(col(constants.PUBMED).isNotNull(), lit("yes")).otherwise(lit("no")),
            )
            .withColumn(
                constants.CLINICAL_TRIAL,
                when(col(constants.CLINICAL_TRIAL).isNotNull(), lit("yes")).otherwise(lit("no")),
            )
            .withColumn(
                constants.JOURNAL,
                when(
                    col(constants.JOURNAL).isNotNull(),
                    trim(regexp_replace(col(constants.JOURNAL), regex, "")),
                ),
            )
        )
        df.show()
        return df

    def _find_top_journals_result(self, df: DataFrame):
        """
        Finds the name fo one or serveral newpapers from the json produced by the data pipeline
        that mention the most different drugs.
        :param: the graph whichs represents a link graph between the different drugs and their respective
        mentions in the different PubMed publications, the scientific publications and finally the newpapers
        with the date associated with each of these mention
        :type: pyspark.sql.DataFrame
        :return: the result
        :type: pyspark.sql.DataFrame
        """
        rows = (
            df.select(col(constants.DRUG_NAME), col(constants.JOURNAL))
            .dropDuplicates([constants.DRUG_NAME, constants.JOURNAL])
            .groupBy(col(constants.JOURNAL))
            .agg(count("*").alias(constants.DRUGS_COUNT))
        )
        max_count = rows.agg({constants.DRUGS_COUNT: "max"}).collect()[0][0]
        print(max_count, type(max_count))

        rows = rows.filter(rows[constants.DRUGS_COUNT] == max_count)
        rows.show()
        return rows

    def _union(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_left_column: str,
        join_right_column: str,
        add_column: str,
        drop_columns: tuple,
        old_columns: list,
        new_columns: list,
        select_columns: list,
    ):
        df = (
            left_df.join(
                right_df,
                upper(right_df[join_left_column]).contains(upper(left_df[join_right_column])),
                "left",
            )
            .na.drop()
            .withColumn(add_column, expr("null"))
            .drop(*drop_columns)
        )
        df = utils.rename_columns(
            df=df,
            old_columns=old_columns,
            new_columns=new_columns,
        )
        return df.select(select_columns)

    def _get_drug_df(self):
        return self.drug_dao.read()

    def _get_clinical_trial_df(self):
        return self.clinical_trial_dao.read()

    def _get_pubmed_df(self):
        return self.pubmed_dao.read()
