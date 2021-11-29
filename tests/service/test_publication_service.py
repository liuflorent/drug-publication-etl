import pytest
import os

from src.service import publication_service
from src.utils import constants

spark = pytest.mark.usefixtures("spark_session")

output_path = "tests/service/output"
input_path = "tests/service/input/"
publication_path = f"{output_path}/publication"
top_journals_path = f"{output_path}/top_journals"


class TestPublicationService(object):
    def test_execute(self, spark_session):
        drug_path = f"{input_path}drugs.csv"
        clinical_trial_path = f"{input_path}clinical_trials.csv"
        pubmed_csv_path = f"{input_path}pubmed.csv"
        pubmed_json_path = f"{input_path}pubmed.json"

        service = publication_service.PublicationService(
            spark=spark_session,
            drug_path=drug_path,
            clinical_trial_path=clinical_trial_path,
            pubmed_csv_path=pubmed_csv_path,
            pubmed_json_path=pubmed_json_path,
            output_path=output_path,
        )
        service.execute()
        assert os.path.exists(publication_path) == True
        assert os.path.exists(top_journals_path) == True

    def test_find_publication_result(self, spark_session):
        columns = [
            constants.DRUG_NAME,
            constants.JOURNAL,
            constants.CLINICAL_TRIAL,
            constants.PUBMED,
        ]

        df = spark_session.read.format("json").load(publication_path)
        result_df = df.select(columns)

        expected_data = [
            ("DIPHENHYDRAMINE", "Journal of emergency nursing", "yes", "no"),
            ("EPINEPHRINE", "Journal of emergency nursing", "yes", "no"),
            ("DIPHENHYDRAMINE", "The Journal of pediatrics", "no", "yes"),
            ("ISOPRENALINE", "Journal of photochemistry and photobiology. B, Biology", "no", "yes"),
        ]
        expected_df = spark_session.createDataFrame(expected_data, columns)

        assert set(result_df.collect()) == set(expected_df.collect())

    def test_find_top_journals_result(self, spark_session):
        columns = [constants.DRUGS_COUNT, constants.JOURNAL]

        df = spark_session.read.format("json").load(top_journals_path)
        result_df = df.select(columns)

        expected_data = [(2, "Journal of emergency nursing")]
        expected_df = spark_session.createDataFrame(expected_data, columns)

        assert set(result_df.collect()) == set(expected_df.collect())
