import logging
import os

from src.service import publication_service
import src.utils.utils as utils

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)


def check_files_exist(argv: list):
    """
    Check if the input files exist
    :param: the list of the input files paths
    :type: list
    """
    error = []
    for file in argv[:-1]:
        if not os.path.exists(file):
            error.append(f"file: {file} no found")
    if len(error) > 0:
        raise Exception(str(error))


def start(argv: list):
    """
    Start & Stop a pyspark.sql session and start the publication service
    :param: the list of the input files paths and output path
    :type: list
    """
    logging.info(f"argv: {argv}")
    check_files_exist(argv)
    drug_path = argv[0]
    clinical_trial_path = argv[1]
    pubmed_csv_path = argv[2]
    pubmed_json_path = argv[3]
    output_path = argv[4]

    spark = utils.get_spark_session("publication-app")
    service = publication_service.PublicationService(
        spark=spark,
        drug_path=drug_path,
        clinical_trial_path=clinical_trial_path,
        pubmed_csv_path=pubmed_csv_path,
        pubmed_json_path=pubmed_json_path,
        output_path=output_path,
    )
    result = service.execute()
    logging.info(result)
    spark.stop()


if __name__ == "__main__":
    input = "input"
    drug_path = f"../../{input}/drugs.csv"
    clinical_trial_path = f"../../{input}/clinical_trials.csv"
    pubmed_csv_path = f"../../{input}/pubmed.csv"
    pubmed_json_path = f"../../{input}/pubmed.json"
    output_path = "../../output"

    argv = [drug_path, clinical_trial_path, pubmed_csv_path, pubmed_json_path, output_path]
    start(argv)
