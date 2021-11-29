from pyspark.sql.types import StructField, StructType, StringType

import src.utils.constants as constants

drug_schema = StructType(
    [
        StructField(constants.DRUG_ATCCODE, StringType()),
        StructField(constants.DRUG_NAME, StringType()),
    ]
)

clinical_trial_schema = StructType(
    [
        StructField(constants.CLINICAL_TRIAL_ID, StringType()),
        StructField(constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE, StringType()),
        StructField(constants.CLINICAL_TRIAL_DATE, StringType()),
        StructField(constants.CLINICAL_TRIAL_JOURNAL, StringType()),
    ]
)

pubmed_schema = StructType(
    [
        StructField(constants.PUBMED_ID, StringType()),
        StructField(constants.PUBMED_TITLE, StringType()),
        StructField(constants.PUBMED_DATE, StringType()),
        StructField(constants.PUBMED_JOURNAL, StringType()),
    ]
)
