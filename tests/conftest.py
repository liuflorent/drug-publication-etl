import pytest
import os
import shutil
import src.utils.utils as utils


@pytest.fixture(scope="session")
def spark_session():
    output_path = "tests/service/output"
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    return utils.get_spark_session("unittest-app")
