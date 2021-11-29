import logging
import sys

from src.app import app

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        logging.error(
            "5 arguments expected: \n"
            "<drup_path> \n"
            "<clinical_trial_path>\n"
            "<pubmed_csv_path> \n"
            "<pubmed_json_path> \n"
            "<output_path>"
        )
        sys.exit()

    logging.info(sys.argv[:-1])
    app.start(sys.argv[1:])
