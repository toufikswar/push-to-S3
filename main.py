import pathlib
import os
import json
import argparse

from pathlib import Path
from jsonschema import validate, ValidationError
from boto3.exceptions import S3UploadFailedError
import pandas as pd

from utils import load_config, define_logger, arrange_df_headers, move_file
from classes.s3_controller import BucketController
from classes.json_validator import Validator


def main():

    my_parser = argparse.ArgumentParser(description="Push Content to S3 the easy way...")
    my_parser.add_argument('--config', help="Path to a JSON config file", required=True)
    my_parser.add_argument('--mapping_file', help="Pass as argument an Excel file made of 2 columns"
                                                  "METADATA and JSON. If no mapping file is provided, make sure"
                                                  "to have a folder for 'sample.json' files and one for "
                                                  "'sample-metadata.json' files in the config")
    # Parse the arguments
    args = my_parser.parse_args()

    # Load the config passed as argument
    config = load_config(args.config)

    log = define_logger(main=True, config=config)
    log.info("Push Content to S3 the easy way...")

    bucket_manager = BucketController(config)
    bucket_manager.create_bucket_obj()

    json_list = [os.path.join(root, file) for root, dirs, files in
                 os.walk(config['input_folder']) for file in files if file.endswith(".json") and
                 ("metadata_act_" not in file)]
    meta_list = [os.path.join(root, file) for root, dirs, files in
                 os.walk(config['input_folder']) for file in files if file.endswith(".json") and
                 ("metadata_act" in file)]

    values = [("JSON", "METADATA")]
    for json_file in json_list:
        metadata_file = next((x for x in meta_list if json_file.split("/act_")[1] in x), None)
        values.append((json_file, metadata_file))

    df = pd.DataFrame(values)
    df = arrange_df_headers(df)
    df.to_excel("test.xlsx")
    validator = Validator(config)
    for index, row in df.iterrows():
        if not pathlib.Path(row['METADATA']).exists():  # if no metadata file we go to next file
            log.error(f"Metadata file {row['METADATA']} not found. Skipping.")
            continue
        if not pathlib.Path(row['JSON']).exists():  # if no metadata file we go to next file
            log.error(f"JSON file {row['JSON']} not found. Skipping.")
            continue
        with open(row['METADATA']) as json_file:
            meta_data = json.load(json_file)
            validation_status = validator.valid_json(meta_data)
            if validation_status:
                log.info(f"Validated JSON schema for : {row['JSON']}")
            else:
                log.error(f"Cannot validate metadata file - Skipping")
                continue

        status_metadata = bucket_manager.upload_json_to_s3(row['METADATA'])
        status_json = bucket_manager.upload_json_to_s3(row['JSON'])

        if status_metadata:
            log.info(f"Successfully uploaded file {row['METADATA']} to S3 bucket")
            move_file(row['METADATA'], config, status_metadata)
        else:
            log.error(f"Could not uploaded file {row['METADATA']} to S3 bucket")
            move_file(row['METADATA'], config, status_metadata)

        if status_json:
            log.info(f"Successfully uploaded file {row['JSON']} to S3 bucket")
            move_file(row['JSON'], config, status_json)
        else:
            log.error(f"Could not uploaded file {row['JSON']} to S3 bucket")
            move_file(row['JSON'], config, status_json)


if __name__ == '__main__':
    main()
