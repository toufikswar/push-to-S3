import pathlib

import boto3
import os
import json
import argparse
import shutil
import logging

from pathlib import Path
from jsonschema import validate, ValidationError
from boto3.exceptions import S3UploadFailedError
import pandas as pd

from utils import load_config
from classes.logger import Logger


def main():

    my_parser = argparse.ArgumentParser(description="Sloth! Why do it manually?")
    my_parser.add_argument('--config', help="Path to a JSON config file", required=True)
    my_parser.add_argument('--mapping_file', help="Pass as argument an Excel file made of 2 columns"
                                                  "METADATA and JSON. If no mapping file is provided, make sure"
                                                  "to have a folder for 'sample.json' files and one for "
                                                  "'sample-metadata.json' files in the config")
    # Parse the arguments
    args = my_parser.parse_args()

    # Load the config passed as argument
    config = load_config(args.config)

    logger = Logger(logging.DEBUG)
    logger.set_handler(file=True)
    logger.logger.info("Initializing Push to S3")
    session = boto3.Session(profile_name="cloud_dev_S3-ContentAdminBuiltin")
    s3 = session.resource("s3")
    buckets = [bucket.name for bucket in s3.buckets.all()]

    if config['bucket_name'] not in buckets:  # Exit if bucket not found in S3
        logger.logger.error(f"Bucket {config['bucket_name']} not found in S3. Program will close")
        exit(1)
    if not Path(config['json_schema']).exists():  # Exit if the JSON schema doesn't exist
        logger.logger.error("Invalid path for the JSON schema. Program will close.")
        exit(1)

    bucket = s3.Bucket(config['bucket_name'])

    with open(config['json_schema']) as json_file:  # Load JSON Schema for validation
        logger.logger.debug("Loading validation schema")
        schema = json.load(json_file)

    json_list = [(Path(file).stem, os.path.join(root, file)) for root, dirs, files in os.walk(config['json_folder'])
                 for file in files if file.endswith(".json")]

    meta_list = [os.path.join(root, file) for root, dirs, files in os.walk(config['meta_folder'])
                 for file in files if file.endswith(".json")]

    # In case no mapping file is provided, create one based 2 folders, JSON and METADATA, provided in config file
    if not args.mapping_file:
        results = []
        for json_item in json_list:
            for meta_item in meta_list:
                if json_item[0] in meta_item:
                    results.append((meta_item, json_item[1]))
        df = pd.DataFrame(results)
        df.columns = ["METADATA", "JSON"]  # set the header row as the df header
    else:
        df = pd.read_excel(args.mapping_file)

    for index, row in df.iterrows():
        if not pathlib.Path(row['METADATA']).exists():  # if no metadata file we go to next file
            logger.logger.error(f"Metadata file {row['METADATA']} not found. Going to next file.")
            continue
        with open(row['METADATA']) as json_file:
            meta_data = json.load(json_file)
            try:
                validate(instance=meta_data, schema=schema)
            except ValidationError as ValError:
                logger.logger.error(f"Cannot validate file due to : {ValError}")
            else:
                logger.logger.debug(f"Validated JSON schema for : {row['JSON']}")
            try:
                logger.logger.debug(f"Uploading file {row['JSON']}")
                with open(row['JSON'], "rb") as f:
                    bucket.put_object(
                        Body=f,  # read the file and load it
                        Key=os.path.split(row['JSON'])[1],  # Path(row['JSON']).stem + ".json",
                        ContentType="application/json",
                        Metadata={
                            'library_uuid': meta_data.get('library_uuid').strip(),
                            "min_mtp_version": json.dumps(meta_data.get('min_mtp_version'), separators=(',', ":")),
                            'latest_version': json.dumps(meta_data.get('latest_version'), separators=(',', ":")),
                            'version_history': json.dumps(meta_data.get('version_history'), separators=(',', ":")),
                            'name': meta_data.get('name').strip(),
                            'description': meta_data.get("description").replace("\n", "").strip(),
                            'type': "act".strip(),  # TODO change for different content types
                            'library_packs': json.dumps(meta_data.get('library_packs'), separators=(',', ":"))
                        }
                    )
                # Move both json and metadata file when successfully processed
                #json_filename = os.path.split(row['JSON'])[1]
                #meta_filename = os.path.split(row['METADATA'])[1]
                #shutil.move(row['JSON'], "output/" + json_filename)
                #shutil.move(row['METADATA'], "output/" + meta_filename)
            except S3UploadFailedError as S3UploadEx:
                logger.logger.error(f"Failed to upload file : {S3UploadEx}")
                continue
            except Exception as ex:
                logger.logger.error(f"Failed to upload file : {ex}")
                continue
            else:
                logger.logger.info(f"Successfully uploaded file : {row['JSON']} and its metadata")


if __name__ == '__main__':
    main()
