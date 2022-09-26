import pathlib

import boto3
import os
import json
import argparse
import shutil

from pathlib import Path
from jsonschema import validate, ValidationError
from boto3.exceptions import S3UploadFailedError
import pandas as pd

from utils import load_config


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

    s3 = boto3.client('s3')  # Create an S3 client
    response = s3.list_buckets()  # get the S3 bucket list
    bucket_list = [bucket_dict.get('Name') for bucket_dict in response.get('Buckets')]  # select bucket names only

    # Exit if bucket not found in S3
    if config['bucket_name'] not in bucket_list:
        print(f"Bucket {args.bucket} not found in S3")
        exit(1)
    # Exit if the JSON schema doesn't exist
    if not Path(config['json_schema']).exists():
        print("Invalid path for the JSON schema. Program will close.")
        exit(1)
    # Load JSON Schema for validation
    with open(config['json_schema']) as json_file:
        schema = json.load(json_file)

    json_list = [(Path(file).stem, root + "/" + file) for root, dirs, files in os.walk(config['json_folder'])
                 for file in files]
    meta_list = [root + "/" + file for root, dirs, files in os.walk(config['meta_folder'])
                 for file in files]

    # In case no mapping file is provided, create one based one JSON and METADATA folders
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
            continue
        with open(row['METADATA']) as json_file:
            data = json.load(json_file)
            try:
                validate(instance=data, schema=schema)
            except ValidationError as ValError:
                print(ValError)
            else:
                print(row['METADATA'] + " validated successfully")

            try:
                s3.upload_file(
                    Filename=row['JSON'],
                    Bucket=config['bucket_name'],
                    Key=data.get('name'),
                    ExtraArgs={
                        'Metadata':
                            {
                                'library_uuid': json.dumps(data.get('library_uuid'),
                                                           separators=(',', ":")),
                                "min_mtp_version": json.dumps(data.get('min_mtp_version'),
                                                              separators=(',', ":")),
                                'latest_version': json.dumps(data.get('latest_version'),
                                                             separators=(',', ":")),
                                'version_history': json.dumps(data.get('version_history'),
                                                              separators=(',', ":")),
                                'name': json.dumps(data.get('name'),
                                                   separators=(',', ":")),
                                'description': json.dumps(data.get("description").replace("\n", ""),
                                                          separators=(',', ":")),
                                'type': json.dumps(data.get('type'),
                                                   separators=(',', ":")),
                                'library_type': json.dumps(data.get('library_type'),
                                                           separators=(',', ":"))
                            }
                    }
                )

                # Move both json and metadata file when successfully processed
                json_filename = os.path.split(row['JSON'])[1]
                meta_filename = os.path.split(row['METADATA'])[1]
                shutil.move(row['JSON'], "output/" + json_filename)
                shutil.move(row['METADATA'], "output/" + meta_filename)

            except S3UploadFailedError as S3UploadEx:
                print(S3UploadEx)
                continue
            except Exception as ex:
                print(ex)
                continue


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
