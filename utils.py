import json
import boto3


def load_config(path_to_json):
    with open(path_to_json, "r") as f:
        return json.load(f)



