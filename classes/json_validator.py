import json
from pathlib import Path
from jsonschema import validate, ValidationError

from utils import define_logger


class Validator:
    def __init__(self, config):
        self.log = define_logger()
        self.config = config
        self.schema = self._load_schema()

    def _load_schema(self):
        if Path(self.config['json_schema']).exists():
            with open(self.config['json_schema']) as json_file:  # Load JSON Schema for validation
                self.log.debug(f"Loading validation schema at {self.config.get('json_schema')}")
                schema = json.load(json_file)
            return schema
        return

    def valid_json(self, data):
        if self.schema:
            try:
                validate(instance=data, schema=self.schema)
            except ValidationError as ValError:
                self.log.exception(f"Cannot validate file {data} due to : {ValError}")
                return False
            except Exception as Err:
                self.log.exception(f"Cannot validate file {data} due to : {Err}")
                return False
            else:
                return True
        else:
            self.log.error(f"No validation schema found or provided")
            return

