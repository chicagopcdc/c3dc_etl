""" Validate JSON data conforms to schema specification """
import json
import logging
import logging.config
import sys

import jsonschema
from jsonschema import ValidationError


_logger = logging.getLogger(__name__)
if _logger.hasHandlers():
    _logger.handlers.clear()

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": { 
        "standard": { 
            "format": "%(asctime)s [%(levelname)s]: %(message)s"
        }
    },
    "handlers": { 
        "console": { 
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "validate_json.log",
            "mode": "w"
        }
    },
    "loggers": { 
        "": {  # root logger
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        },
        "__main__": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        }
    }
})


def validate(
    schema_file_path: str,
    data_file_path: str,
    schema_file_encoding: str='utf-8',
    data_file_encoding: str='utf-8'
) -> None:
    """ Validate data file against schema file """

    schema: any = None
    with open(schema_file_path, 'r', encoding=schema_file_encoding) as schema_file:
        schema = json.load(schema_file)

    json_data: any = None
    with open(data_file_path, 'r', encoding=data_file_encoding) as input_file:
        json_data = json.load(input_file)

    _logger.info('Validating data file %s against schema file %s', data_file_path, schema_file_path)
    try:
        jsonschema.validate(instance=json_data, schema=schema)
        _logger.info('Validation succeeded')
    except ValidationError as verr:
        _logger.critical('Validation failed:')
        _logger.critical(verr.message)


def print_usage() -> None:
    """ Print script usage """ 
    print(f'usage: python {sys.argv[0]} [schema file] [data file]')


def main() -> None:
    """ Script entry point """
    if len(sys.argv) != 3:
        print_usage()
        return

    validate(schema_file_path=sys.argv[1], data_file_path=sys.argv[2])


if __name__ == '__main__':
    main()
