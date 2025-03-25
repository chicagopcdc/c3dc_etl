""" JSON Data Pivoter """
from copy import deepcopy
import json
import logging
import logging.config
import os
import sys
from typing import Generator
import warnings

import petl

# suppress openpyxl warning about inability to parse header/footer
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

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
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "json_pivoter.log",
            "mode": "w"
        }
    },
    "loggers": {
        "": { # root logger
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        },
        "__main__": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        }
    }
})


def is_list_of_scalars(val: any) -> bool:
    """ determine whether specified value is list of scalars (["1", "2"] or [1, 2] etc) """
    return isinstance(val, list) and val and all(isinstance(v, (str, int, float)) for v in val)


# solution adapted from https://stackoverflow.com/a/63500540 ("Convert nested JSON to CSV file in Python")
def cross_join(left: list[dict[str, any]], right: list[dict[str, any]]) -> list[dict[str, any]]:
    """ return cross-join (cartesian) of two lists of dictionaries """
    new_rows: list[dict[str, any]] = [] if right else left
    left_row: dict[str, any]
    for left_row in left:
        right_row: dict[str, any]
        for right_row in right:
            temp_row: dict[str, any] = deepcopy(left_row)
            key: str
            value: any
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data: list[any]) -> Generator[any, any, None]:
    """ flatten specified input collection """
    elem: any
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def flatten_json(data: any, prev_heading: str='') -> list[dict[str, any]]:
    """ flatten specified json data with optional previous heading prefixed to key names """
    rows: list[dict[str, any]]
    if isinstance(data, dict):
        rows = [{}]
        key: str
        value: any
        for key, value in data.items():
            rows = cross_join(rows, flatten_json(value, f'{prev_heading}.{key}'))
    elif isinstance(data, list):
        if is_list_of_scalars(data):
            # collate string arrays such as 'final_diagnosis', 'fusion_tier_one_or_two_result.summary', etc
            return [{prev_heading[1:]: '\r\n'.join(data)}]

        rows = []
        item: dict[str, any]
        for item in data:
            elem: any
            for elem in flatten_list(flatten_json(item, prev_heading)):
                rows.append(elem)
    else:
        rows = [{prev_heading[1:]: data}]
    return rows


def pivot_json_file_to_xlsx(input_path: str, output_path: str) -> None:
    """ Convert (pivot) JSON file(s) at specified source path into XLSX file at output path """
    # collate source files if directory specified instead of individual file
    if not os.path.isfile(input_path) or not input_path.endswith('.json'):
        raise RuntimeError(f'"{input_path}" not a valid JSON file')

    _logger.info('Pivoting "%s" to XLSX: "%s"', input_path, output_path)

    with open(input_path, encoding='utf-8') as fp:
        json_data: list[dict[str, any]] = flatten_json(json.load(fp))
        header: dict[str, str] = {}
        record: dict[str, any]
        for record in json_data:
            header.update(record)
        tbl: any = petl.fromdicts(json_data, header=list(header))
        petl.toxlsx(tbl, output_path)

    _logger.info('JSON to XLS pivot complete')


def print_usage() -> None:
    """ Print script usage """
    _logger.info('usage: python %s [source file or directory] [output file]', sys.argv[0])


def main() -> None:
    """ Script entry point """
    if len(sys.argv) != 3:
        print_usage()
        return

    source_path: str = sys.argv[1]
    output_path: str = sys.argv[2]
    pivot_json_file_to_xlsx(source_path, output_path)


if __name__ == '__main__':
    main()
