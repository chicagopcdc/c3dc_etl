""" Unpivoter for to convert internal field mapping to deliverable transformation mapping """
import csv
import json
import logging
import logging.config
import os
import sys

from urllib.parse import urlparse, ParseResult
from urllib.request import url2pathname

import dotenv
import requests


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
            "filename": "mapping_unpivoter.log",
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

class MappingUnpivoter: #pylint: disable=too-few-public-methods
    """ Unpivot tabular transformation field mappings to JSON format """
    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._version: str = config.get('VERSION')
        self._json_schema_url: str = config.get('JSON_SCHEMA_URL')
        self._json_schema: dict[str, any] = {}
        self._node_type_properties: list[str] = []
        self._output_file: str = config.get('OUTPUT_FILE')
        self._transformation_mappings_files: list[dict[str, any]] = json.loads(
            config.get('TRANSFORMATION_MAPPINGS_FILES', '[]')
        )
        self._transformation_config: dict[str, any] = {'version': self._version, 'transformations': []}
        self._transformation_mappings: dict[str, list[dict[str, any]]] = {}

        self._load_json_schema()
        self._cache_node_type_properties()

    @staticmethod
    def url_to_path(url: str) -> str:
        """ Convert specified URL to path specific to local platform """
        url_parts: ParseResult = urlparse(url)
        host = f"{os.path.sep}{os.path.sep}{url_parts.netloc}{os.path.sep}"
        return os.path.normpath(os.path.join(host, url2pathname(url_parts.path)))

    @staticmethod
    def get_url_content(url: str) -> any:
        """ Retrieve and return contents from specified URL """
        url_content: str | bytes | bytearray
        if url.startswith('file://'):
            with open(MappingUnpivoter.url_to_path(url), 'rb') as local_file:
                url_content = local_file.read()
        else:
            with requests.get(url, timeout=30) as response:
                response.raise_for_status()
                url_content = response.content

        return json.loads(url_content)

    def unpivot_transformation_mappings(self) -> None:
        """ Unpivot tablular mapping data to JSON """
        _logger.info('Unpivoting transformation mappings')
        errors: list[str] = []
        err_msg: str
        self._transformation_config['transformations'] = self._transformation_config.get('transformation', [])
        self._transformation_config['transformations'].clear()
        transformation_mappings_file: dict[str, any]
        for transformation_mappings_file in self._transformation_mappings_files:
            _logger.info('Loading transformation mappings file %s', transformation_mappings_file['mappings_file'])
            with open(transformation_mappings_file['mappings_file'], encoding='utf-8') as csv_fp:
                reader: csv.DictReader = csv.DictReader(csv_fp)
                record: dict[str, any]
                unpivoted_mappings: list[dict[str, any]] = []
                for record in reader:
                    # unpivot pivoted mapping
                    unpivoted_mapping: dict[str, any] = self._unpivot_mapping(record)
                    if not unpivoted_mapping:
                        continue

                    # check for existing unpivoted mapping for this pivoted mapping's output field and type
                    # group, if found then this field mapping has multiple replacement values so append
                    existing_unpivoted_mappings: list[dict[str, any]] = [
                        m for m in unpivoted_mappings
                            if m['output_field'] == unpivoted_mapping['output_field'] and
                                m['type_group_index'] == unpivoted_mapping['type_group_index']
                    ]
                    if not existing_unpivoted_mappings:
                        unpivoted_mappings.append(unpivoted_mapping)
                    else:
                        source_field_count: int = len(set(m.get('source_field') for m in existing_unpivoted_mappings))
                        if source_field_count != 1:
                            err_msg = (
                                f'Invalid mapping: unexpected number of source fields ({source_field_count}) ' +
                                f'for output field {unpivoted_mapping["output_field"]}'
                            )
                            errors.append(err_msg)
                            continue

                        existing_unpivoted_mappings[0].get('replacement_values').extend(
                            unpivoted_mapping['replacement_values']
                        )

                transformation: dict[str, any] = {
                    'name': transformation_mappings_file['transformation_name'],
                    'mappings': unpivoted_mappings
                }
                self._transformation_config['transformations'].append(transformation)

        _logger.info('Saving unpivoted transformation mappings to %s', self._output_file)
        # save transformation config to output file specified in config
        with open(self._output_file, mode='w', encoding='utf-8') as json_fp:
            json.dump(self._transformation_config, json_fp, indent=4)

    def _load_json_schema(self) -> dict[str, any]:
        """ Load JSON schema from location specified in config """
        _logger.info('Loading JSON schema from %s', self._json_schema_url)
        self._json_schema = MappingUnpivoter.get_url_content(self._json_schema_url)
        return self._json_schema

    def _cache_node_type_properties(self) -> list[str]:
        """ Get list of node type properties in 'node_type.property_name' format and cache in instance var """
        _logger.info('Caching node type properties')

        if not self._json_schema:
            self._load_json_schema()
            if not self._json_schema:
                raise RuntimeError('Unable to cache node type properties; failed to load JSON schema')

        self._node_type_properties = []
        defs: dict[str, any] = self._json_schema.get('$defs', {})
        etl_node_name: str
        etl_node_obj: dict[str, any]
        # enumerate nodes in $defs
        for etl_node_name, etl_node_obj in defs.items():
            property_name: str
            # enumerate properties in 'properties'
            for property_name in etl_node_obj.get('properties', {}):
                self._node_type_properties.append(f'{etl_node_name}.{property_name}')
        if self._node_type_properties:
            _logger.info('Cached %d properties for %d node types', len(self._node_type_properties), len(defs.keys()))
        else:
            raise RuntimeError('No node type properties found to be cached')
        return self._node_type_properties

    def _unpivot_mapping(self, pivoted_mapping: dict[str, any]) -> dict[str, any]:
        """ Convert specified flat mapping loaded from tabular data file to JSON mapping """
        # if output field isn't set/defined then this mapping entry is for reference only and can be skipped
        target_variable_name: str = pivoted_mapping.get('Target Variable Name', '')
        if target_variable_name not in self._node_type_properties:
            _logger.warning(
                'Mapping has invalid "Target Variable Name", skipping: "%s"',
                pivoted_mapping.get('Target Variable Name', '')
            )
            return {}

        unpivoted_mapping: dict[str, any] = {}
        unpivoted_mapping['output_field']: str = pivoted_mapping['Target Variable Name']
        unpivoted_mapping['source_field']: str = pivoted_mapping['Source Variable Name']
        unpivoted_mapping['type_group_index']: str = str(pivoted_mapping['Type Group Index'])
        unpivoted_mapping['replacement_values']: list[dict[str, any]] = unpivoted_mapping.get('replacement_values', [])
        unpivoted_mapping['replacement_values'].append(
            {
                'old_value': json.loads(pivoted_mapping['Source Permissible Values Term'] or '""'),
                'new_value': json.loads(pivoted_mapping['Target Permissible Values Term'] or '""')
            }
        )
        return unpivoted_mapping

def print_usage() -> None:
    """ Print script usage """
    _logger.info('usage: python %s [optional config file name/path if not .env_mapping_unpivoter]', sys.argv[0])


def main() -> None:
    """ Script entry point """
    if len(sys.argv) > 2:
        print_usage()
        return

    config_file: str = sys.argv[1] if len(sys.argv) == 2 else '.env_mapping_unpivoter'
    if not os.path.exists(config_file):
        raise FileNotFoundError(f'Config file "{config_file}" not found')
    config: dict[str, str] = dotenv.dotenv_values(config_file)
    mapping_unpivoter: MappingUnpivoter = MappingUnpivoter(config)
    mapping_unpivoter.unpivot_transformation_mappings()


if __name__ == '__main__':
    main()
