""" Unpivoter for to convert internal field mapping to deliverable transformation mapping """
from __future__ import annotations

import csv
import hashlib
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

                    # check for consistent default values for this output field's mappings
                    if unpivoted_mapping['default_value'] is not None:
                        existing_default_values: set[any] = set()
                        default_value: any
                        for default_value in [
                            m['default_value'] for m in unpivoted_mappings
                                if m['output_field'] == unpivoted_mapping['output_field'] and
                                    m['default_value'] is not None
                        ]:
                            if not isinstance(default_value, (list, set, tuple)):
                                default_value = set([default_value])
                            existing_default_values.update(default_value)
                        if isinstance(unpivoted_mapping['default_value'], (list, set, tuple)):
                            existing_default_values.update(unpivoted_mapping['default_value'])
                        else:
                            existing_default_values.add(unpivoted_mapping['default_value'])
                        if len(existing_default_values) > 1:
                            raise RuntimeError(
                                f'Invalid mapping for output field "{unpivoted_mapping["output_field"]}", ' +
                                f'multiple default values specified: {existing_default_values}')

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

        # save transformation config to output file specified in config
        _logger.info('Saving unpivoted transformation mappings to %s', self._output_file)
        with open(self._output_file, mode='w', encoding='utf-8') as json_fp:
            json.dump(self._transformation_config, json_fp, indent=4)

        self._update_transformation_mapping_reference_file_mappings()
        _logger.info('Saving updated unpivoted transformation mappings to %s', self._output_file)
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
        unpivoted_mapping['output_field'] = pivoted_mapping['Target Variable Name']
        unpivoted_mapping['source_field'] = pivoted_mapping['Source Variable Name']
        unpivoted_mapping['type_group_index'] = str(pivoted_mapping['Type Group Index'])
        unpivoted_mapping['default_value'] = json.loads(pivoted_mapping['Default Value If Null/Blank'] or 'null')
        unpivoted_mapping['replacement_values'] = unpivoted_mapping.get('replacement_values', [])

        try:
            unpivoted_mapping['replacement_values'].append(
                {
                    'old_value': json.loads(pivoted_mapping['Source Permissible Values Term'] or '""'),
                    'new_value': json.loads(pivoted_mapping['Target Permissible Values Term'] or '""')
                }
            )

            repl_vals: dict[str, str] = json.loads(pivoted_mapping.get('Replacement Values', '{}') or '{}')
            replacement_values: list[dict[str, str]] = [{'old_value': k, 'new_value': v} for k,v in repl_vals.items()]
            unpivoted_mapping['replacement_values'].extend(replacement_values)
        except json.decoder.JSONDecodeError as err:
            _logger.error('Error loading replacement values while unpivoting mapping:')
            _logger.error(err)
            _logger.error(pivoted_mapping)
            raise
        return unpivoted_mapping

    def _get_ref_file_size_and_md5sum_mappings(
        self,
        mappings: list[dict[str, any]],
        type_group_index: str,
    ) -> list[dict[str, any]]:
        """ Get all 'reference_file.*' mappings in list of mappings for specified type group index value """
        ref_file_mappings: list[dict[str, any]] = [
            m for m in mappings
                if m.get('type_group_index') == type_group_index and
                    m.get('output_field', '').startswith('reference_file.')
        ]
        file_name_match: list[dict[str, any]] = [
            m for m in ref_file_mappings
                if m.get('output_field') == 'reference_file.file_name' and any(
                    r for r in m.get('replacement_values', [])
                        if r.get('new_value') == os.path.basename(self._output_file)
                )
        ]
        file_cat_match: list[dict[str, any]] = [
            m for m in ref_file_mappings
                if m.get('output_field') == 'reference_file.file_category' and any(
                    r for r in m.get('replacement_values', [])
                        if r.get('new_value') == 'transformation/mapping'
                )
        ]
        file_size_match: list[dict[str, any]] = [
            m for m in ref_file_mappings
                if m.get('output_field') == 'reference_file.file_size' and any(
                    r for r in m.get('replacement_values', [])
                        if r.get('new_value') == 0
                )
        ]
        file_md5sum_match: list[dict[str, any]] = [
            m for m in ref_file_mappings
                if m.get('output_field') == 'reference_file.md5sum' and any(
                    r for r in m.get('replacement_values', [])
                        if r.get('new_value') == ''
                )
        ]
        if file_name_match and file_cat_match and file_size_match and file_md5sum_match:
            return [
                m for m in ref_file_mappings
                    if m.get('output_field') in ('reference_file.file_size', 'reference_file.md5sum')
            ]
        return []


    def _update_transformation_mapping_reference_file_mappings(self) -> list[dict[str, any]]:
        """ Find mapping entries for reference file size and md5 sum in transformation config for output file """
        _logger.info('Updating transformation/mapping reference file values for file size and md5 sum')
        # get size and md5 hash of output file to set matching reference file properties
        mapping_file_size: int = os.path.getsize(self._output_file)

        md5: hashlib._Hash = hashlib.md5()
        with open(self._output_file, mode='rb') as json_fp:
            chunk: any
            for chunk in iter(lambda: json_fp.read(4096), b''):
                md5.update(chunk)
        mapping_file_md5sum: str = md5.hexdigest()

        ref_file_field_value_map: dict[str, any] = {
            'reference_file.file_size': mapping_file_size,
            'reference_file.md5sum': mapping_file_md5sum
        }

        output_file_name: str = os.path.basename(self._output_file)

        # update file size and md5 values of matching transformation/mapping ref file entries for each transformation
        for xform_cfg in self._transformation_config.get('transformations', []):
            reference_file_tgis: list[str] = []
            mapping: dict[str, any]
            # get type group index values of relevant transformation/mapping files
            # by matching reference file's file name property to our output file
            for mapping in [
                m for m in xform_cfg['mappings']
                    if m.get('type_group_index') and
                        m.get('output_field') == 'reference_file.file_name' and
                        any(r for r in m.get('replacement_values', []) if r.get('new_value') == output_file_name)
            ]:
                reference_file_tgis.append(mapping.get('type_group_index'))

            if not reference_file_tgis:
                _logger.warning(
                    'No reference file mapping for "%s" found in transformation "%s"',
                    output_file_name,
                    xform_cfg.get('name')
                )
            # set file size and md5sum values for each reference file set circumscribed by type group index
            tgi: str
            for tgi in set(reference_file_tgis):
                ref_file_size_md5sum_mappings: list[dict[str, any]] = self._get_ref_file_size_and_md5sum_mappings(
                    xform_cfg['mappings'],
                    tgi
                )
                output_field: str
                new_value: any
                for output_field, new_value in ref_file_field_value_map.items():
                    ref_file_mapping: dict[str, any] = [
                        m for m in ref_file_size_md5sum_mappings if m.get('output_field') == output_field
                    ]
                    if not ref_file_mapping:
                        raise RuntimeError(
                            f'Mapping for "{output_field}" not found for transformation {xform_cfg.get("name")} ' +
                            f'(type group index {tgi})'
                        )
                    ref_file_mapping = ref_file_mapping[0]
                    replacement_value: dict[str, any]
                    for replacement_value in ref_file_mapping.get('replacement_values', []):
                        if replacement_value['new_value'] not in ('', 0):
                            raise RuntimeError(
                                f'Unexpected value for "new_value" in mapping replacement_values: {ref_file_mapping}'
                            )
                        _logger.info(
                            (
                                'Setting "%s" replacement "new_value" properties to "%s" for ' +
                                    'type_group_index "%s" in transformation "%s"'
                            ),
                            output_field,
                            new_value,
                            tgi,
                            xform_cfg.get('name')
                        )
                        replacement_value['new_value'] = new_value


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
