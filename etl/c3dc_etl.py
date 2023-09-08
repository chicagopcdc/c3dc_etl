""" C3DC ETL File Creator """
from __future__ import annotations
from enum import Enum
import json
import logging
import logging.config
import os
import pathlib
import random
import re
import sys
import uuid
from urllib.parse import urlparse, ParseResult
from urllib.request import url2pathname
import warnings

import dotenv
import jsonschema
from jsonschema import ValidationError
import petl
import requests

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
            "filename": "c3dc_etl.log",
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


class C3dcEtlModelNode(str, Enum):
    """
    Enum class for ETL timing sub-types
    """
    DIAGNOSIS = 'diagnosis'
    PARTICIPANT = 'participant'
    REFERENCE_FILE = 'reference_file'
    SAMPLE = 'sample'
    STUDY = 'study'

    def __str__(self):
        return self.value

    @staticmethod
    def get(node_type: str) -> C3dcEtlModelNode:
        """ Get C3dcModelNode matching specified node_type or None if not found """
        try:
            return C3dcEtlModelNode[node_type.upper()]
        except KeyError:
            return None


class C3dcEtl:
    """ Build C3DC json ETL file from (XLSX) source file """
    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._json_schema_url: str = config.get('JSON_SCHEMA_URL')
        self._json_schema: dict[str, any] = {}
        self._json_schema_nodes: dict[str, any] = {}
        self._json_etl_data_sets: dict[str, any] = {}
        self._raw_etl_data_tables: dict[any] = {}
        self._random: random.Random = random.Random()

        # Remote study config should contain env-agnostic info like mappings, local study config
        # should contain info specific to the local env like file paths; remote and local will be
        # merged together to form the final study configuration object
        self._study_configurations: list[dict[str, any]] = json.loads(config.get('STUDY_CONFIGURATIONS', '[]'))
        self._study_configurations = [sc for sc in self._study_configurations if sc.get('active', True)]

        self.load_json_schema()
        self.load_transformations()
        self._assert_valid_study_configurations()

    @property
    def json_etl_data(self) -> dict[str, any]:
        """
        Get internal JSON ETL data object, building if needed
        """
        if not self._json_etl_data_sets:
            study_configuration: dict[str, any]
            for study_configuration in self._study_configurations:
                self._transform_study(study_configuration)
        return self._json_etl_data_sets

    @property
    def raw_etl_data_tables(self) -> dict[str, any]:
        """
        Get internal schema object, building if needed
        """
        if not self._raw_etl_data_tables:
            study_configuration: dict[str, any]
            for study_configuration in self._study_configurations:
                self._load_study_data(study_configuration)
        return self._raw_etl_data_tables

    @staticmethod
    def url_to_path(url: str) -> str:
        """ Convert specified URL to path specific to local platform """
        url_parts: ParseResult = urlparse(url)
        host = f"{os.path.sep}{os.path.sep}{url_parts.netloc}{os.path.sep}"
        return os.path.normpath(os.path.join(host, url2pathname(url_parts.path)))

    @staticmethod
    def get_url_content(url: str) -> any:
        """ Retrieve and return contents from specified URL """
        json_data: any
        if url.startswith('file://'):
            local_path: str = C3dcEtl.url_to_path(url)
            with open(local_path, 'r', encoding='utf-8') as local_file:
                json_data = json.load(local_file)
        else:
            with requests.get(url, timeout=30) as response:
                response.raise_for_status()
                json_data = json.loads(response.content)
        return json_data

    @staticmethod
    def guess_file_encoding(file_path: str) -> str:
        """ 'guess' and return the encoding for the specified file """
        # try to open as utf-8-sig first to handle BOM (\ufeff) if present
        encodings: list[str] = ['utf-8-sig', 'utf-8', 'iso-8859-1', 'windows-1252']
        encoding: str
        for encoding in encodings:
            try:
                with open(file_path, mode='r', encoding=encoding) as file:
                    file.readlines()
                    file.seek(0)
                if encoding != encodings[0]:
                    _logger.info('Detected encoding: %s', encoding)
                return encoding
            except UnicodeDecodeError:
                _logger.warning(
                    'Unable to open %s with encoding %s%s',
                    file_path, encoding,
                    ', retrying with different encoding' if encoding != encodings[-1] else ''
                )
        return None

    @staticmethod
    def get_petl_table_from_source_file(source_file_path: str) -> any:
        """ Load and return PETL table for data within specified source file """
        tbl: any = None
        source_file_extension: str = pathlib.Path(source_file_path).suffix.lower()
        if source_file_extension in ['.csv', '.tsv']:
            encoding: str = C3dcEtl.guess_file_encoding(source_file_path)
            if encoding is None:
                _logger.warning('Unable to detect encoding for %s', source_file_path)
            if source_file_extension == '.csv':
                tbl = petl.fromcsv(source_file_path, encoding=encoding)
            elif source_file_extension == '.tsv':
                tbl = petl.fromtsv(source_file_path, encoding=encoding)
        elif source_file_extension == '.xlsx':
            tbl = petl.fromxlsx(source_file_path)
        else:
            raise RuntimeError(f'Unsupported source file type/extension: {source_file_path}')
        # remove columns without headers
        tbl = petl.cut(tbl, [h for h in petl.header(tbl) if (h or '').strip()])
        return tbl


    def load_transformations(self, save_local_copy: bool = False) -> dict[str, any]:
        """ Download JSON transformations from configured URLs and merge with local config """

        # enumerate each per-study transformation config object in local config
        st_index: int
        study_config: list[dict[str, any]]
        for st_index, study_config in enumerate(self._study_configurations):
            # load remote study config containing transformations and mappings
            remote_transforms: dict[str, any] = C3dcEtl.get_url_content(study_config.get('transformations_url'))
            if save_local_copy:
                with open(f'./{os.path.basename(urlparse(study_config.get("transformations_url")))}', 'wb') as file:
                    json.dump(remote_transforms, file)

            # match by 'name' to merge remote with local transformation config sections containing e.g. paths
            remote_transforms = [t for t in remote_transforms.get('transformations') if t.get('active', True)]
            rt_index: int
            remote_transform: dict[str, any]
            for rt_index, remote_transform in enumerate(remote_transforms):
                transform_config: dict[str, any] = next(
                    (
                        t for t in study_config.get('transformations', [])
                            if t.get('name') and t.get('name') == remote_transform.get('name')
                    ),
                    {}
                )
                _logger.info('updating transformation at index %d for study configuration %d', rt_index, st_index)
                transform_config.update(remote_transform)

            # log warnings for any unmatched transformations
            unmatched_transforms: list[dict[str, any]] = [
                st for st in study_config.get('transformations', [])
                    if st.get('name') not in [rt.get('name') for rt in remote_transforms]
            ]
            unmatched_transform: dict[str, any]
            for unmatched_transform in unmatched_transforms:
                _logger.warning(
                    'Local transformations config entry "%s" (%s) not found in remote transformations config',
                    unmatched_transform.get('name'),
                    study_config.get('study')
                )

        return self._study_configurations

    def load_json_schema(self, save_local_copy: bool = False) -> dict[str, any]:
        """ Download JSON schema from configured URL """
        self._json_schema = C3dcEtl.get_url_content(self._json_schema_url)
        if save_local_copy:
            with open(f'./{os.path.basename(urlparse(self._json_schema_url).path)}', 'wb') as file:
                json.dump(self._json_schema, file)

        if not self._json_schema:
            raise RuntimeError('Unable to get node types, JSON schema not loaded')
        if '$defs' not in self._json_schema:
            raise RuntimeError('Unable to get node types, JSON schema does not contain root-level "$defs" property')

        # cache schema objects matching C3DC model nodes
        self._json_schema_nodes = {k:v for k,v in self._json_schema['$defs'].items() if C3dcEtlModelNode.get(k)}
        return self._json_schema

    def validate_json_etl_data(self) -> None:
        """ Validate JSON ETL data resulting from study configurations specified in config """
        are_etl_data_sets_valid: bool = True
        study_configuration: dict[str, any]
        for study_configuration in self._study_configurations:
            transformation_name: str
            for transformation_name in [t.get('name') for t in study_configuration.get('transformations', [])]:
                are_etl_data_sets_valid = (
                    self._is_json_etl_data_valid(study_configuration.get('study'), transformation_name)
                    and
                    are_etl_data_sets_valid
                )
        return are_etl_data_sets_valid

    def create_json_etl_files(self) -> None:
        """ Create ETL files and save to output paths for study configurations specified in conf """
        study_configuration: dict[str, any]
        for study_configuration in self._study_configurations:
            transformation: dict[str, any]
            for transformation in study_configuration.get('transformations', []):
                self._create_json_etl_file(study_configuration.get('study'), transformation)

    def _generate_uuid(self) -> uuid.UUID:
        """ Generate and return UUID(v4) using internal RNG that may be seeded for idempotent values """
        return uuid.UUID(int=self._random.getrandbits(128), version=4)

    def _is_json_etl_data_valid(self, study_id: str, transformation_name: str) -> bool:
        """ Validate JSON ETL data for specified transformation against JSON schema """
        _logger.info('Validating JSON ETL data for transformation %s (study %s)', transformation_name, study_id)
        log_msg: str
        if not self._json_etl_data_sets.get(study_id, {}).get(transformation_name):
            _logger.warning('No JSON ETL data loaded to validate')
            return False

        if not self._json_schema:
            self.load_json_schema()
            if not self._json_schema:
                log_msg = f'Unable to download JSON schema from "{self._json_schema_url}" to perform validation'
                _logger.critical(log_msg)
                raise RuntimeError(log_msg)

        try:
            jsonschema.validate(
                instance=self._json_etl_data_sets[study_id][transformation_name],
                schema=self._json_schema
            )
            _logger.info(
                'Validation succeeded for JSON ETL data for transformation %s (study %s)',
                transformation_name,
                study_id
            )
            return True
        except ValidationError as verr:
            _logger.warning(
                'ETL data for transformation %s (study %s) failed schema validation:',
                transformation_name,
                study_id
            )
            _logger.warning(verr.message)
        return False

    def _save_json_etl_data(self, study_id: str, transformation: dict[str, any]) -> None:
        """ Save JSON ETL data for specified transformation to designated output file """
        with open(transformation.get('output_file_path'), 'w', encoding='utf-8') as output_file:
            _logger.info('Saving ETL data to %s', transformation.get('output_file_path'))
            json.dump(self._json_etl_data_sets[study_id][transformation.get('name')], output_file, indent=2)

    def _load_source_data(self, study_id: str, transformation: dict[str, any]) -> any:
        """ Load raw ETL data from source file specified in config """
        raw_etl_data_tbl: any = C3dcEtl.get_petl_table_from_source_file(transformation.get('source_file_path'))

        # add row numbers for logging/auditing
        raw_etl_data_tbl = petl.addrownumbers(raw_etl_data_tbl, start=2, field='source_file_row_num')
        self._raw_etl_data_tables[study_id] = self._raw_etl_data_tables.get(study_id, {})
        self._raw_etl_data_tables[study_id][transformation.get('name')] = raw_etl_data_tbl
        return self._raw_etl_data_tables[study_id][transformation.get('name')]

    def _load_study_data(self, study_configuration: dict[str, any]) -> any:
        """ Load raw ETL data from source files specified in study config """
        transformation: dict[str, any]
        for transformation in study_configuration.get('transformations', []):
            self._load_source_data(study_configuration.get('study'), transformation)
        return self._raw_etl_data_tables[study_configuration.get('study')]

    def _get_json_schema_node_properties(self, node_type: C3dcEtlModelNode) -> dict[str, any]:
        """ Get properties for specified node in JSON schema  """
        if not self._json_schema_nodes:
            raise RuntimeError('Unable to get JSON schema nodes')
        if node_type not in self._json_schema_nodes:
            raise RuntimeError(f'Unable to get JSON schema node for type "{node_type}"')
        if 'properties' not in self._json_schema_nodes[node_type]:
            raise RuntimeError(f'"properties" not found in JSON schema node for type "{node_type}"')
        return self._json_schema_nodes[node_type]['properties']

    def _get_replacement_new_value_errors(
        self,
        source_header: list[str],
        study_id: str,
        transformation_name: str,
        replacement_new_value: any
    ) -> list[str]:
        """
        Get errors for specified transformation mapping replacement value (new_value), ignoring allowed
        values such as for phs_accession (literal strings within square brackets e.g. "['phs123456']").
        Errors may occur in specification of macro-like substitutions contained within square brackets as below:
        [uuid] => substitute with a UUID (v4)
        [field:{source field name}] => substitute with specified record's source field value, e.g. [field:TARGET USI]
        """
        if '[' not in str(replacement_new_value) and ']' not in str(replacement_new_value):
            return []

        errors: list[str] = []
        macro: str
        for macro in re.findall(r'\[.*?\]', replacement_new_value):
            macro_text: str = macro.strip(' []').strip()
            # source field to be replaced will be specified as '[field: FIELD_NAME]
            if not (
                (macro_text.startswith('"') and macro_text.endswith('"')) or
                (macro_text.startswith("'") and macro_text.endswith("'")) or
                macro_text.lower() == 'uuid' or
                (
                    macro_text.lower().startswith('field:')
                    and
                    macro_text[len('field:'):] in source_header
                )
            ):
                errors.append(f'{transformation_name} ({study_id}): invalid replacement macro: {replacement_new_value}')

        return errors

    def _get_transformation_mapping_errors(
        self,
        source_header: list[str],
        study_id: str,
        transformation_name: str,
        mapping: dict[str, any]
    ) -> list[str]:
        """ Get errors for specified transformation mapping """
        errors: list[str] = []

        source_field: str = mapping.get('source_field')
        if not source_field:
            errors.append(f'{transformation_name} ({study_id}): mapping source field not specified: {mapping}')
        if source_field != '[string_literal]' and source_field not in source_header:
            errors.append(f'{transformation_name} ({study_id}): source field not present in source data: {mapping}')
        output_field: str = mapping.get('output_field')
        if not output_field:
            errors.append(f'{transformation_name} ({study_id}): mapping output field not specified: {mapping}')
        output_field_parts = output_field.split('.')
        output_node: str = output_field_parts.pop(0)
        output_property: str = '.'.join(output_field_parts)
        if (
            output_node not in self._json_schema_nodes or
            output_property not in self._json_schema_nodes.get(output_node, '{}').get('properties', {})
        ):
            errors.append(f'{transformation_name}: mapping output field invalid: {mapping}')

        replacement_entry: dict[str, str]
        for replacement_entry in mapping.get('replacement_values', []):
            old_value: str = replacement_entry.get('old_value', '*')
            new_value: any = replacement_entry.get('new_value', [])
            if not new_value or not old_value:
                errors.append(
                    f'{transformation_name}: replacement entry missing new or old value: ' +
                    str(replacement_entry)
                )

            if source_field == '[string_literal]' and old_value not in ('+', '*'):
                errors.append(
                    f'{transformation_name}: replacement entry has invalid old value for ' +
                    f'string literal source: {mapping}'
                )

            # new_value can be list or scalar so handle scalars as single-valued lists and enumerate all
            new_value = [new_value] if not isinstance(new_value, (list, set, tuple)) else new_value
            new_sub_value: str
            for new_sub_value in new_value:
                errors.extend(
                    self._get_replacement_new_value_errors(source_header, study_id, transformation_name, new_sub_value)
                )

        return errors

    def _get_transformation_errors(self, study_id: str, index: int, transformation: dict[str, any]) -> list[str]:
        """ Get errors for specified transformation """
        errors: list[str] = []
        required_properties: tuple[str, ...] = ('name', 'source_file_path', 'output_file_path', 'mappings')
        if any(not transformation.get(p) for p in required_properties):
            errors.append(
                f'Study {study_id}, transformation {index + 1}: one or more of "{required_properties}" missing/invalid'
            )

        if transformation.get('source_file_path') and not os.path.isfile(transformation.get('source_file_path')):
            errors.append(
                f'{transformation.get("name")} ({study_id}): invalid source file ' +
                f'"{transformation.get("source_file_path")}"'
            )

        if (
            transformation.get('name')
            and
            not (self._raw_etl_data_tables or {}).get(study_id, {}).get(transformation.get('name'))
        ):
            if transformation.get('source_file_path') and os.path.isfile(transformation.get('source_file_path')):
                self._load_source_data(study_id, transformation)
            if not (self._raw_etl_data_tables or {}).get(study_id, {}).get(transformation.get('name')):
                errors.append(f'{transformation.get("name")}: unable to load source data')

        if not errors:
            source_header: list[str] = petl.header(self._raw_etl_data_tables[study_id][transformation.get('name')])
            mapping: dict[str, any]
            for mapping in transformation['mappings']:
                mapping_errors: list[str] = self._get_transformation_mapping_errors(
                    source_header, study_id, transformation.get('name'), mapping
                )
                errors.extend(mapping_errors)
        return errors

    def _get_study_configuration_errors(self, index: int, study_configuration: dict[str, any]) -> list[str]:
        """ Get errors for specified study configuration """
        errors: list[str] = []
        required_properties: tuple[str, ...] = ('study', 'transformations_url', 'transformations')
        if any(not study_configuration.get(p) for p in required_properties):
            errors.append(f'Study configuration {index + 1}: one or more of "{required_properties}" missing/invalid')
        t_index: int
        transformation: dict[str, any]
        for t_index, transformation in enumerate(study_configuration.get('transformations')):
            errors.extend(self._get_transformation_errors(study_configuration.get('study'), t_index, transformation))
        return errors

    def _assert_valid_study_configurations(self) -> None:
        """ Assert that study configurations specified in config are invalid else raise exception(s) """
        _logger.info('Validating study configurations')
        if not self._study_configurations:
            raise RuntimeError('No study configurations to validate')

        errors: list[str] = []

        if len(self._study_configurations) != len({st.get('study', '') for st in self._study_configurations}):
            raise RuntimeError('Duplicate study ids found in study configurations')

        sc_index: int
        study_configuration: dict[str, any]
        for sc_index, study_configuration in enumerate(self._study_configurations):
            errors.extend(self._get_study_configuration_errors(sc_index, study_configuration))

        error: str
        for error in errors:
            _logger.error(error)
        if errors:
            raise RuntimeError('Invalid transformation(s) found')

    def _get_mapped_output_value(
        self,
        mapping: dict[str, any],
        source_record: dict[str, any]
    ) -> any:
        """
        Get output value for specified mapping and source record.
        
        Mappings can specify old values to be replaced that are explicit values (replace on exact match) or
        wildcards using '+' or '*', where '+' results in replacement only if there is an existing value and
        '*' always results in replacement, whether the existing value is populated or null/blank. The new value
        can be specified with explicit scalar values or macro-like substitution directives as specified below:
        [uuid] => substitute with a UUID (v4 w/ optional seed value for RNG from config)
        [field:{source field name}] => substitute with specified record's source field value, e.g. [field:TARGET USI]
        """
        output_value: any = None

        source_field: str = mapping.get('source_field')
        source_value: str = source_record.get(source_field, None)

        replacement_entry: dict[str, str]
        for replacement_entry in mapping.get('replacement_values', []):
            old_value: str = replacement_entry.get('old_value', '*')
            new_value: any = replacement_entry.get('new_value', None)

            new_vals: list[any] = new_value if isinstance(new_value, (list, set, tuple)) else [new_value]
            new_val: any
            for i, new_val in enumerate(new_vals):
                if '[' not in str(new_val) and ']' not in str(new_val):
                    continue
                macros: list[str] = re.findall(r'\[.*?\]', new_val)
                macro: str
                for macro in macros:
                    macro_text: str = macro.strip(' []').strip()
                    if macro_text.lower().startswith('field:'):
                        # source field to be replaced will be specified as '[field: FIELD_NAME]
                        macro_field: str = macro_text[len('field:'):]
                        if macro_field in source_record:
                            new_val = new_val.replace(macro, source_record[macro_field])
                    elif macro_text.lower() == 'uuid':
                        new_val = new_val.replace(macro, str(self._generate_uuid()))
                new_vals[i] = new_val
            new_value = new_vals if isinstance(new_value, (list, set, tuple)) else new_vals[0]
            if (
                source_field == '[string_literal]' or
                old_value == '*' or
                (old_value == '+' and source_value) or
                (str(source_value) or '').lower() == (str(old_value) or '').lower()
            ):
                output_value = new_value
                break

        return output_value

    def _transform_record_default(
        self,
        transformation: dict[str, any],
        node_type: C3dcEtlModelNode,
        source_record: dict[str, any] = None
    ) -> dict[str, any]:
        """ Transform and return result after applying non-customized transformation to specified source record """
        source_record = source_record or {}
        output_record: dict[str, any] = {}
        mappings: list[dict[str, any]] = [
            t for t in transformation.get('mappings') if t.get('output_field', '').startswith(f'{node_type}.')
        ]
        mapping: dict[str, any]
        for mapping in mappings:
            source_field: str = mapping.get('source_field')
            source_value: str = source_record.get(source_field, None)

            replacement_entries: list[dict[str, str]] = mapping.get('replacement_values', [])
            allowed_values: list[str] = [
                r['old_value'] for r in replacement_entries if r.get('old_value', '') not in ('+', '*')
            ]
            if allowed_values and source_value not in allowed_values:
                log_msg = (
                    '"{source_value}" not specified as allowed value (old_value) in transformation for ' +
                    'field "{source_field}", skipping creation of {node_type} record for source record ' +
                    'in row {source_file_row_num}'
                ).format(
                    source_value=source_value,
                    source_field=source_field,
                    node_type=node_type,
                    source_file_row_num=source_record['source_file_row_num']
                )
                _logger.warning(log_msg)
                return None

            output_value: any = self._get_mapped_output_value(mapping, source_record)
            output_field: str = mapping.get('output_field')[len(f'{node_type}.'):]
            output_record[output_field] = output_value or source_value
        return output_record

    def _build_node(
        self,
        transformation: dict[str, any],
        node_type: C3dcEtlModelNode,
        source_record: dict[str, any] = None
    ) -> dict[str, any]:
        """
        Build and return specified C3DC model node type. If a custom method named '_transform_record_{node_type}'
        is found then that will be called, otherwise the default base method will be called for all node types
        """
        transform_method_name: str = f'_transform_record_{node_type}'
        transform_method: any = getattr(self, transform_method_name, lambda: None)
        if (
            transform_method is None or
            not hasattr(self, transform_method_name) or
            not callable(transform_method)
        ):
            return self._transform_record_default(transformation, node_type, source_record)

        return transform_method(source_record)

    def _transform_source_data(self, study_id: str, transformation: dict[str, any]) -> dict[str, any]:
        """ Transform and return ETL data transformed using rules specified in config """
        if (
            not self._raw_etl_data_tables.get(study_id, {}).get(transformation.get('name'), {})
            or
            not petl.nrows(self._raw_etl_data_tables[study_id][transformation.get('name')])
        ):
            self._load_source_data(study_id, transformation)
            if not petl.nrows(self._raw_etl_data_tables[study_id][transformation.get('name')]):
                raise RuntimeError(f'No data loaded to transform for study {study_id}')

        nodes: dict[str, any] = {
            'diagnoses': [],
            'participants': [],
            'studies': []
        }

        study: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.STUDY)
        study['participant.participant_id'] = []

        rec: dict[str, any]
        for rec in petl.dicts(self._raw_etl_data_tables[study_id][transformation.get('name')]):
            diagnosis: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.DIAGNOSIS, rec)
            if not diagnosis:
                _logger.warning(
                    '%s (%s): Unable to build diagnosis node for record %d, skipping',
                    transformation.get('name'),
                    study_id,
                    rec['source_file_row_num']
                )
            participant: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.PARTICIPANT, rec)
            if not participant:
                _logger.warning(
                    '%s (%s): Unable to build participant node for record %d, skipping',
                    transformation.get('name'),
                    study_id,
                    rec['source_file_row_num']
                )
            if not diagnosis or not participant:
                continue

            diagnosis['participant.participant_id'] = participant['participant_id']
            participant['diagnosis.diagnosis_id'] = [diagnosis['diagnosis_id']]
            participant['study.study_id'] = study['study_id']
            study['participant.participant_id'].append(participant['participant_id'])

            nodes['diagnoses'].append(diagnosis)
            nodes['participants'].append(participant)

        nodes['studies'].append(study)
        self._json_etl_data_sets[study_id] = self._json_etl_data_sets.get(study_id) or {}
        self._json_etl_data_sets[study_id][transformation.get('name')] = nodes

        _logger.info(
            '1 study, %d diagnosis, %d participant records created for transformation %s',
            len(nodes['diagnoses']),
            len(nodes['participants']),
            transformation.get('name')
        )

        return self._json_etl_data_sets[study_id][transformation.get('name')]

    def _transform_study(self, study_configuration: dict[str, any]) -> dict[str, any]:
        """ Transform and return ETL data for specified study transformation object """
        transformation: dict[str, any]
        for transformation in study_configuration.get('transformations', []):
            self._transform_source_data(study_configuration.get('study'), transformation)
        return self._raw_etl_data_tables[study_configuration.get('study')]

    def _create_json_etl_file(self, study_id: str, transformation: dict[str, any]) -> None:
        """ Create ETL file for specified raw source data set """
        _logger.info('Creating ETL file for transformation %s ()', transformation.get('name'))
        self._random = random.Random()
        self._random.seed(transformation.get('uuid_seed', None))
        self._load_source_data(study_id, transformation)
        self._transform_source_data(study_id, transformation)
        self._save_json_etl_data(study_id, transformation)


def print_usage() -> None:
    """ Print script usage """
    _logger.info('usage: python %s [optional config file name/path if not .env]', sys.argv[0])


def main() -> None:
    """ Script entry point """
    if len(sys.argv) > 2:
        print_usage()
        return

    config_file: str = sys.argv[1] if len(sys.argv) == 2 else '.env'
    if not os.path.exists(config_file):
        raise FileNotFoundError(f'Config file "{config_file}" not found')
    config: dict[str, str] = dotenv.dotenv_values(config_file)
    etl: C3dcEtl = C3dcEtl(config)
    etl.create_json_etl_files()
    etl.validate_json_etl_data()


if __name__ == '__main__':
    main()
