""" C3DC ETL File Creator """
import copy
import csv
import hashlib
import io
import json
import logging
import logging.config
import os
import pathlib
import random
import re
import sys
import tempfile
from typing import Callable
import uuid
import warnings

import dotenv
import jsonschema
from jsonschema import ValidationError
import jsonschema.exceptions
import petl

from c3dc_etl_model_node import C3dcEtlModelNode


def look_up_and_append_sys_path(*args: tuple[str, ...]) -> None:
    """ Append specified dir_name to sys path for import """
    dir_to_find: str
    for dir_to_find in args:
        parent: pathlib.Path
        for parent in pathlib.Path(os.getcwd()).parents:
            peer_dirs: list[os.DirEntry] = [d for d in os.scandir(parent) if d.is_dir()]
            path_to_append: str = next((p.path for p in peer_dirs if p.name == dir_to_find), None)
            if path_to_append:
                if path_to_append not in sys.path:
                    sys.path.append(path_to_append)
                break
look_up_and_append_sys_path('file_manager')
from c3dc_file_manager import C3dcFileManager # pylint: disable=wrong-import-position; # type: ignore


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


class C3dcEtl:
    """ Build C3DC json ETL file from (XLSX) source file """
    TYPE_NAME_CLASS_MAP: dict[str, type | list[type]] = {
        'array': list,
        'integer': int,
        'number': float,
        'string': str
    }
    MULTIPLE_VALUE_DELIMITER: str = ';'
    ETHNICITY_ALLOWED_VALUES: set[str] = {'Hispanic or Latino'}
    RACE_UNDETERMINED_VALUES: set[str] = {'Not Allowed to Collect', 'Not Reported', 'Unknown'}
    OBSERVATION_NODES: tuple[C3dcEtlModelNode, ...] = (
        C3dcEtlModelNode.DIAGNOSIS,
        C3dcEtlModelNode.GENETIC_ANALYSIS,
        C3dcEtlModelNode.LABORATORY_TEST,
        C3dcEtlModelNode.SURVIVAL,
        C3dcEtlModelNode.SYNONYM,
        C3dcEtlModelNode.TREATMENT,
        C3dcEtlModelNode.TREATMENT_RESPONSE
    )

    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._verify_config()

        self._json_schema_url: str = config.get('JSON_SCHEMA_URL')
        self._json_schema: dict[str, any] = {}
        self._json_schema_nodes: dict[str, any] = {}
        self._json_schema_property_enum_values: dict[str, list[str]] = {}
        self._json_schema_property_enum_code_values: dict[str, dict[str, str]] = {}
        self._json_etl_data_sets: dict[str, any] = {}
        self._raw_etl_data_tables: dict[str, any] = {}
        self._random: random.Random = random.Random()
        self._c3dc_file_manager: C3dcFileManager = C3dcFileManager()

        # These are the schema properties for which 'sub' source records may be needed as they're enum/string
        # properties for which source values may be delimited (';'). Enum/string properties in the schema where
        # the allowed values may contain ';' (diagnosis.diagnosis) will be excluded
        self._sub_source_record_enum_properties: dict[str, str] = {}

        # Remote study config should contain env-agnostic info like mappings, local study config
        # should contain info specific to the local env like file paths; remote and local will be
        # merged together to form the final study configuration object
        self._study_configurations: list[dict[str, any]] = json.loads(config.get('STUDY_CONFIGURATIONS', '[]'))
        self._study_configurations = [sc for sc in self._study_configurations if sc.get('active', True)]

        # find and suppress duplicate harmonized records using cache; key will be
        # hash of record normalized by serializing to JSON with blank/null assigned to id
        # { study id =>
        #   { (record hash, participant id, node) => [ transformation names (incl dupes) where source record found ] }
        # }
        self._harmonized_record_cache: dict[str, dict[tuple[str, str, str], list[str]]] = {}

        # { study id => { (participant id, node) => { set of duplicate records } } }
        self._duplicate_harmonized_records: dict[str, dict[tuple[str, str], set[str]]] = {}

        # node => [node records]
        self._merged_harmonized_records: dict[str, list[dict[str, any]]] = {}

        # to translate node names from singular to plural and back, e.g. study => studies, diagnoses- => diagnosis
        self._node_names_singular_to_plural: dict[str, str] = {
            n.value:C3dcEtlModelNode.get_pluralized_node_name(n.value) for n in C3dcEtlModelNode
        }
        self._node_names_plural_to_singular: dict[str, str] = {
            C3dcEtlModelNode.get_pluralized_node_name(n.value):n.value for n in C3dcEtlModelNode
        }

        self.load_json_schema()
        self.load_transformations()
        self._assert_valid_study_configurations()

    @property
    def json_schema(self) -> dict[str, any]:
        """ Get internal JSON schema, loading if needed """
        return self._json_schema if self._json_schema else self.load_json_schema()

    @property
    def json_etl_data(self) -> dict[str, any]:
        """ Get internal JSON ETL data object, building if needed """
        if not self._json_etl_data_sets:
            self.create_json_etl_files()
        return self._json_etl_data_sets

    @property
    def raw_etl_data_tables(self) -> dict[str, any]:
        """ Get internal source data tables, loading if needed """
        if not self._raw_etl_data_tables:
            study_configuration: dict[str, any]
            for study_configuration in self._study_configurations:
                self._load_study_data(study_configuration)
        return self._raw_etl_data_tables

    @staticmethod
    def is_number(value: str) -> bool:
        """ Determine whether specified string is number (float or int) """
        try:
            float(value)
        except (TypeError, ValueError):
            return False
        return True

    @staticmethod
    def is_allowed_value(value: any, allowed_values: set[any]) -> bool:
        """ Determine whether specified value is contained in or subset of specified allowed values """
        return (
            allowed_values
            and
            (
                (not isinstance(value, (list, set, tuple)) and value in allowed_values)
                or
                (isinstance(value, (list, set, tuple)) and value and set(value or {}).issubset(allowed_values))
            )
        )

    @staticmethod
    def is_replacement_match(
        source_field: str,
        source_record: dict[str, any],
        old_value: str
    ) -> bool:
        """ Determine whether the specified source value is a replacement match for the old value """
        # source field is single field
        if not (source_field.startswith('[') and source_field.endswith(']')):
            old_value = '' if old_value is None else str(old_value).strip().casefold()
            source_value: str = source_record.get(source_field, '')
            source_value = '' if source_value is None else str(source_value).strip().casefold()
            return (
                source_field == '[string_literal]'
                or
                old_value == '*'
                or
                (old_value == '+' and source_value != '')
                or
                source_value and old_value and source_value == old_value
            )

        # source field contains multiple fields, e.g. [race, ethnicity], so check to
        # see if old values to be replaced match source values by ordinal position
        source_field_names: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
        old_values: list[str] = (
            list(next(csv.reader([old_value.strip(' []')], delimiter=C3dcEtl.MULTIPLE_VALUE_DELIMITER)))
        ) if old_value not in ('*', '+') else [old_value for n in source_field_names]
        if len(old_values) != len(source_field_names):
            raise RuntimeError(
                'Invalid replacement entry, number of old values to be replaced must match number of (compound) ' +
                f'source fields: old value => "{old_value}", source field => "{source_field}"'
            )
        index: int
        source_field_name: str
        for index, source_field_name in enumerate(source_field_names):
            old_val: str = old_values[index]
            old_val = '' if old_val is None else str(old_val).strip().casefold()
            src_val: str = source_record.get(source_field_name, '')
            src_val = '' if src_val is None else str(src_val).strip().casefold()

            if not (old_val == '*' or (old_val == '+' and src_val != '') or src_val == old_val):
                return False
        return True

    @staticmethod
    def is_macro_mapping(mapping: dict[str, any]) -> bool:
        """ Determnine whether specified mapping uses macro replacement function """
        return any(
            r
            and
            isinstance(r.get('new_value'), str)
            and
            r.get('new_value').strip().startswith('{')
            and
            r.get('new_value').strip().endswith('}')
            for r in mapping.get('replacement_values', [])
        )

    @staticmethod
    def sort_data(data: any) -> any:
        """ Sort specified input data, including nested elements """
        if isinstance(data, dict):
            return {k:C3dcEtl.sort_data(v) for k,v in sorted(data.items())}
        if isinstance(data, list):
            return sorted(data)
        if isinstance(data, tuple):
            return tuple(sorted(data))
        return data

    @staticmethod
    def get_node_id_field_name(node: C3dcEtlModelNode | str, fully_qualified: bool = False) -> str:
        """ Get name of id field for specified node """
        return f'{node}.{node}_id' if fully_qualified else f'{node}_id'

    @staticmethod
    def get_cacheable_record(record: dict[str, any], node: C3dcEtlModelNode | str) -> dict[str, any]:
        """ Get cacheable version of specified record (will not modify original) """
        cacheable_record: dict[str, any] = copy.deepcopy(record)
        match node:
            case C3dcEtlModelNode.CONSENT_GROUP:
                # identical consent group records can have different signatures due to
                # different participants having different ids across source files
                cacheable_record[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)] = []
            case C3dcEtlModelNode.PARTICIPANT:
                # identical participant records can have different signatures due to same observations having
                # different ids across source files so validate with stripped down record containing id only
                obs_node: C3dcEtlModelNode
                for obs_node in C3dcEtl.OBSERVATION_NODES:
                    obs_node_id_field_qualified: str = C3dcEtl.get_node_id_field_name(obs_node, True)
                    if isinstance(cacheable_record.get(obs_node_id_field_qualified), list):
                        cacheable_record[obs_node_id_field_qualified] = []

                # left for reference; consent group id is expected to be unique across harmonization cycles for
                # studies having multiple source files so consent group id will not be normalized (blanked).
                # otherwise identical participants can appear to be distinct if consent group id is set using
                # e.g. random uuid generation instead of deterministically
                # cacheable_record[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.CONSENT_GROUP, True)] = ''
            case C3dcEtlModelNode.STUDY:
                # identical study records can have different signatures due to different
                # consent groups and same reference files having different ids across source files
                cacheable_record[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.CONSENT_GROUP, True)] = []
                cacheable_record[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.REFERENCE_FILE, True)] = []
            case C3dcEtlModelNode.REFERENCE_FILE:
                if 'dcf_indexd_guid' in cacheable_record:
                    cacheable_record['dcf_indexd_guid'] = ''
        # clear own id (<node>_id, not 'id' field used internally by NCI) since identical records generated during
        # separate harmonization cycles for different source files (discovery/validation etc) can differ if <node>_id
        # is set using random uuid generation
        cacheable_record[C3dcEtl.get_node_id_field_name(node)] = ''
        return cacheable_record

    @staticmethod
    def get_cache_key(record: dict[str, any], participant_id: str, node: str) -> tuple[str, str, str]:
        """ Get cache key for specified record, participant id and node as tuple[str, str, str] """
        # records to be hashed should have id fields blanked for dupe differentiation
        cache_key: tuple[str, str, str] = (
            hashlib.sha1(
                json.dumps(
                    C3dcEtl.sort_data(C3dcEtl.get_cacheable_record(record, node)),
                    sort_keys=True
                ).encode('utf-8')
            ).hexdigest(),
            participant_id,
            node
        )
        return cache_key

    def load_transformations(self, save_local_copy: bool = False) -> list[dict[str, any]]:
        """ Download JSON transformations from configured URLs and merge with local config """
        # enumerate each per-study transformation config object in local config
        st_index: int
        study_config: list[dict[str, any]]
        for st_index, study_config in enumerate(self._study_configurations):
            # load remote study config containing transformations and mappings
            remote_transforms: dict[str, any] = json.loads(
                self._c3dc_file_manager.read_file(study_config.get('transformations_url')).decode('utf-8')
            )
            if save_local_copy:
                with open(
                    f'./{self._c3dc_file_manager.get_basename(study_config.get("transformations_url"))}',
                    'wb'
                ) as file:
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
                _logger.info('Updating transformation at index %d for study configuration %d', rt_index, st_index)
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

        # cache source => output field mappings for the enum/string properties for each transformation
        for study_config in self._study_configurations:
            transformation: dict[str, any]
            for transformation in study_config.get('transformations', []):
                sub_src_rec_enum_prop: str
                for sub_src_rec_enum_prop in self._sub_source_record_enum_properties:
                    self._sub_source_record_enum_properties[sub_src_rec_enum_prop] = (
                        self._find_source_field(transformation, sub_src_rec_enum_prop) or None
                    )

        return self._study_configurations

    def load_json_schema(self, save_local_copy: bool = False) -> dict[str, any]:
        """ Download JSON schema from configured URL """
        # download remote JSON schema file
        self._json_schema = json.loads(self._c3dc_file_manager.read_file(self._json_schema_url).decode('utf-8'))
        if save_local_copy:
            self._c3dc_file_manager.write_file(
                json.dumps(self._json_schema).encode('utf-8'),
                f'./{self._c3dc_file_manager.get_basename(self._json_schema_url)}'
            )

        if not self._json_schema:
            raise RuntimeError('Unable to get node types, JSON schema not loaded')
        if '$defs' not in self._json_schema:
            raise RuntimeError('Unable to get node types, JSON schema does not contain root-level "$defs" property')

        # log warnings for remote nodes not found in C3DC model
        unmapped_def: str
        for unmapped_def in [k for k in self._json_schema['$defs'] if k != 'nodes' and not C3dcEtlModelNode.get(k)]:
            _logger.warning('Schema "$defs" child node "%s" not defined in C3dcEtlModelNode enum', unmapped_def)

        # cache schema objects matching C3DC model nodes
        self._json_schema_nodes = {k:v for k,v in self._json_schema['$defs'].items() if C3dcEtlModelNode.get(k)}

        # cache allowed values for enum properties; map node.property => [permissible values]
        self._sub_source_record_enum_properties.clear()
        self._json_schema_property_enum_values.clear()
        node_type: str
        for node_type in self._json_schema_nodes:
            enum_properties: dict[str, any] = {
                k:v for k,v in self._get_json_schema_node_properties(node_type).items()
                    if 'enum' in v or 'enum' in v.get('items', {})
            }
            prop_name: str
            prop_props: dict[str, any]
            for prop_name, prop_props in enum_properties.items():
                enum_values: list[str] = prop_props.get('enum', None)
                enum_values = prop_props.get('items', {}).get('enum', []) if enum_values is None else enum_values
                self._json_schema_property_enum_values[f'{node_type}.{prop_name}'] = enum_values
                # enum properties that contain ';' in any of the permissible values don't support
                # multiple delimited values since ';' is the delimiting character
                if not any(C3dcEtl.MULTIPLE_VALUE_DELIMITER in v for v in enum_values):
                    self._sub_source_record_enum_properties[f'{node_type}.{prop_name}'] = None

        # cache allowed values for enum codes; map node.property => {enum code => enum value}
        self._json_schema_property_enum_code_values.clear()
        node_type_dot_property_name: str
        for node_type_dot_property_name, enum_values in self._json_schema_property_enum_values.items():
            self._json_schema_property_enum_code_values[node_type_dot_property_name] = {
                (v.partition(' : '))[0]:v for v in enum_values
            }

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
            study_id: str = study_configuration.get('study')
            # clear caches for duplicate record tracking
            self._harmonized_record_cache[study_id] = {}
            self._duplicate_harmonized_records[study_id] = {}

            transformation: dict[str, any]
            for transformation in study_configuration.get('transformations', []):
                self._create_json_etl_file(study_id, transformation)
            if study_configuration.get('merged_output_file_path'):
                self._create_merged_json_etl_file(study_id, study_configuration.get('merged_output_file_path'))
                if study_configuration.get('duplicate_record_report_path'):
                    self._create_harmonized_duplicate_record_report_file(
                        study_id,
                        study_configuration.get('duplicate_record_report_path')
                    )
                self._validate_merged_harmonized_data(study_id)

    def _verify_config(self) -> None:
        required_config_var: str
        missing_config_vars: list[str] = []
        for required_config_var in ('JSON_SCHEMA_URL', 'STUDY_CONFIGURATIONS'):
            if not self._config.get(required_config_var):
                missing_config_vars.append(required_config_var)
        if missing_config_vars:
            raise RuntimeError(
                f'One or more required variables not specified in configuration: {tuple(missing_config_vars)}'
            )

        study_configs: list[dict[str, any]] = json.loads(self._config.get('STUDY_CONFIGURATIONS', '[]'))
        study_config: dict[str, any]
        for study_config in study_configs:
            for required_config_var in ('study', 'transformations', 'transformations_url'):
                if not study_config.get(required_config_var):
                    missing_config_vars.append(f'STUDY_CONFIGURATIONS => {required_config_var}')
            xform: dict[str, any]
            for xform in study_config.get('transformations', []):
                for required_config_var in ('name', 'source_file_path', 'output_file_path'):
                    if not xform.get(required_config_var):
                        missing_config_vars.append(f'STUDY_CONFIGURATIONS => transformations => {required_config_var}')

        if missing_config_vars:
            raise RuntimeError(
                f'One or more required variables not specified in configuration: {tuple(missing_config_vars)}'
            )

    def _get_petl_table_from_source_file(self, source_file_path: str, xl_sheet_name: str = None) -> any:
        """ Load and return PETL table for data within specified source file """
        tbl: any = None
        source_file_extension: str = pathlib.Path(source_file_path).suffix.lower()

        if source_file_extension in ['.csv', '.tsv']:
            source_data: bytes = self._c3dc_file_manager.read_file(source_file_path)
            if source_file_extension == '.csv':
                tbl = petl.fromcsv(io.StringIO(source_data.decode('utf-8')))
            elif source_file_extension == '.tsv':
                tbl = petl.fromtsv(io.StringIO(source_data.decode('utf-8')))
        elif source_file_extension == '.xlsx':
            if C3dcFileManager.is_local_path(source_file_path):
                tbl = petl.fromxlsx(
                    C3dcFileManager.url_to_path(source_file_path)
                        if source_file_path.startswith('file://')
                        else source_file_path,
                    sheet=xl_sheet_name
                )
            else:
                tmp_file: any
                tbl = petl.empty()
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    tmp_file.write(self._c3dc_file_manager.read_file(source_file_path))
                    tmp_file.flush()
                    tmp_file.close()
                    tbl = petl.fromxlsx(tmp_file.name, sheet=xl_sheet_name, data_only = True)

                    # reload from in-memory data because petl maintains association with table source file
                    # after delete, even if eager access methods (lookall, convertall, etc) are called
                    dicts: list[dict[str, any]] = list(petl.dicts(tbl))
                    tbl = petl.fromdicts(dicts, petl.header(tbl))
                    if self._c3dc_file_manager.file_exists(tmp_file.name):
                        self._c3dc_file_manager.delete_file(tmp_file.name)
        else:
            raise RuntimeError(f'Unsupported source file type/extension: {source_file_path}')
        # remove columns without headers
        tbl = petl.cut(tbl, [h for h in petl.header(tbl) if (h or '').strip()])

        # strip whitespace from header fields
        tbl = petl.rename(tbl, dict((h, h.strip()) for h in petl.header(tbl)))
        return tbl

    def _generate_uuid(self) -> uuid.UUID:
        """ Generate and return UUID(v4) using internal RNG that may be seeded for idempotent values """
        return uuid.UUID(int=self._random.getrandbits(128), version=4)

    def _get_race(self, source_race: str, source_ethnicity: str = None) -> list[str]:
        """ Determine race given specified source values of race and ethnicity """
        # Note delimited multi-entry source values are supported for both
        # race ("White;Other") and ethnicity ("Not Reported;Unknown")

        races: set[str] = set()
        # keep source ethnicity(-ies) if allowed and determinate race value like 'Hispanic or Latino' specified
        # and not indeterminate value such as 'Not Reported', 'Unknown', etc
        if source_ethnicity:
            source_ethnicities: set[str] = {
                e.strip() for e in source_ethnicity.split(C3dcEtl.MULTIPLE_VALUE_DELIMITER)
                    if e.strip().casefold() in {v.casefold() for v in C3dcEtl.ETHNICITY_ALLOWED_VALUES}
            }
            if all(
                e.casefold() in {v.casefold() for v in C3dcEtl.ETHNICITY_ALLOWED_VALUES} for e in source_ethnicities
            ):
                races.update(source_ethnicities)

        # keep source race(s) if:
        #   - specified as allowed and determinate value (not 'Unknown', 'Not Allowed to Collect', 'Not Reported', etc)
        #   *OR*
        #   - source ethnicity not specified or not allowed/determinate race value
        if source_race:
            source_races: set[str] = {r.strip() for r in source_race.split(C3dcEtl.MULTIPLE_VALUE_DELIMITER)}
            if not races:
                # no ethnicity value provided so just use race values whether they're determinate or not
                races.update(source_races)
            else:
                # determinate race value ('Hispanic or Latino') came through ethnicity field so add only determinate
                # values from race field since indeterminate values ('Not Reported', 'Unknown', etc) don't apply
                races.update(
                    {
                        r for r in source_races if r.strip().casefold() not in {
                            v.casefold() for v in C3dcEtl.RACE_UNDETERMINED_VALUES
                        }
                    }
                )

        return list(races)

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
        except jsonschema.exceptions.ValidationError as verr:
            _logger.error(
                'ETL data for transformation %s (study %s) failed schema validation:',
                transformation_name,
                study_id
            )
            _logger.error(verr)
            validator: jsonschema.Validator = jsonschema.Draft202012Validator(self._json_schema)
            validation_error: ValidationError
            for validation_error in validator.iter_errors(self._json_etl_data_sets[study_id][transformation_name]):
                _logger.error('%s: %s', validation_error.json_path, validation_error.message)
        return False

    def _save_json_etl_data(self, study_id: str, transformation: dict[str, any]) -> None:
        """ Save JSON ETL data for specified transformation to designated output file """
        _logger.info('Saving JSON ETL data to "%s"', transformation.get('output_file_path'))
        self._c3dc_file_manager.write_file(
            json.dumps(self._json_etl_data_sets[study_id][transformation.get('name')], indent=2).encode('utf-8'),
            transformation.get('output_file_path')
        )

    def _load_source_data(self, study_id: str, transformation: dict[str, any]) -> any:
        """ Load raw ETL data from source file specified in config """
        raw_etl_data_tbl: any = self._get_petl_table_from_source_file(
            transformation.get('source_file_path'),
            transformation.get('source_file_sheet')
        )

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

    def _get_json_schema_node_property_type(self, node_type_dot_property_name: str) -> str:
        """ Get JSON schema type name for specified node_type.property_name """
        if '.' not in node_type_dot_property_name:
            raise RuntimeError(f'Invalid node.property specified: {node_type_dot_property_name}')
        node_type: str = node_type_dot_property_name.split('.')[0]
        property_name: str = node_type_dot_property_name.split('.')[-1]
        return self._json_schema_nodes.get(node_type, {}).get('properties', {}).get(property_name, {}).get('type')

    def _get_json_schema_node_properties(self, node_type: C3dcEtlModelNode) -> dict[str, any]:
        """ Get properties for specified node in JSON schema  """
        if not self._json_schema_nodes:
            raise RuntimeError('Unable to get JSON schema nodes')
        if node_type not in self._json_schema_nodes:
            raise RuntimeError(f'Unable to get JSON schema node for type "{node_type}"')
        if 'properties' not in self._json_schema_nodes[node_type]:
            raise RuntimeError(f'"properties" not found in JSON schema node for type "{node_type}"')
        return self._json_schema_nodes[node_type]['properties']

    def _get_json_schema_node_required_properties(self, node_type: C3dcEtlModelNode) -> dict[str, any]:
        """ Get required properties for specified node in JSON schema  """
        node_properties: dict[str, any] = self._get_json_schema_node_properties(node_type)
        return {
            k:v for k,v in node_properties.items() if self._is_json_schema_node_property_required(f'{node_type}.{k}')
        }

    # pylint: disable-next=too-many-return-statements
    def _get_json_schema_node_property_converted_value(
        self,
        node_type_dot_property_name: str,
        value: any
    ) -> float | int | list | str:
        """ Get output value converted to JSON schema type for specified property array, float, integer, string """
        if value is None:
            return None

        # collate into string if collection specified
        value = C3dcEtl.MULTIPLE_VALUE_DELIMITER.join(value) if isinstance(value, (list, set, tuple)) else value

        if '.' not in node_type_dot_property_name:
            raise RuntimeError(f'Unexpected schema property name ("." not present): "{node_type_dot_property_name}"')

        json_type: str = self._get_json_schema_node_property_type(node_type_dot_property_name)
        if json_type not in C3dcEtl.TYPE_NAME_CLASS_MAP:
            raise RuntimeError(f'Schema type "{json_type}" not in type name class map')

        python_type = C3dcEtl.TYPE_NAME_CLASS_MAP.get(json_type)
        if python_type in (float, int) and not C3dcEtl.is_number(value):
            _logger.warning(
                'Unable to convert source value "%s" to type "%s" for property "%s"',
                value,
                json_type,
                node_type_dot_property_name
            )
            return None
        if python_type == list:
            if node_type_dot_property_name not in self._json_schema_property_enum_values:
                # values not constrained, split on delimiter
                return [s.strip() for s in str(value).split(C3dcEtl.MULTIPLE_VALUE_DELIMITER)]

            # multi-valued properties may specify multiple values using delimiter
            vals: dict[str, str] = {v:v for v in value.split(C3dcEtl.MULTIPLE_VALUE_DELIMITER)}
            val: str
            for val in vals:
                case_matched_val: str = self._case_match_json_schema_enum_value(node_type_dot_property_name, val)
                if case_matched_val is not None:
                    vals[val] = case_matched_val
                else:
                    _logger.warning(
                        'Unable to case match source sub-value "%s" for enum property "%s", omitting',
                        val,
                        node_type_dot_property_name
                    )
            return [v for v in vals.values() if v is not None]

        if python_type == float:
            return float(value)
        if python_type == int:
            return int(float(value))
        if python_type == str:
            if node_type_dot_property_name not in self._json_schema_property_enum_values:
                return str(value)
            new_value: str = self._case_match_json_schema_enum_value(node_type_dot_property_name, str(value))
            if new_value is None:
                _logger.warning(
                    'Unable to case match source value "%s" for enum property "%s", omitting',
                    value,
                    node_type_dot_property_name
                )
            return new_value

        raise RuntimeError(f'Unsupported output value conversion type: "{python_type}" (schema type "{json_type}")')

    def _case_match_json_schema_enum_value(self, node_type_dot_property_name: str, value: any) -> any:
        """ Align case of specified enum value with JSON schema permissible values, e.g. 'unknown' => 'Unknown' """
        if value is None or not isinstance(value, str):
            return value

        enum_values: list[str] = self._json_schema_property_enum_values.get(node_type_dot_property_name)
        if not enum_values:
            return value

        enum_value_matches: list[str] = [e for e in enum_values if e.casefold() == str(value).casefold()]
        if len(enum_value_matches) > 1:
            raise RuntimeError(
                f'Multiple enum value matches for "{value}" found in schema property "{node_type_dot_property_name}"'
            )
        return enum_value_matches[0] if len(enum_value_matches) == 1 else None

    def _is_json_schema_node_property_required(self, node_type_dot_property_name: str) -> bool:
        """ Determine if JSON schema property for specified output field ('node_name.field_name') is required """
        if '.' not in node_type_dot_property_name:
            raise RuntimeError(f'Invalid node.property specified: {node_type_dot_property_name}')
        node_type: str = node_type_dot_property_name.split('.')[0]
        property_name: str = node_type_dot_property_name.split('.')[-1]
        return property_name in self._json_schema_nodes.get(node_type, {}).get('required', [])

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
        for macro in re.findall(r'\{.*?\}', replacement_new_value):
            macro_text: str = macro.strip(' {}').strip()
            # source field to be replaced will be specified as '{field: FIELD_NAME}'
            if not (
                (macro_text.startswith('"') and macro_text.endswith('"')) or
                (macro_text.startswith("'") and macro_text.endswith("'")) or
                macro_text.lower() in ('find_enum_value', 'race', 'sum', 'uuid') or
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

        compound_source_fields: list[str] = []
        source_field: str = mapping.get('source_field')
        if not source_field:
            errors.append(f'{transformation_name} ({study_id}): mapping source field not specified: {mapping}')
        if source_field.startswith('[') and source_field.endswith(']'):
            # strip extra spaces; csv module parses "field 1, field 2" into ["field 1", " field 2"]
            compound_source_fields = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
            if not {s for s in compound_source_fields if s != 'string_literal'}.issubset(set(source_header)):
                errors.append(
                    f'{transformation_name} ({study_id}): compound source field in mapping ("{source_field}") ' +
                    f'not present in source data header: {source_header}'
                )
                errors.append('Verify all mapped source fields in remote transformations file are in source data')
        elif source_field not in source_header:
            errors.append(
                (
                    f'{transformation_name} ({study_id}): source field in mapping ("{source_field}") ' +
                    f'not present in source data header ("{source_header}")'
                )
            )
            errors.append('Verify all mapped source fields in remote transformations file are in source data')
        output_field: str = mapping.get('output_field')
        if not output_field:
            errors.append(f'{transformation_name} ({study_id}): mapping output field not specified: {mapping}')
        output_field_parts = output_field.split('.')
        output_node: str = output_field_parts.pop(0)
        output_property: str = '.'.join(output_field_parts)
        if (
            output_node not in self._json_schema_nodes
            or
            output_property not in self._json_schema_nodes.get(output_node, {}).get('properties', {})
        ):
            errors.append(f'{transformation_name} ({study_id}): mapping output field invalid: {mapping}')

        replacement_entry: dict[str, str]
        for replacement_entry in mapping.get('replacement_values', []):
            if 'old_value' not in replacement_entry or 'new_value' not in replacement_entry:
                errors.append(
                    f'{transformation_name} ({study_id}): replacement entry missing new or old value: ' +
                    str(replacement_entry)
                )

            old_value: str = replacement_entry.get('old_value', '*')
            new_value: any = replacement_entry.get('new_value', '')

            if source_field == '[string_literal]' and old_value not in ('+', '*'):
                errors.append(
                    f'{transformation_name} ({study_id}): replacement entry has invalid old value for ' +
                    f'string literal source: {mapping}'
                )

            # if multiple source fields then old value must be wildcard or be multi-valued with matching count
            if old_value != '*' and compound_source_fields:
                old_values: list[str] = [
                    o.strip() for o in next(
                        csv.reader([old_value.strip(' []')], delimiter=C3dcEtl.MULTIPLE_VALUE_DELIMITER)
                    )
                ]
                if len(old_values) != len(compound_source_fields):
                    errors.append(
                        f'{transformation_name} ({study_id}): replacement entry for mapping with multiple source ' +
                        'field (macro?) must have old value set to wildcard ("*") or contain same number of values ' +
                        'as source fields, delimited by "{C3dcEtl.MULTIPLE_VALUE_DELIMITER}"'
                    )
                    errors.append('ex: [source_field_1,source_field_2] => "old_value_1;old_value_2"')

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

        if (
            transformation.get('source_file_path')
            and
            not self._c3dc_file_manager.file_exists(transformation.get('source_file_path'))
        ):
            errors.append(
                f'{transformation.get("name")} ({study_id}): invalid source file ' +
                f'"{transformation.get("source_file_path")}"'
            )

        if (
            transformation.get('name')
            and
            not (self._raw_etl_data_tables or {}).get(study_id, {}).get(transformation.get('name'))
        ):
            if (
                transformation.get('source_file_path')
                and
                self._c3dc_file_manager.file_exists(transformation.get('source_file_path'))
            ):
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
        {uuid} => substitute with a UUID (v4 w/ optional seed value for RNG from config)
        {sum} => substitute with sum of values for specified fields in 'source_field' propery
        {field:source_field_name} => substitute with specified record's source field value, e.g. {field:TARGET USI}
        """
        msg: str
        output_value: any = None

        source_field: str = mapping.get('source_field').strip(' \t\r\n\'"')
        source_value: str = source_record.get(source_field, None)

        default_value: any = mapping.get('default_value', None)

        replacement_entry: dict[str, str]
        # pylint: disable=too-many-nested-blocks
        for replacement_entry in mapping.get('replacement_values', []):
            old_value: str = replacement_entry.get('old_value', '*')
            new_value: any = replacement_entry.get('new_value', None)

            # check for and apply macros such as 'sum' if specified for new value
            new_vals: list[any] = new_value if isinstance(new_value, (list, set, tuple)) else [new_value]
            new_val: any
            for i, new_val in enumerate(new_vals):
                if not (str(new_val).startswith('{') and str(new_val).endswith('}')):
                    continue
                macros: list[str] = re.findall(r'\{.*?\}', new_val)
                if not macros:
                    continue
                macro: str = macros[0]
                macro_text: str = macro.strip(' {}').strip()
                if macro_text.lower() == 'uuid':
                    new_val = new_val.replace(macro, str(self._generate_uuid()))
                elif macro_text.lower().startswith('field:'):
                    # source field to be replaced will be specified as '[field: FIELD_NAME]
                    macro_field: str = macro_text[len('field:'):].strip()
                    if macro_field not in source_record:
                        msg = f'Macro field not found in source record: "{macro_field}"'
                        _logger.critical(msg)
                        raise RuntimeError(msg)
                    new_val = new_val.replace(macro, source_record[macro_field])
                elif macro_text.lower() == 'find_enum_value':
                    # source field will be code such as '8000/0' or 'C71.9' that can be found in output value enum
                    output_field: str = mapping.get('output_field')
                    enum_value: any = self._get_json_schema_node_property_converted_value(
                        output_field,
                        self._json_schema_property_enum_code_values.get(output_field, {}).get(source_value)
                    )
                    if source_value and not enum_value:
                        _logger.warning('No enum value found for "%s" value code "%s"', source_field, source_value)
                    new_val = enum_value
                elif macro_text.lower() == 'sum':
                    # source field should contain list of source fields to be added together
                    if not (source_field.startswith('[') and source_field.endswith(']')):
                        msg = (
                            f'Invalid source field "{source_field}" for "{macro_text.lower()}" macro in row ' +
                            f'{source_record["source_file_row_num"]}, must be comma-delimited ' +
                            '(csv) string within square brackets, e.g. "[field1, field2]"'
                        )
                        _logger.critical(msg)
                        raise RuntimeError(msg)
                    # strip extra spaces; csv module parses "field 1, field 2" into ["field 1", " field 2"]
                    source_field_names: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
                    addends: list[float | int] = []
                    source_field_name: str
                    for source_field_name in source_field_names:
                        addend: str = source_record.get(source_field_name, f'{source_field_name} not found')
                        addend = '' if addend is None else str(addend).strip()
                        if addend in (None, ''):
                            # set output sum to blank/null if any addend is invalid/blank/null
                            return None

                        if not C3dcEtl.is_number(addend):
                            msg = (
                                f'Invalid "{source_field_name}" value "{addend}" for "{macro_text}" macro in row ' +
                                f'{source_record["source_file_row_num"]}, must be a number'
                            )
                            _logger.warning(msg)
                            addends.append(None)
                        else:
                            addends.append(float(addend))
                    new_val = sum(addends) if all(a is not None for a in addends) else default_value
                elif macro_text.lower() == 'race':
                    # source field may contain 'race' and 'ethnicity' source
                    # fields from which to derive final 'race' output value
                    source_field_names: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
                    if not source_field_names or len(source_field_names) > 2:
                        msg = (
                            f'Invalid source field "{source_field}" for "race" macro in source row ' +
                            f'{source_record["source_file_row_num"]}, must be single field OR comma-separated ' +
                            '(csv) string within square brackets specifying race and ethnicity fields such as ' +
                            '[race, ethnicity]'
                        )
                        _logger.critical(msg)
                        raise RuntimeError(msg)
                    source_race: str = source_record.get(source_field_names[0])
                    source_race = (
                        C3dcEtl.MULTIPLE_VALUE_DELIMITER.join(source_race)
                            if isinstance(source_race, (list, set, tuple))
                            else source_race
                    )
                    source_ethnicity: str = (
                        source_record.get(source_field_names[1]) if len(source_field_names) == 2 else ''
                    )
                    source_ethnicity = (
                        C3dcEtl.MULTIPLE_VALUE_DELIMITER.join(source_ethnicity)
                            if isinstance(source_ethnicity, (list, set, tuple))
                            else source_ethnicity
                    )
                    races: list[str] = self._get_race(source_race, source_ethnicity)
                    race: str
                    valid_races: list[str] = []
                    output_field: str = mapping.get('output_field')
                    for race in races:
                        case_matched_race: str = self._case_match_json_schema_enum_value(output_field, race)
                        if case_matched_race:
                            valid_races.append(case_matched_race)
                        else:
                            msg = (
                                f'Invalid source value "{race}" in "{source_field}" for "race" macro in source row ' +
                                f'{source_record["source_file_row_num"]}, not found in data dictionary'
                            )
                            _logger.warning(msg)
                    new_val = (
                        sorted(valid_races)
                            if valid_races and all(r not in ('', None) for r in valid_races)
                            else default_value
                    )
                new_vals[i] = new_val

            if new_value == '{find_enum_value}' and new_val is None:
                # enum lookup didn't find match, continue on to next replacement entry if available
                # ex: 0001/0 not in diagnosis.diagnosis enum value list but has replacement 8000/0
                # manual replacement ('Replacement Values' col in mapping) will follow the enum lookup
                # in the mapping's replacment_entries section
                continue

            new_value = new_vals if isinstance(new_value, (list, set, tuple)) else new_vals[0]
            if C3dcEtl.is_replacement_match(source_field, source_record, old_value):
                output_value = new_value
                break

        return output_value

    def _get_type_group_index_mappings(
        self,
        transformation: dict[str, any],
        node_type: C3dcEtlModelNode,
        clear_cache: bool = False
    ) -> dict[str, list[dict[str, any]]]:
        """
        Collate and return mappings of specified tranformation by type group index if defined, for example if
        multiple mappings of the same type are needed for a single transformation operation as may be the case
        for reference files, initial diagnosis + relapse diagnoses, etc
        """
        type_group_index_mappings: dict[str, list[dict[str, any]]] = transformation.get(
            '_type_group_index_mappings',
            {}
        ).get(node_type, {})
        if type_group_index_mappings and not clear_cache:
            return type_group_index_mappings

        # get mappings for specified node type
        type_group_index: str
        type_group_index_mappings: dict[str, list[dict[str, any]]] = {}
        mappings: list[dict[str, any]] = [
            m for m in transformation.get('mappings', []) if m.get('output_field', '').startswith(f'{node_type}.')
        ]
        mapping: dict[str, any]
        for mapping in mappings:
            type_group_indexes: list[str] = [i.strip() for i in str(mapping.get('type_group_index', '*')).split(',')]
            for type_group_index in type_group_indexes:
                type_group_index_mappings[type_group_index] = type_group_index_mappings.get(type_group_index) or []
                type_group_index_mappings[type_group_index].append(mapping)

        # replicate base/default mapping collection to remaining mapping groups, using
        # custom sort order to maintain order by type group index with exception for '*'
        type_group_index_mappings = dict(
            sorted(
                type_group_index_mappings.items(),
                key=lambda item: 0 if item[0] in ('', '*') else int(item[0])
            )
        )
        if not type_group_index_mappings:
            return type_group_index_mappings

        base_mappings: list[dict[str, any]] = type_group_index_mappings.get('*', [])
        non_base_mappings: dict[str, list[dict[str, any]]] = {
            k:v for k,v in type_group_index_mappings.items() if k != '*'
        }

        if base_mappings and non_base_mappings:
            for type_group_index, mappings in non_base_mappings.items():
                for mapping in reversed(base_mappings):
                    if not any(m for m in mappings if m.get('output_field') == mapping.get('output_field')):
                        mappings.insert(0, mapping)

        # the base/default mapping group is only needed if it's the only group so remove if there are multiple groups
        if non_base_mappings:
            type_group_index_mappings = non_base_mappings

        # cache the type group index mapping in the tranformation object for future re-use
        transformation['_type_group_index_mappings'] = transformation.get('_type_group_index_mappings', {})
        transformation['_type_group_index_mappings'][node_type] = type_group_index_mappings
        return type_group_index_mappings

    def _get_allowed_values(self, mapping: dict[str, any]) -> set[str]:
        """ Get allowed values for specified mapping """
        replacement_entries: list[dict[str, str]] = mapping.get('replacement_values', [])
        allowed_values: set[str] = set(
            r.get('old_value') for r in replacement_entries
                if r.get('old_value') not in ('+', '*', None) and r.get('new_value')
        )

        # don't include default value in allowed values unless output property is enum
        default_value: any = mapping.get('default_value')
        if default_value is not None and mapping.get('output_field') in self._json_schema_property_enum_values:
            if not isinstance(default_value, (list, set, tuple)):
                default_value = set([default_value])
            allowed_values.update(default_value)

        # check if all values are allowed if output property is enum
        if any(
            r.get('old_value') in ('+', '*') and r.get('new_value') == '{find_enum_value}' for r in replacement_entries
        ):
            # all enum values for mapped output field are allowed
            enum_code_values: dict[str, str] = self._json_schema_property_enum_code_values.get(
                mapping.get('output_field'),
                {}
            )
            allowed_values.update(set(enum_code_values.keys()))

        # empty string ("") and None are treated equally for matching/comparison purposes
        if '' in allowed_values:
            allowed_values.add(None)

        return allowed_values

    def _find_source_field(
        self,
        transformation: dict[str, any],
        output_field: str,
        type_group_index: str = None
    ) -> str:
        """
        Find source field for specified output field in transformation mappings. If the number of matches found
        is not equal to 1 then None will be returned.
        """
        matched_mappings: list[dict[str, any]] = [
            m for m in transformation.get('mappings', [])
                if (
                    m.get('output_field', '').strip() == output_field
                    and
                    (type_group_index is None or m.get('type_group_index', '*').strip() == type_group_index)
                )
        ]
        return matched_mappings[0].get('source_field', '').strip() if len(matched_mappings) == 1 else None

    def _transform_record_default(
        self,
        transformation: dict[str, any],
        node_type: C3dcEtlModelNode,
        source_record: dict[str, any] = None
    ) -> list[dict[str, any]]:
        """ Transform and return result after applying non-customized transformation to specified source record """
        source_record = source_record or {}
        output_records: list[dict[str, any]] = []

        type_group_index_mappings: dict[str, list[dict[str, any]]] = self._get_type_group_index_mappings(
            transformation,
            node_type
        )
        if not type_group_index_mappings:
            return []

        # a single source field can be mapped to multiple target fields, for example
        # 'First Event' => survival.first_event, survival.event_free_survival_status
        # so pre-load all allowed values for each source field to determine when a
        # source record contains invalid/unmapped values and should be skipped/ignored
        allowed_values: set[str]
        source_field_allowed_values: dict[str, list[str]] = {}
        type_group_index: str
        mappings: list[dict[str, any]]
        mapping: dict[str, any]
        for type_group_index, mappings in type_group_index_mappings.items():
            for mapping in mappings:
                source_field: str = mapping.get('source_field')
                if source_field.startswith('[') and source_field.endswith(']'):
                    # don't constrain string substitution mappings like '[string_literal]'
                    continue
                source_field_allowed_values[source_field] = source_field_allowed_values.get(source_field, [])
                source_field_allowed_values[source_field].extend(self._get_allowed_values(mapping))

        # if there are multiple type group indexes (multiple records are mapped) then default
        # values to base ("*") values first and then overwrite individual mapped fields for
        # each mapping sub-group specified for that type group index
        base_record: dict[str, any] = {}
        for type_group_index, mappings in type_group_index_mappings.items():
            output_record: dict[str, any] = {}
            output_record.update(base_record)
            for mapping in mappings:
                output_field: str = mapping.get('output_field')
                output_field_property: str = output_field[len(f'{node_type}.'):]
                output_field_type: str = self._get_json_schema_node_property_type(output_field)

                default_value: any = mapping.get('default_value')

                source_field: str = mapping.get('source_field')
                source_value: str = source_record.get(source_field, None)
                if source_value in ('', None) and default_value is not None:
                    source_value = default_value

                # check source value against all of this source field's mapped replacement values
                # in case there are multiple output field mappings for this source field
                allowed_values = source_field_allowed_values.get(source_field, set())
                source_value_allowed: bool = C3dcEtl.is_allowed_value(source_value, allowed_values)
                if allowed_values and not source_value_allowed:
                    _logger.warning(
                        (
                            '"%s" not specified as allowed value (old_value) in transformation(s) for source field ' +
                            '"%s" (output field "%s"), source record "%s": %s'
                        ),
                        source_value if source_value is not None else '',
                        source_field,
                        output_field,
                        source_record.get('source_file_row_num'),
                        allowed_values
                    )

                # check source value against mapped replacement values for this particular output field mapping
                allowed_values = self._get_allowed_values(mapping)
                source_value_allowed = C3dcEtl.is_allowed_value(source_value, allowed_values)
                if allowed_values and not source_value_allowed and not C3dcEtl.is_macro_mapping(mapping):
                    _logger.warning(
                        'value "%s" not allowed for source field "%s" (type group "%s")',
                        source_value if source_value is not None else '',
                        source_field,
                        type_group_index
                    )
                    continue

                output_value: any = self._get_mapped_output_value(mapping, source_record)
                output_record[output_field_property] = (
                    output_value
                        if output_value is not None
                        else self._get_json_schema_node_property_converted_value(output_field, source_value)
                )
                if (
                    output_field_type in ('integer', 'number')
                    and
                    output_record[output_field_property]
                    and
                    not C3dcEtl.is_number(output_record[output_field_property])
                ):
                    _logger.warning(
                        'Unable to set output property "%s" (source field "%s") having type "%s" to value "%s"',
                        output_field,
                        source_field,
                        output_field_type,
                        output_record[output_field_property]
                    )
                    output_record[output_field_property] = None
                if output_field_type == 'integer' and C3dcEtl.is_number(output_record[output_field_property]):
                    # some source values are read from Excel as floats instead of ints, e.g. age at diagnosis
                    # 3660.9999999999995 instead of 3661 for some TARGET OS records, so round (not int() which
                    # will truncate) harmonized values for integer output fields
                    output_record[output_field_property] = round(output_record[output_field_property])

            # verify that record is valid and contains all required properties
            record_valid: bool = True
            required_properties: dict[str, any] = self._get_json_schema_node_required_properties(node_type)
            required_property: str
            for required_property in required_properties:
                schema_field: str = f'{node_type}.{required_property}'
                required_property_value: any = output_record.get(required_property, None)
                if (
                    required_property_value in ('', None, [])
                    or
                    isinstance(required_property_value, list) and all(v in ('', None) for v in required_property_value)
                ):
                    record_valid = False
                    _logger.warning(
                        'Required output field "%s" (source field "%s") is null/empty for source record "%s"',
                        schema_field,
                        self._find_source_field(transformation, schema_field, type_group_index) or '*not mapped*',
                        source_record.get("source_file_row_num")
                    )

            if not record_valid:
                # record failed validation, move on to next type group index
                continue

            if output_record:
                output_records.append(output_record)
            if type_group_index == 0:
                base_record.update(output_record)

        return output_records

    def _build_node(
        self,
        transformation: dict[str, any],
        node_type: C3dcEtlModelNode,
        source_record: dict[str, any] = None
    ) -> list[dict[str, any]]:
        """
        Build and return specified C3DC model node type. If a custom method named '_transform_record_{node_type}'
        is found then that will be called, otherwise the default base method will be called for all node types
        """
        transform_method_name: str = f'_transform_record_{node_type}'
        transform_method: Callable[[dict[str, any], dict[str, any]], list[dict[str, any]]] = getattr(
            self,
            transform_method_name,
            lambda t, s: None
        )
        if (
            transform_method is None or
            not hasattr(self, transform_method_name) or
            not callable(transform_method)
        ):
            return self._transform_record_default(transformation, node_type, source_record)

        return transform_method(transformation, source_record)

    def _build_sub_source_records(
        self,
        source_record: dict[str, any],
        node_props: dict[str, any]
    ) -> list[dict[str, any]]:
        """
        If source record contains enum fields having multiple values separated by delimiter then process as multiple
        source records for each distinct value with all other properties kept the same except record id, which must
        be unique for each newly created record
        """
        # check each property in the source record that is an enum/string (as opposed to enum/array)
        # where ';' isn't present in the list of permissible values. if the source value contains ';'
        # then create a sub source record for each distinct value otherwise return empty collection
        sub_source_records: list[dict[str, any]] = []
        sub_src_rec_enum_props: set[str] = {
            p for p in self._sub_source_record_enum_properties if p.startswith(f'{node_props["type"]}.')
        }
        sub_src_rec_enum_prop: str
        for sub_src_rec_enum_prop in sub_src_rec_enum_props:
            source_field: str = self._sub_source_record_enum_properties.get(sub_src_rec_enum_prop)
            if not source_field or C3dcEtl.MULTIPLE_VALUE_DELIMITER not in str(source_record.get(source_field, '')):
                continue
            sub_src_ids_vals: dict[str, str] = {
                f'{source_record[node_props["source_id_field"]]}_{k}':v for k,v in dict(
                    enumerate(
                        sorted(
                            set(
                                s.strip() for s in source_record[source_field].split(
                                    C3dcEtl.MULTIPLE_VALUE_DELIMITER
                                ) if s.strip()
                            )
                        ),
                        1
                    )
                ).items()
            }
            _logger.info(
                (
                    '"%s" "%s" has %d distinct delimited value(s) for "%s" ("%s"), creating separate record per value'
                ),
                node_props['type'],
                source_record[node_props["source_id_field"]],
                len(sub_src_ids_vals),
                sub_src_rec_enum_prop.partition('.')[2],
                source_field
            )
            sub_src_id: str
            sub_src_val: str
            for sub_src_id, sub_src_val in sub_src_ids_vals.items():
                src_rec_clone: dict[str, any] = json.loads(json.dumps(source_record))
                src_rec_clone[node_props['source_id_field']] = sub_src_id
                src_rec_clone[source_field] = sub_src_val
                sub_source_records.append(src_rec_clone)
        return sub_source_records

    def _transform_source_data(self, study_id: str, transformation: dict[str, any]) -> dict[str, any]:
        """ Transform and return ETL data transformed using rules specified in config """
        _logger.info('Transforming source data')
        if not petl.nrows(self._raw_etl_data_tables.get(study_id, {}).get(transformation.get('name'), petl.empty())):
            self._load_source_data(study_id, transformation)
            if not petl.nrows(self._raw_etl_data_tables[study_id][transformation.get('name')]):
                raise RuntimeError(f'No data loaded to transform for study {study_id}')

        participant_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)
        subject_id_field: str = self._find_source_field(transformation, participant_id_field)
        if not subject_id_field:
            raise RuntimeError(
                f'Unable to find single source mapping for "{participant_id_field}" in transformation mappings; ' +
                f'"{participant_id_field}" is either not mapped or is mapped multple times'
            )
        unmapped_nodes: set[C3dcEtlModelNode] = set()
        nodes: dict[C3dcEtlModelNode, dict[str, any]] = {
            C3dcEtlModelNode.CONSENT_GROUP: {},
            C3dcEtlModelNode.DIAGNOSIS: {},
            C3dcEtlModelNode.GENETIC_ANALYSIS: {},
            C3dcEtlModelNode.LABORATORY_TEST: {},
            C3dcEtlModelNode.PARTICIPANT: {},
            C3dcEtlModelNode.REFERENCE_FILE: {},
            C3dcEtlModelNode.STUDY: {},
            C3dcEtlModelNode.SURVIVAL: {},
            C3dcEtlModelNode.SYNONYM: {},
            C3dcEtlModelNode.TREATMENT: {},
            C3dcEtlModelNode.TREATMENT_RESPONSE: {}
        }
        node: C3dcEtlModelNode
        node_props: dict[str, any]
        for node, node_props in nodes.items():
            node_props['harmonized_records'] = []
            node_props['type'] = node
            node_props['id_field'] = C3dcEtl.get_node_id_field_name(node)
            node_props['id_field_full'] = C3dcEtl.get_node_id_field_name(node, True)
            node_props['source_id_field'] = subject_id_field

            type_group_index_mappings: dict[str, list[dict[str, any]]] = self._get_type_group_index_mappings(
                transformation,
                node
            )
            if not type_group_index_mappings:
                _logger.warning('No mappings found for type "%s", will be omitted from output', node)
                unmapped_nodes.add(node)

        # build study node and add to node collection
        study: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.STUDY)
        if len(study) != 1:
            raise RuntimeError(f'Unexpected number of study nodes built ({len(study)}), check mapping')
        study = study[0]
        study[nodes[C3dcEtlModelNode.CONSENT_GROUP]['id_field_full']] = []
        study[nodes[C3dcEtlModelNode.REFERENCE_FILE]['id_field_full']] = []

        # build consent group node and add to node collection
        consent_group: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.CONSENT_GROUP)
        if len(consent_group) != 1:
            raise RuntimeError(f'Unexpected number of consent group nodes built ({len(consent_group)}), check mapping')
        consent_group = consent_group[0]
        consent_group[nodes[C3dcEtlModelNode.PARTICIPANT]['id_field_full']] = []
        consent_group[nodes[C3dcEtlModelNode.STUDY]['id_field_full']] = study[nodes[C3dcEtlModelNode.STUDY]['id_field']]
        study[nodes[C3dcEtlModelNode.CONSENT_GROUP]['id_field_full']].append(
            consent_group[nodes[C3dcEtlModelNode.CONSENT_GROUP]['id_field']]
        )

        # build reference file nodes and add to node collection
        reference_files: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.REFERENCE_FILE)
        reference_file: dict[str, any]
        for reference_file in reference_files:
            reference_file[nodes[C3dcEtlModelNode.STUDY]['id_field_full']] = study[
                nodes[C3dcEtlModelNode.STUDY]['id_field']
            ]
            study[nodes[C3dcEtlModelNode.REFERENCE_FILE]['id_field_full']].append(
                reference_file[nodes[C3dcEtlModelNode.REFERENCE_FILE]['id_field']]
            )
        nodes[C3dcEtlModelNode.REFERENCE_FILE]['harmonized_records'].extend(reference_files)

        # add observation and participant records to match source data records
        rec: dict[str, any]
        for rec in petl.dicts(self._raw_etl_data_tables[study_id][transformation.get('name')]):
            # check for and skip empty rows loaded by petl
            if all(v in (None, '') for k,v in rec.items() if k != 'source_file_row_num'):
                _logger.warning('Skipping empty source record %d', rec['source_file_row_num'])
                continue

            participant: dict[str, any]
            participants: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.PARTICIPANT, rec)
            if len(participants) != 1:
                _logger.warning(
                    '%s (%s): Unexpected number of participant nodes (%d) built for source record %d, excluding',
                    transformation.get('name'),
                    study_id,
                    len(participants),
                    rec['source_file_row_num']
                )
                participant = None
                continue
            participant = participants[0]

            for node in C3dcEtl.OBSERVATION_NODES:
                # make sure relationship collection is defined, even if no records are added
                participant[nodes[node]['id_field_full']] = []

                sub_src_rec: dict[str, any]
                for sub_src_rec in self._build_sub_source_records(rec, nodes[node]) or [rec]:
                    harmonized_recs: list[dict[str, any]] = self._build_node(transformation, node, sub_src_rec)
                    if not harmonized_recs and node not in unmapped_nodes:
                        _logger.warning(
                            '%s (%s): Unable to build "%s" node for source record "%s"',
                            transformation.get('name'),
                            study_id,
                            node,
                            sub_src_rec['source_file_row_num']
                        )

                    harmonized_rec: dict[str, any]
                    for harmonized_rec in harmonized_recs:
                        # set referential ids for this observation node and parent participant
                        harmonized_rec[nodes[C3dcEtlModelNode.PARTICIPANT]['id_field_full']] = participant[
                            nodes[C3dcEtlModelNode.PARTICIPANT]['id_field']
                        ]
                        participant[nodes[node]['id_field_full']].append(harmonized_rec[nodes[node]['id_field']])

                    # populate final transformed record collection for this observation node
                    nodes[node]['harmonized_records'].extend(harmonized_recs)

            participant[nodes[C3dcEtlModelNode.CONSENT_GROUP]['id_field_full']] = consent_group[
                nodes[C3dcEtlModelNode.CONSENT_GROUP]['id_field']
            ]
            consent_group[nodes[C3dcEtlModelNode.PARTICIPANT]['id_field_full']].append(
                participant[nodes[C3dcEtlModelNode.PARTICIPANT]['id_field']]
            )
            nodes[C3dcEtlModelNode.PARTICIPANT]['harmonized_records'].append(participant)

        # check for dupe ids
        for node, node_props in nodes.items():
            node_rec: dict[str, any]
            id_cache: set[str] = set()
            dupe_ids: set[str] = set()
            for node_rec in node_props['harmonized_records']:
                if node_rec[node_props['id_field']] in id_cache:
                    dupe_ids.add(node_rec[node_props['id_field']])
                id_cache.add(node_rec[node_props['id_field']])
            if dupe_ids:
                raise RuntimeError(f'Duplicate {node} id(s) found: {dupe_ids}')

        # attach the consent group object
        nodes[C3dcEtlModelNode.CONSENT_GROUP]['harmonized_records'].append(consent_group)

        # attach the study object
        nodes[C3dcEtlModelNode.STUDY]['harmonized_records'].append(study)

        self._json_etl_data_sets[study_id] = self._json_etl_data_sets.get(study_id) or {}
        self._json_etl_data_sets[study_id][transformation.get('name')] = {
            self._node_names_singular_to_plural[k]:v['harmonized_records'] for k,v in nodes.items()
        }

        _logger.info(
            '%s records built for transformation "%s"',
            ', '.join(f'{len(v["harmonized_records"])} {k}' for k,v in nodes.items()),
            transformation.get("name")
        )

        return self._json_etl_data_sets[study_id][transformation.get('name')]

    def _create_json_etl_file(self, study_id: str, transformation: dict[str, any]) -> None:
        """ Create JSON ETL data file for specified raw source data set """
        _logger.info('Creating JSON ETL data file for transformation %s (%s)', transformation.get('name'), study_id)
        self._random = random.Random()
        self._random.seed(transformation.get('uuid_seed', None))
        self._load_source_data(study_id, transformation)
        self._transform_source_data(study_id, transformation)
        self._save_json_etl_data(study_id, transformation)

    def _get_merged_harmonized_node_ids(self, node: C3dcEtlModelNode) -> list[str]:
        """ Get collection of ids from merged harmonized data set for specified node type """
        return [
            r[C3dcEtl.get_node_id_field_name(node)] for r in
                self._merged_harmonized_records[self._node_names_singular_to_plural[node]]
        ]

    def _get_merged_harmonized_record(
        self,
        node: C3dcEtlModelNode,
        node_id: str,
        raise_error_if_not_found: bool = False
    ) -> dict[str, any]:
        """ Get merged harmonized record for specified node type and id """
        record: dict[str, any] = next(
            (
                r for r in self._merged_harmonized_records[self._node_names_singular_to_plural[node]]
                    if r[C3dcEtl.get_node_id_field_name(node)] == node_id
            ),
            None
        )
        if record is None and raise_error_if_not_found:
            raise RuntimeError(f'Merged harmonized record for "{node}" with id "{node_id}" not found')

        return record


    def _validate_merged_harmonized_node_data(self, node: C3dcEtlModelNode) -> None:
        """ Validate merged harmonized data for specified node """
        _logger.info('Validating merged harmonized node data: "%s"', node)

        node_for_id_field: C3dcEtlModelNode
        node_id_fields: dict[str, str] = {}
        node_id_fields_qualified: dict[str, str] = {}
        for node_for_id_field in C3dcEtlModelNode:
            node_id_fields[node_for_id_field] = C3dcEtl.get_node_id_field_name(node_for_id_field)
            node_id_fields_qualified[node_for_id_field] = C3dcEtl.get_node_id_field_name(node_for_id_field, True)

        node_id_field: str = node_id_fields[node]
        study_id_field: str = node_id_fields[C3dcEtlModelNode.STUDY]
        study_id_field_qualified: str = node_id_fields_qualified[C3dcEtlModelNode.STUDY]

        consent_group_id_field: str = node_id_fields[C3dcEtlModelNode.CONSENT_GROUP]
        consent_group_id_field_qualified: str = node_id_fields_qualified[C3dcEtlModelNode.CONSENT_GROUP]

        participant_id_field_qualified: str = node_id_fields_qualified[C3dcEtlModelNode.PARTICIPANT]
        reference_file_id_field_qualified: str = node_id_fields_qualified[C3dcEtlModelNode.REFERENCE_FILE]

        if len(self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]]) != 1:
            raise RuntimeError('Error validating merged harmonized data, number of study records in data set != 1')
        merged_study: dict[str, any] = next(
            s for s in self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]]
        )

        if len(
            self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.CONSENT_GROUP]]
        ) != 1:
            raise RuntimeError(
                'Error validating merged harmonized data, number of consent group records in data set != 1'
            )
        merged_consent_group: dict[str, any] = next(
            c for c in self._merged_harmonized_records[
                self._node_names_singular_to_plural[C3dcEtlModelNode.CONSENT_GROUP]
            ]
        )

        merged_consent_group_ids: list[str] = self._get_merged_harmonized_node_ids(C3dcEtlModelNode.CONSENT_GROUP)
        merged_participant_ids: list[str] = self._get_merged_harmonized_node_ids(C3dcEtlModelNode.PARTICIPANT)
        merged_reference_file_ids: list[str] = self._get_merged_harmonized_node_ids(C3dcEtlModelNode.REFERENCE_FILE)

        # ensure consistency of records and record id lists for linked relationships between
        # participants <=> observations, consent group <=> participants, and study <=> reference files
        node_name_plural: str = self._node_names_singular_to_plural[node]
        for record in self._merged_harmonized_records.get(node_name_plural, []):
            record_id: str = record[node_id_field]
            if node == C3dcEtlModelNode.CONSENT_GROUP:
                # make sure consent group participant ids match ids of actual participant records and are unique
                if sorted(record[participant_id_field_qualified]) != sorted(merged_participant_ids):
                    _logger.critical('consent group participant ids:')
                    _logger.critical(sorted(record[participant_id_field_qualified]))
                    _logger.critical('merged participant ids:')
                    _logger.critical(sorted(merged_participant_ids))
                    raise RuntimeError('Mismatch between consent group participant id list and participant ids')
                participant_id_counts: dict[str, int] = {}
                participant_id: str
                for participant_id in record[participant_id_field_qualified]:
                    participant_id_counts[participant_id] = participant_id_counts.get(participant_id, 0) + 1
                dupe_participant_ids: set[str] = {pid for pid, cnt in participant_id_counts.items() if cnt > 1}
                if dupe_participant_ids:
                    raise RuntimeError(
                        f'Duplicate entries in consent group participant id list: {dupe_participant_ids}'
                    )

            if node in (C3dcEtlModelNode.CONSENT_GROUP, C3dcEtlModelNode.REFERENCE_FILE):
                # make sure study id matches existing study
                if record[study_id_field_qualified] != merged_study[study_id_field]:
                    raise RuntimeError(
                        f'{node} study id "{record[study_id_field_qualified]}" != "{merged_study[study_id_field]}"'
                    )

                # make sure consent group or reference file id in merged study's id list
                if record_id not in merged_study[node_id_fields_qualified[node]]:
                    raise RuntimeError(f'"{node}" id "{record_id}" not in study "{node}" id list')

            if node == C3dcEtlModelNode.PARTICIPANT:
                # make sure consent group id matches existing consent group
                if record[consent_group_id_field_qualified] != merged_consent_group[consent_group_id_field]:
                    raise RuntimeError(
                        f'{node} consent group id "{record[consent_group_id_field_qualified]}" != ' +
                            f'"{merged_consent_group[consent_group_id_field]}"'
                    )

                # make sure participant id in merged consent group's id list
                if record_id not in merged_consent_group[node_id_fields_qualified[node]]:
                    raise RuntimeError(f'"{node}" id "{record_id}" not in consent group "{node}" id list')

                # make sure all observations exist
                observation_node: C3dcEtlModelNode
                for observation_node in C3dcEtlModelNode:
                    if not isinstance(record.get(node_id_fields_qualified[observation_node]), list):
                        continue
                    observation_node_name_plural: str = self._node_names_singular_to_plural[observation_node]
                    observation_id: str
                    for observation_id in record[node_id_fields_qualified[observation_node]]:
                        if not any(
                            on for on in self._merged_harmonized_records[observation_node_name_plural]
                                if on[node_id_fields[observation_node]] == observation_id
                        ):
                            raise RuntimeError(
                                f'"{observation_node}" "{observation_id}" not found for participant "{record_id}"'
                            )

            if node == C3dcEtlModelNode.STUDY:
                # make sure study consent group ids match ids of actual consent group records and are unique
                if sorted(record[consent_group_id_field_qualified]) != sorted(merged_consent_group_ids):
                    _logger.critical('study consent group ids:')
                    _logger.critical(sorted(record[participant_id_field_qualified]))
                    _logger.critical('merged consent group ids:')
                    _logger.critical(sorted(merged_consent_group_ids))
                    raise RuntimeError('Mismatch between study consent group id list and consent group ids')
                consent_group_id_counts: dict[str, int] = {}
                consent_group_id: str
                for consent_group_id in record[consent_group_id_field_qualified]:
                    consent_group_id_counts[consent_group_id] = consent_group_id_counts.get(consent_group_id, 0) + 1
                dupe_consent_group_ids: set[str] = {cgid for cgid, cnt in consent_group_id_counts.items() if cnt > 1}
                if dupe_consent_group_ids:
                    raise RuntimeError(f'Duplicate entries in study consent group id list: {dupe_consent_group_ids}')

                # make sure study reference file ids match ids of actual reference file records and are unique
                if sorted(record[reference_file_id_field_qualified]) != sorted(merged_reference_file_ids):
                    _logger.critical('study reference file ids:')
                    _logger.critical(sorted(record[reference_file_id_field_qualified]))
                    _logger.critical('merged reference file ids:')
                    _logger.critical(sorted(merged_reference_file_ids))
                    raise RuntimeError('Mismatch between study reference file id list and reference file ids')
                reference_file_id_counts: dict[str, int] = {}
                reference_file_id: str
                for reference_file_id in record[node_id_fields_qualified[C3dcEtlModelNode.REFERENCE_FILE]]:
                    reference_file_id_counts[reference_file_id] = reference_file_id_counts.get(reference_file_id, 0) + 1
                dupe_reference_file_ids: set[str] = {rfid for rfid, cnt in reference_file_id_counts.items() if cnt > 1}
                if dupe_reference_file_ids:
                    raise RuntimeError(f'Duplicate entries in study reference file id list: {dupe_reference_file_ids}')

            # observations such as diagnosis, survival, etc
            if isinstance(record.get(participant_id_field_qualified), str):
                # observation associated with participant; make sure participant exists and
                # observation id in participant's list of observations for node type
                record_participant_id: str = record[participant_id_field_qualified]
                if record_participant_id not in merged_participant_ids:
                    raise RuntimeError(
                        f'Participant "{record_participant_id}" not in merged participant list for record: {record}'
                    )
                record_participant: dict[str, any] = self._get_merged_harmonized_record(
                    C3dcEtlModelNode.PARTICIPANT,
                    record_participant_id
                )
                if not record_participant:
                    raise RuntimeError(f'Participant "{record_participant_id}" not found for record: {record}')

        _logger.info('Validation of merged harmonized node data for "%s" found no issues', node)

    def _validate_merged_harmonized_data(self, study_id: str) -> None:
        """
        Validate merged harmonized data, checking for duplicates and matching counts against individual data sets
        """
        _logger.info('Validating merged harmonized data against unmerged JSON ETL data for study "%s"', study_id)

        # verify study matches id
        merged_study: dict[str, any] = next(
            iter(self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]])
        )
        study_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.STUDY)
        if merged_study[study_id_field] != study_id:
            raise RuntimeError(f'Merged study id "{merged_study[study_id_field]}" != "{study_id}"')

        participant_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT)
        participant_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)

        # cache records for merged and distinct unmerged records for comparison
        unmerged_node_cache_keys: dict[str, set[tuple[str, str, str]]] = {}
        merged_node_cache_keys: dict[str, set[tuple[str, str, str]]] = {}
        xform_data: dict[str, list[dict[str, any]]]
        node_name_plural: str
        for xform_data in self._json_etl_data_sets[study_id].values():
            records: list[dict[str, any]]
            for node_name_plural, records in xform_data.items():
                node_name: str = self._node_names_plural_to_singular[node_name_plural]
                unmerged_node_cache_keys[node_name_plural] = unmerged_node_cache_keys.get(node_name_plural, set())
                record: dict[str, any]
                for record in records:
                    cacheable_record: dict[str, any] = C3dcEtl.get_cacheable_record(record, node_name)
                    participant_id: str = record.get(
                        participant_id_field,
                        record.get(participant_id_field_qualified, '')
                    )
                    participant_id = participant_id if isinstance(participant_id, str) else ''
                    cache_key: tuple[str, str, str] = C3dcEtl.get_cache_key(cacheable_record, participant_id, node_name)
                    unmerged_node_cache_keys[node_name_plural].add(cache_key)
        _logger.info(
            '%s distinct unmerged records found for study "%s"',
            ', '.join(f'{len(v)} {self._node_names_plural_to_singular[k]}' for k,v in unmerged_node_cache_keys.items()),
            study_id
        )

        for node_name_plural, records in self._merged_harmonized_records.items():
            node_name: str = self._node_names_plural_to_singular[node_name_plural]
            merged_node_cache_keys[node_name_plural] = merged_node_cache_keys.get(node_name_plural, set())
            record: dict[str, any]
            for record in records:
                participant_id: str = record.get(participant_id_field, record.get(participant_id_field_qualified, ''))
                participant_id = participant_id if isinstance(participant_id, str) else ''
                # identical participant records can have different signatures due to same observations having
                # different ids for different source files so use proxy record only containing participant id
                cacheable_record: dict[str, any] = C3dcEtl.get_cacheable_record(record, node_name)
                cache_key: tuple[str, str, str] = C3dcEtl.get_cache_key(cacheable_record, participant_id, node_name)
                merged_node_cache_keys[node_name_plural].add(cache_key)
        _logger.info(
            '%s merged harmonized records found for study "%s"',
            ', '.join(f'{len(v)} {self._node_names_plural_to_singular[k]}' for k,v in merged_node_cache_keys.items()),
            study_id
        )

        # compare merged and unmerged participant ids
        unmerged_participant_ids: set[str] = set()
        for xform_data in self._json_etl_data_sets[study_id].values():
            record: dict[str, any]
            for record in xform_data[self._node_names_singular_to_plural[C3dcEtlModelNode.PARTICIPANT]]:
                unmerged_participant_ids.add(record[participant_id_field])

        merged_participants: list[dict[str, any]] = self._merged_harmonized_records[
            self._node_names_singular_to_plural[C3dcEtlModelNode.PARTICIPANT]
        ]
        merged_participant_ids: dict[str, int] = {}
        record: dict[str, any]
        for record in merged_participants:
            merged_participant_ids[record[participant_id_field]] = \
                merged_participant_ids.get(record[participant_id_field], 0) + 1
        dupe_merged_participant_ids: set[str] = {pid for pid, cnt in merged_participant_ids.items() if cnt > 1}
        if dupe_merged_participant_ids:
            raise RuntimeError(f'Duplicate merged participant records found: {dupe_merged_participant_ids}')
        if unmerged_participant_ids != merged_participant_ids.keys():
            _logger.error('Mismatch between participant ids in unmerged and merged data sets')
            _logger.error('Unmerged participant ids:')
            _logger.error(unmerged_participant_ids)
            _logger.error('Merged participant ids:')
            _logger.error(merged_participant_ids)
            raise RuntimeError('Mismatch between participant ids in unmerged and merged data sets')

        # compare merged and unmerged node names and counts
        unmerged_merged_key_diff: set[str] = set(unmerged_node_cache_keys.keys()).difference(merged_node_cache_keys)
        if unmerged_merged_key_diff:
            raise RuntimeError(
                'Mismatch in node names between merged and unmerged distinct records: ' +
                    '"{unmerged_merged_key_diff}"'
            )
        node_count: int
        for node_name_plural, node_count in {k:len(v) for k,v in unmerged_node_cache_keys.items()}.items():
            if len(merged_node_cache_keys[node_name_plural]) != node_count:
                raise RuntimeError(
                    'Mismatch in node record counts between merged and unmerged distinct records: ' +
                        f'"{node_name_plural}", {len(merged_node_cache_keys[node_name_plural])} != {node_count}'
                )

        # validate each node type in the merged harmonized data set
        node: C3dcEtlModelNode
        for node in C3dcEtlModelNode:
            self._validate_merged_harmonized_node_data(node)

        _logger.info('Validation of marged harmonized data for study "%s" found no errors', study_id)

    def _cache_harmonized_record(
        self,
        study_id: str,
        transformation_name: str,
        record_participant_id_node: tuple[dict[str, any], str, str] = None,
        cache_key: tuple[str, str, str] = None
    ) -> dict[str, ]:
        """
        Cache harmonized record for speciifed study, transformation, participant and node. Note that if cache key
        is specified, record/participant/node tuple is optional and will be ignored. If cache key not provided then
        record/participant/node tuple must be specified in order to generate cache key.
        """
        if cache_key is None and record_participant_id_node is None:
            raise RuntimeError('Cache key or record/participant/node tuple must be specified')
        cache_key = cache_key or C3dcEtl.get_cache_key(*record_participant_id_node)
        # add to cache; { study => { (record hash, participant id, node) => [transformation names] } }
        self._harmonized_record_cache[study_id][cache_key] = self._harmonized_record_cache[study_id].get(cache_key, [])
        self._harmonized_record_cache[study_id][cache_key].append(transformation_name)

    def _cache_harmonized_data_set(self, transformation_name: str, data: dict[str, list[dict[str, any]]]) -> None:
        """ Cache records in specified harmonized data set """
        # harmonized data set must be dict of pluralized node names => node records
        participant_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)

        study: dict[str, any] = next(s for s in data[self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]])
        study_id: str = study[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.STUDY)]

        node_name_pluralized: str
        node_records: list[dict[str, any]]
        for node_name_pluralized, node_records in data.items():
            node_name: str = self._node_names_plural_to_singular[node_name_pluralized]
            if node_name not in (*C3dcEtl.OBSERVATION_NODES, C3dcEtlModelNode.REFERENCE_FILE):
                # only cache participant observations and reference files
                continue

            record: dict[str, any]
            for record in node_records:
                cacheable_record: dict[str, any] = C3dcEtl.get_cacheable_record(record, node_name)
                participant_id: str = record.get(participant_id_field_qualified, '')
                self._cache_harmonized_record(
                    study_id,
                    transformation_name,
                    (cacheable_record, participant_id, node_name)
                )

    def _add_participant_to_merged_data_set(
        self,
        participant: dict[str, any],
        participant_data_set: dict[str, list[dict[str, any]]],
        transformation_name: str
    ) -> None:
        """ Add records for specified participant from source transformation data to merged data set """
        participant_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT)
        participant_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)
        participant_id: str = participant[participant_id_field]
        if any(
            p[participant_id_field] == participant_id
                for p in
                    self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.PARTICIPANT]]
        ):
            raise RuntimeError(f'Error adding participant "{participant_id}" to merged data, record already exists')

        consent_group_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.CONSENT_GROUP, True)
        consent_group_id: str = participant[consent_group_id_field_qualified]
        merged_consent_group: dict[str, any] = self._get_merged_harmonized_record(
            C3dcEtlModelNode.CONSENT_GROUP,
            consent_group_id,
            True
        )

        study_id: str = merged_consent_group[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.STUDY, True)]

        # add participant's observations to matching observation collections in merged data set
        node: C3dcEtlModelNode
        for node in C3dcEtl.OBSERVATION_NODES:
            observation_id_field: str = C3dcEtl.get_node_id_field_name(node)
            observation_id_field_qualified: str = C3dcEtl.get_node_id_field_name(node, True)
            observation_id_collection: list[str] = participant.get(observation_id_field_qualified, [])
            observation_id: str
            node_name_plural: str = self._node_names_singular_to_plural[node]
            for observation_id in list(observation_id_collection):
                # add collection for this observation node if not already present in merged data set
                self._merged_harmonized_records[node_name_plural] = self._merged_harmonized_records.get(
                    node_name_plural,
                    []
                )

                # find the observation object matching this id
                observation: dict[str, any] = next(
                    o for o in participant_data_set[node_name_plural] if o[observation_id_field] == observation_id
                )
                cacheable_observation: dict[str, any] = C3dcEtl.get_cacheable_record(observation, node)
                cache_key: tuple[str, str, str] = C3dcEtl.get_cache_key(cacheable_observation, participant_id, node)
                if cache_key in self._harmonized_record_cache[study_id]:
                    #  same-file duplicate record (e.g. lab test), log and withhold from output
                    _logger.warning(
                        'Duplicate "%s" harmonized record found and suppressed for "%s":',
                        node,
                        participant_id
                    )
                    _logger.warning(observation)
                    dupe_key: tuple[str, str] = (participant_id, node)
                    self._duplicate_harmonized_records[study_id][dupe_key] = self._duplicate_harmonized_records.get(
                        dupe_key,
                        set()
                    )
                    self._duplicate_harmonized_records[study_id][dupe_key].add(json.dumps(cacheable_observation))
                    observation_id_collection.remove(observation_id)
                else:
                    self._merged_harmonized_records[node_name_plural].append(observation)

                # add to cache; (record hash, participant id) => { node => [transformation names] }
                self._cache_harmonized_record(study_id, transformation_name, cache_key=cache_key)

        # add participant to merged data set's list of participants
        self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.PARTICIPANT]].append(
            participant
        )

        # add participant id to merged consent group's list of participant ids
        merged_consent_group[participant_id_field_qualified].append(participant_id)

    def _update_participant_in_merged_data_set(
        self,
        participant: dict[str, any],
        participant_data_set: dict[str, list[dict[str, any]]],
        transformation_name: str
    ) -> None:
        """ Update records for specified participant from source transformation data into merged data set """
        participant_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT)
        participant_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)
        participant_id: str = participant[participant_id_field]
        # verify participant present in merged data set's participant and study participant id collections
        merged_participant: dict[str, any] = next(
            (
                p for p in
                    self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.PARTICIPANT]]
                        if p[participant_id_field] == participant_id
            ),
            None
        )
        if not merged_participant:
            raise RuntimeError(f'Unable to update participant "{participant_id}" in merged data, participant not found')

        consent_group_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.CONSENT_GROUP, True)
        consent_group_id: str = participant[consent_group_id_field_qualified]
        merged_consent_group: dict[str, any] = self._get_merged_harmonized_record(
            C3dcEtlModelNode.CONSENT_GROUP,
            consent_group_id,
            True
        )
        study_id: str = merged_consent_group[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.STUDY, True)]

        if participant_id not in merged_consent_group[participant_id_field_qualified]:
            raise RuntimeError(
                f'Unable to update participant "{participant_id}" in merged data, ' +
                    'id not in consent group participant id list'
            )

        # add participant's non-duplicate observations to matching observation collections in merged data set
        node: C3dcEtlModelNode
        for node in C3dcEtl.OBSERVATION_NODES:
            observation_id_field: str = C3dcEtl.get_node_id_field_name(node)
            observation_id_field_qualified: str = C3dcEtl.get_node_id_field_name(node, True)
            observation_id_collection: list[str] = participant.get(observation_id_field_qualified, [])
            observation_id: str
            node_name_plural: str = self._node_names_singular_to_plural[node]
            for observation_id in observation_id_collection:
                # add collection for this observation node if not already present in merged data set
                self._merged_harmonized_records[node_name_plural] = self._merged_harmonized_records.get(
                    node_name_plural,
                    []
                )

                # find the observation object matching this id
                observation: dict[str, any] = next(
                    o for o in participant_data_set[node_name_plural] if o[observation_id_field] == observation_id
                )
                cacheable_observation: dict[str, any] = C3dcEtl.get_cacheable_record(observation, node)
                cache_key: tuple[str, str, str] = C3dcEtl.get_cache_key(cacheable_observation, participant_id, node)
                if cache_key in self._harmonized_record_cache[study_id]:
                    #  cross-file duplicate record, log and withhold from output
                    _logger.warning(
                        'Duplicate "%s" harmonized record found and suppressed for "%s":',
                        node,
                        participant_id
                    )
                    _logger.warning(cacheable_observation)
                    dupe_key: tuple[str, str] = (participant_id, node)
                    self._duplicate_harmonized_records[study_id][dupe_key] = self._duplicate_harmonized_records.get(
                        dupe_key,
                        set()
                    )
                    self._duplicate_harmonized_records[study_id][dupe_key].add(json.dumps(cacheable_observation))
                else:
                    self._merged_harmonized_records[node_name_plural].append(observation)

                # add to cache; (record hash, participant id) => { node => [transformation names] }
                self._cache_harmonized_record(study_id, transformation_name, cache_key=cache_key)

    def _create_merged_json_etl_file(self, study_id: str, merged_output_file_path: str) -> None:
        """ Create merged harmonized output file for specified study with duplicate records removed """
        _logger.info(
            'Creating merged JSON ETL data file for study "%s" and saving to "%s"',
            study_id,
            merged_output_file_path
        )
        self._merged_harmonized_records = {}

        participant_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT)
        participant_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT, True)
        reference_file_id_field: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.REFERENCE_FILE)
        reference_file_id_field_qualified: str = C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.REFERENCE_FILE, True)

        merged_participant_ids: set[str] = set()
        merged_study: dict[str, any]
        merged_consent_group: dict[str, any]
        xform_name: str
        xform_data: dict[str, list[dict[str, any]]]
        # use copy to preserve transformation-specific data while de-duplicating records to create merged data set
        json_etl_data: dict[str, any] = copy.deepcopy(self._json_etl_data_sets[study_id])
        for xform_name, xform_data in json_etl_data.items():
            _logger.info('Merging data set for transformation "%s"', xform_name)

            # make sure merged output data set has containers for all node types found in transformation's data set
            node_name_plural: str
            for node_name_plural in xform_data:
                self._merged_harmonized_records[node_name_plural] = self._merged_harmonized_records.get(
                    node_name_plural,
                    []
                )

            if not self._merged_harmonized_records.get(self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]):
                # no data merged yet, add study and consent group to merged data set
                self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]] = \
                    xform_data[self._node_names_singular_to_plural[C3dcEtlModelNode.STUDY]]
                merged_study = self._get_merged_harmonized_record(C3dcEtlModelNode.STUDY, study_id, True)
                merged_study[reference_file_id_field_qualified] = []

                self._merged_harmonized_records[self._node_names_singular_to_plural[C3dcEtlModelNode.CONSENT_GROUP]] = \
                    xform_data[self._node_names_singular_to_plural[C3dcEtlModelNode.CONSENT_GROUP]]
                merged_consent_group_ids: list[str] = self._get_merged_harmonized_node_ids(
                    C3dcEtlModelNode.CONSENT_GROUP
                )
                if len(merged_consent_group_ids) != 1:
                    raise RuntimeError(f'No merged consent group ids found for study {study_id}')
                merged_consent_group = self._get_merged_harmonized_record(
                    C3dcEtlModelNode.CONSENT_GROUP,
                    merged_consent_group_ids[0],
                    True
                )
                merged_consent_group[participant_id_field_qualified] = []

            # enumerate and add/update participants in this data set while checking for and logging duplicates
            participant: dict[str, any]
            for participant in xform_data[self._node_names_singular_to_plural[C3dcEtlModelNode.PARTICIPANT]]:
                participant_id: str = participant[participant_id_field]
                if participant_id not in merged_participant_ids:
                    self._add_participant_to_merged_data_set(participant, xform_data, xform_name)
                    merged_participant_ids.add(participant_id)
                else:
                    self._update_participant_in_merged_data_set(participant, xform_data, xform_name)

            # enumerate and add reference file entries as/if needed
            reference_file: dict[str, any]
            for reference_file in xform_data[self._node_names_singular_to_plural[C3dcEtlModelNode.REFERENCE_FILE]]:
                reference_file_id: str = reference_file[reference_file_id_field]
                cacheable_reference_file: dict[str, any] = C3dcEtl.get_cacheable_record(
                    reference_file,
                    C3dcEtlModelNode.REFERENCE_FILE
                )
                cache_key: tuple[str, str, str] = C3dcEtl.get_cache_key(
                    cacheable_reference_file,
                    '',
                    C3dcEtlModelNode.REFERENCE_FILE
                )
                if cache_key in self._harmonized_record_cache[study_id]:
                    continue
                # add reference file to merged data set's list of reference files
                self._merged_harmonized_records[
                    self._node_names_singular_to_plural[C3dcEtlModelNode.REFERENCE_FILE]
                ].append(reference_file)

                # add reference file id to merged study's list of reference file ids
                merged_study[reference_file_id_field_qualified].append(reference_file_id)

                # cache reference file record
                self._cache_harmonized_record(study_id, xform_name, cache_key=cache_key)

        # save merged data to specified output file
        self._c3dc_file_manager.write_file(
            json.dumps(self._merged_harmonized_records, indent=2).encode('utf-8'),
            merged_output_file_path
        )

        _logger.info(
            '%s merged harmonized records for study "%s"',
            ', '.join(
                f'{len(v)} {self._node_names_plural_to_singular[k]}' for k,v in self._merged_harmonized_records.items()
            ),
            study_id
        )

    def _create_harmonized_duplicate_record_report_file(self, study_id: str, duplicate_record_report_path: str) -> None:
        """ Save duplicate harmonized record details to file specified in config """
        dupe_recs: dict[tuple[str, str, str], list[str]] = {
            k:v for k,v in self._harmonized_record_cache.get(study_id, {}).items() if len(v) > 1
        }
        if not dupe_recs:
            _logger.info('No duplicate harmonized records found/suppressed for study "%s"', study_id)
            return

        _logger.warning(
            'Saving harmonized duplicate record report for study "%s" to "%s"',
            study_id,
            duplicate_record_report_path
        )
        _logger.warning(
            '%d total duplicate harmonized records found and suppressed for %d distinct participants',
            len(dupe_recs),
            len(set(p for (_, p, _) in dupe_recs.keys()))
        )

        if not self._duplicate_harmonized_records.get(study_id):
            raise RuntimeError(f'Duplicate harmonized record report cache is empty for study "{study_id}"')

        node: C3dcEtlModelNode | str
        node_counts: dict[C3dcEtlModelNode, int] = {}
        for node in C3dcEtlModelNode:
            node_counts[node] = sum(1 for (h, p, n) in dupe_recs if n == node)
        _logger.warning(
            '%s duplicate records found/suppressed for study "%s"',
            ', '.join(f'{v} {k}' for k,v in node_counts.items()),
            study_id
        )

        if not duplicate_record_report_path:
            _logger.warning(
                'Duplicate report output path "duplicate_report_path" not specified in study config, ' +
                    'duplicate report CSV file not written'
            )
            return

        # aggregate dupes by participant => node => [xforms]
        parts_nodes_xforms: dict[str, dict[str, set[str]]] = {}
        participant_id: str
        xforms: list[str] | set[str]
        for (_, participant_id, node), xforms in dupe_recs.items():
            parts_nodes_xforms[participant_id] = parts_nodes_xforms.get(participant_id, {})
            parts_nodes_xforms[participant_id][node] = parts_nodes_xforms[participant_id].get(node, set())
            parts_nodes_xforms[participant_id][node].update(xforms)

        # create tabular reports for participant => dupe node/transform names
        # participant id | diagnosis      | diagnosis_dupe_rec | ... | survival        | survival_dupe_rec
        # participant 1  | xform1, xform2 | <dupe rec(s)>      | ... | xform1, xform 3 | <dupe rec(s)>
        # participant 2  | xform2, xform3 | <dupe rec(s)>      | ... | xform2, xform 4 | <dupe rec(s)>
        # ...
        dupe_report_records: list[dict[str, str]] = []
        nodes_xforms: dict[str, set[str]]
        for participant_id, nodes_xforms in dict(sorted(parts_nodes_xforms.items())).items():
            dupe_report_record: dict[str, str] = {}
            dupe_report_record[C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT)] = participant_id
            for node in C3dcEtlModelNode:
                xforms: list[str] = sorted(set(nodes_xforms.get(node, set())))
                dupe_report_record[str(node)] = ', '.join(xforms)
                dupe_report_record[f'{str(node)}_dupe_recs'] = '\n'.join(
                    self._duplicate_harmonized_records[study_id].get((participant_id, node), set())
                )
            dupe_report_records.append(dupe_report_record)

        fieldnames: list[str] = (
            [C3dcEtl.get_node_id_field_name(C3dcEtlModelNode.PARTICIPANT)] +
                sorted(k for k in dupe_report_records[0] if k != 'participant_id')
        )
        output_io: io.StringIO = io.StringIO()
        writer: csv.DictWriter = csv.DictWriter(output_io, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dupe_report_records)
        self._c3dc_file_manager.write_file(output_io.getvalue().encode('utf-8'), duplicate_record_report_path)


def print_usage() -> None:
    """ Print script usage """
    _logger.info('usage: python %s [optional config file name/path if not .env]', sys.argv[0])


def main() -> None:
    """ Script entry point """
    if len(sys.argv) > 2:
        print_usage()
        return

    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    config_file: str = sys.argv[1] if len(sys.argv) == 2 else '.env'
    if not c3dc_file_manager.file_exists(config_file):
        raise FileNotFoundError(f'Config file "{config_file}" not found')
    config: dict[str, str] = dotenv.dotenv_values(
        stream=io.StringIO(c3dc_file_manager.read_file(config_file).decode('utf-8'))
    )
    etl: C3dcEtl = C3dcEtl(config)
    etl.create_json_etl_files()
    if not etl.validate_json_etl_data():
        raise RuntimeError('Harmonized output data failed JSON validation')


if __name__ == '__main__':
    main()
