""" C3DC ETL File Creator """
import csv
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
import uuid
import warnings

from c3dc_etl_model_node import C3dcEtlModelNode
import dotenv
import jsonschema
from jsonschema import ValidationError
import petl

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
    """ Build C3DC harmonized data file (JSON) from CCDI template source file (XLSX) """
    TYPE_NAME_CLASS_MAP: dict[str, type | list[type]] = {
        'array': list,
        'integer': int,
        'string': str
    }

    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._json_schema_url: str = config.get('JSON_SCHEMA_URL')
        self._json_schema: dict[str, any] = {}
        self._json_schema_nodes: dict[str, any] = {}
        self._json_schema_property_enum_values: dict[str, list[str]] = {}
        self._json_etl_data_sets: dict[str, any] = {}
        self._output_type_source_tabs: dict[str, str] = {}
        self._raw_etl_data_tables: dict[str, any] = {}
        self._random: random.Random = random.Random()
        self._c3dc_file_manager: C3dcFileManager = C3dcFileManager()

        # Remote study config should contain env-agnostic info like mappings, local study config
        # should contain info specific to the local env like file paths; remote and local will be
        # merged together to form the final study configuration object
        self._study_configurations: list[dict[str, any]] = json.loads(config.get('STUDY_CONFIGURATIONS', '[]'))
        self._study_configurations = [sc for sc in self._study_configurations if sc.get('active', True)]

        self.load_json_schema()
        self.load_transformations()

        # study config verification loads the source data and can be relatively slow
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
                    read_only=True
                )
            else:
                tmp_file: any
                tbl = petl.empty()
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    tmp_file.write(self._c3dc_file_manager.read_file(source_file_path))
                    tmp_file.flush()
                    tmp_file.close()
                    tbl = petl.fromxlsx(tmp_file.name, xl_sheet_name, read_only=True)

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
        return tbl

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
        except ValidationError:
            _logger.warning(
                'ETL data for transformation %s (study %s) failed schema validation:',
                transformation_name,
                study_id
            )
            validator: jsonschema.Validator = jsonschema.Draft7Validator(self._json_schema)
            validation_error: ValidationError
            for validation_error in validator.iter_errors(self._json_etl_data_sets[study_id][transformation_name]):
                _logger.warning('%s: %s', validation_error.json_path, validation_error.message)
        return False

    def _save_json_etl_data(self, study_id: str, transformation: dict[str, any]) -> None:
        """ Save JSON ETL data for specified transformation to designated output file """
        _logger.info('Saving JSON ETL data to %s', transformation.get('output_file_path'))
        self._c3dc_file_manager.write_file(
            json.dumps(self._json_etl_data_sets[study_id][transformation.get('name')], indent=2).encode('utf-8'),
            transformation.get('output_file_path')
        )

    def _load_source_data(self, study_id: str, transformation: dict[str, any], force_reload: bool=False) -> any:
        """ Load raw ETL data from source file specified in config """
        if not force_reload and (self._raw_etl_data_tables or {}).get(study_id, {}).get(transformation.get('name')):
            return self._raw_etl_data_tables[study_id][transformation.get('name')]

        source_field: str
        output_field: str
        mapping: dict[str, any]
        output_type: str
        self._output_type_source_tabs = {}
        for mapping in transformation.get('mappings'):
            source_field = mapping.get('source_field', '').strip()
            output_field = mapping.get('output_field', '').strip()
            output_type = output_field.split('.')[0]
            if source_field and output_field and '.' in source_field:
                if ',' in source_field and source_field.startswith('[') and source_field.endswith(']'):
                    # compound field, parse to determine all source tabs
                    src_fld: str
                    src_tab: str = ''
                    for src_fld in [s.strip() for s in source_field.lstrip('[').rstrip(']').split(',')]:
                        src_tab = src_fld.split('.')[0].strip()
                        if src_tab and self._output_type_source_tabs.get(output_type, src_tab) != src_tab:
                            raise RuntimeError(
                                f'Output type "{output_type}" mapped to multiple source tabs: "{source_field}"'
                            )
                        self._output_type_source_tabs[output_type] = src_tab
                else:
                    source_tab: str = source_field.split('.')[0].strip()
                    if source_tab and self._output_type_source_tabs.get(output_type, source_tab) != source_tab:
                        raise RuntimeError(
                            f'Output type "{output_type}" mapped to multiple source tabs: "{source_field}"'
                        )
                    self._output_type_source_tabs[output_type] = source_tab

        if set(self._output_type_source_tabs.keys()).difference(set(self._json_schema_nodes.keys())):
            raise RuntimeError(
                'Node type(s) specified in mapping not found in schema: ' +
                    set(self._output_type_source_tabs.keys()).difference(set(self._json_schema_nodes.keys())),
            )
        source_tab: str
        for output_type, source_tab in dict(sorted(self._output_type_source_tabs.items())).items():
            _logger.info('Loading output type "%s" from spreadsheet tab "%s"', output_type, source_tab)
            raw_etl_data_tbl: any = self._get_petl_table_from_source_file(
                transformation.get('source_file_path'), source_tab
            )

            # remove empty/invalid rows e.g. where id ([source_tab]_id) is empty
            raw_etl_data_tbl = petl.select(raw_etl_data_tbl, lambda rec, id_field=f'{source_tab}_id': rec[id_field])

            # add row numbers for logging/auditing
            raw_etl_data_tbl = petl.addrownumbers(raw_etl_data_tbl, start=2, field='source_file_row_num')
            self._raw_etl_data_tables[study_id] = self._raw_etl_data_tables.get(study_id, {})
            self._raw_etl_data_tables[study_id][transformation.get('name')] = self._raw_etl_data_tables[study_id].get(
                transformation.get('name'), {}
            )
            self._raw_etl_data_tables[study_id][transformation.get('name')][output_type] = raw_etl_data_tbl
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

    def _get_json_schema_node_property_converted_value(
        self,
        node_type_dot_property_name: str,
        value: any
    ) -> list | int | str:
        """ Get output value converted to JSON schema type for specified property array, integer, string """
        if value is None:
            return None

        if isinstance(value, (list, set, tuple)):
            _logger.critical('Unsupported value type (list|set|tuple) for conversion:')
            _logger.critical(value)
            raise RuntimeError('Unsupported value type (list|set|tuple) for conversion')

        if '.' not in node_type_dot_property_name:
            raise RuntimeError(f'Unexpected schema property name ("." not present): "{node_type_dot_property_name}"')

        json_type: str = self._get_json_schema_node_property_type(node_type_dot_property_name)
        if json_type not in C3dcEtl.TYPE_NAME_CLASS_MAP:
            raise RuntimeError(f'Schema type "{json_type}" not in type name class map')

        python_type = C3dcEtl.TYPE_NAME_CLASS_MAP.get(json_type)
        if python_type == list:
            if node_type_dot_property_name not in self._json_schema_property_enum_values:
                return [value]

            # multi-valued properties may specify multiple values using semi-colon delimiter
            vals: dict[str, str] = {v:v for v in value.split(';')}
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
                macro_text.lower() in ('sum', 'uuid') or
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
        if source_field.startswith('[') and source_field.endswith(']'):
            # strip extra spaces; csv module parses "field 1, field 2" into ["field 1", " field 2"]
            source_fields: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
            if not {s for s in source_fields if s != 'string_literal'}.issubset(set(source_header)):
                errors.append(
                    f'{transformation_name} ({study_id}): compound source field not present in source data: {mapping}'
                )
                _logger.warning({s for s in source_fields if s != 'string_literal'})
        elif source_field not in source_header:
            errors.append(f'{transformation_name} ({study_id}): source field not present in source data: {mapping}')
        output_field: str = mapping.get('output_field')
        if not output_field:
            errors.append(f'{transformation_name} ({study_id}): mapping output field not specified: {mapping}')
        output_field_parts = output_field.split('.')
        output_node: str = output_field_parts.pop(0)
        output_property: str = '.'.join(output_field_parts)
        if (
            output_node not in self._json_schema_nodes or
            output_property not in self._json_schema_nodes.get(output_node, {}).get('properties', {})
        ):
            errors.append(f'{transformation_name}: mapping output field invalid: {mapping}')

        replacement_entry: dict[str, str]
        for replacement_entry in mapping.get('replacement_values', []):
            if 'old_value' not in replacement_entry or 'new_value' not in replacement_entry:
                errors.append(
                    f'{transformation_name}: replacement entry missing new or old value: ' +
                    str(replacement_entry)
                )

            old_value: str = replacement_entry.get('old_value', '*')
            new_value: any = replacement_entry.get('new_value', '')

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
            # each transformation consists of source tab => source data pairings, e.g.
            # 'diagnosis' => {...}
            source_header: list[str] = []
            output_type: str
            source_tab: str
            raw_etl_data_tbl: any
            for output_type, raw_etl_data_tbl in self._raw_etl_data_tables[study_id][transformation['name']].items():
                source_tab = self._output_type_source_tabs.get(output_type)
                if not source_tab:
                    errors.append(f'No source tab match for output type "{output_type}"')
                source_header.extend(f'{source_tab}.{h}' for h in petl.header(raw_etl_data_tbl))

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
        
        Values are expected to be passed without transformation for the most part, however the mapping implementation
        supports required type conversions such as string-to-list (race, ethnicity, etc) as well as bespoke
        transformation/replacement/injection such as diagnosis.tumor_classification => 'Primary' for all diagnoses.

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

        node_type: str = source_record.get('type', '')
        source_tab: str = self._output_type_source_tabs.get(node_type)
        source_field: str = self._get_source_field(mapping, node_type, source_tab)
        source_value: str = source_record.get(source_field, None)

        default_value: any = mapping.get('default_value', None)

        replacement_entry: dict[str, str]
        for replacement_entry in mapping.get('replacement_values', []):
            old_value: str = replacement_entry.get('old_value', '*')
            new_value: any = replacement_entry.get('new_value', None)

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
                elif macro_text.lower() == 'sum':
                    # source field should contain list of source fields to be added together
                    if not (source_field.startswith('[') and source_field.endswith(']')):
                        msg = (
                            f'Invalid source field "{source_field} for "sum" macro in row ' +
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
                        if not str(addend or '').strip():
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
                            addend = float(addend)
                            addends.append(addend if not addend.is_integer() else int(addend))
                    new_val = sum(addends) if all(a is not None for a in addends) else default_value
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

    def _get_source_field(self, mapping: dict[str, any], node_type: C3dcEtlModelNode, source_tab: str) -> str:
        """ Get source field for specified mapping, node type and source tab """
        source_field: str = mapping.get('source_field').strip(' \'"')
        if source_field.startswith(f'{node_type}.'):
            source_field = source_field[len(f'{node_type}.'):]
        elif source_tab and source_tab != node_type and source_field.startswith(f'{source_tab}.'):
            source_field = source_field[len(f'{source_tab}.'):]
        return source_field

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
            _logger.warning('No mappings found for type %s, unable to transform record', node_type)
            return []

        # may be blank for types w/o source tab mapping such as reference_file
        source_tab: str = self._output_type_source_tabs.get(node_type)
        output_source_field_map: dict[str, str] = {}

        # if there are multiple type group indexes (multiple records are mapped) then default
        # values to base ("*") values first and then overwrite individual mapped fields for
        # each mapping sub-group specified for that type group index
        base_record: dict[str, any] = {}

        type_group_index: str
        mappings: list[dict[str, any]]
        for type_group_index, mappings in type_group_index_mappings.items():
            output_record: dict[str, any] = {}
            output_record.update(base_record)
            mapping: dict[str, any]
            for mapping in mappings:
                output_field: str = mapping.get('output_field')
                output_field_property: str = output_field[len(f'{node_type}.'):]

                default_value: any = mapping.get('default_value')

                source_field: str = self._get_source_field(mapping, node_type, source_tab)
                source_value: str = source_record.get(source_field, None)

                if source_value in ('', None) and default_value is not None:
                    source_value = default_value

                if output_field not in output_source_field_map:
                    output_source_field_map[output_field] = source_field

                allowed_values: set[str] = set(self._json_schema_property_enum_values.get(output_field, []))

                output_value: any = self._get_mapped_output_value(mapping, source_record)

                if output_value is not None:
                    # mapped (aka replacement) value found in mapping transformation
                    if allowed_values and not C3dcEtl.is_allowed_value(output_value, allowed_values):
                        _logger.warning(
                            'Mapped value "%s" not allowed for output field "%s" (source field "%s")',
                            output_value,
                            output_field,
                            source_field
                        )
                        continue
                    output_record[output_field_property] = output_value
                else:
                    # passthrough (not explicitly mapped) values need to be converted to destination field schema type
                    converted_source_value: any = self._get_json_schema_node_property_converted_value(
                        output_field,
                        source_value
                    )

                    if converted_source_value is not None or self._is_json_schema_node_property_required(output_field):
                        # only set destination field if value is not None or field is required
                        # because validation will fail for (non-required) enum field set to None
                        if allowed_values and not C3dcEtl.is_allowed_value(converted_source_value, allowed_values):
                            _logger.warning(
                                'Passthrough source value "%s" not allowed for output field "%s" (source field "%s")',
                                converted_source_value,
                                output_field,
                                source_field
                            )
                            continue
                        output_record[output_field_property] = converted_source_value

            # verify that record is valid and contains all required properties
            record_valid: bool = True
            required_properties: dict[str, any] = self._get_json_schema_node_required_properties(node_type)
            required_property: str
            for required_property in required_properties:
                schema_field: str = f'{node_type}.{required_property}'
                if output_record.get(required_property, None) in ('', None):
                    record_valid = False
                    _logger.warning(
                        'Required output field "%s" (source field "%s") has null value for source record file "%s"',
                        schema_field,
                        output_source_field_map.get(schema_field, '*not mapped*'),
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

    def _get_latest_follow_up_record(self, follow_ups: list[dict[str, any]]) -> dict[str, any]:
        """ Find and return follow up record having most recent event age, prioritizing 'Dead' over 'Alive' """
        # check for any 'Alive' records having event age greater than any 'Dead' records
        dead_fus: list[dict[str, any]] = [f for f in follow_ups if f['vital_status'] == 'Dead']
        max_dead_fu_age: int = max((int(float(f['age_at_follow_up'])) for f in dead_fus), default=sys.maxsize)
        alive_fus: list[dict[str, any]] = [f for f in follow_ups if f['vital_status'] == 'Alive']
        max_alive_fu_age: int = max((int(float(f['age_at_follow_up'])) for f in alive_fus), default=-999)
        if max_alive_fu_age > -999 and max_alive_fu_age > max_dead_fu_age:
            _logger.warning(
                (
                    'Follow up record having status "Alive" preceded by record with status "Dead", ' +
                    'unable to determine latest follow up record: %s'
                ),
                follow_ups
            )
            return {}

        final_follow_up: dict[str, any] = {}
        follow_up: dict[str, any]
        for follow_up in follow_ups:
            if not final_follow_up:
                final_follow_up = follow_up
                continue

            if (
                follow_up['vital_status'] == 'Dead'
                or
                int(float(follow_up['age_at_follow_up'])) > int(float(final_follow_up['age_at_follow_up']))
            ):
                final_follow_up = follow_up
                break

        return final_follow_up

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
        transform_method: any = getattr(self, transform_method_name, lambda: None)
        if (
            transform_method is None or
            not hasattr(self, transform_method_name) or
            not callable(transform_method)
        ):
            return self._transform_record_default(transformation, node_type, source_record)

        return transform_method(transformation, node_type, source_record)

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

        study_id_field: str = f'{C3dcEtlModelNode.STUDY}_id'
        study_id_field_remote: str = f'{C3dcEtlModelNode.STUDY}.{C3dcEtlModelNode.STUDY}_id'
        pat_id_field: str = f'{C3dcEtlModelNode.PARTICIPANT}_id'
        pat_id_field_remote: str = f'{C3dcEtlModelNode.PARTICIPANT}.{pat_id_field}'
        dx_id_field: str = f'{C3dcEtlModelNode.DIAGNOSIS}_id'
        dx_id_field_remote: str = f'{C3dcEtlModelNode.DIAGNOSIS}.{dx_id_field}'
        surv_id_field: str = f'{C3dcEtlModelNode.SURVIVAL}_id'
        surv_id_field_remote: str = f'{C3dcEtlModelNode.SURVIVAL}.{surv_id_field}'
        ref_id_field: str = f'{C3dcEtlModelNode.REFERENCE_FILE}_id'
        ref_id_field_remote: str = f'{C3dcEtlModelNode.REFERENCE_FILE}.{ref_id_field}'

        nodes: dict[str, any] = {
            'diagnoses': [],
            'participants': [],
            'reference_files': [],
            'studies': [],
            'survivals': []
        }

        raw_etl_data_tbls: dict[str, any] = self._raw_etl_data_tables.get(study_id, {}).get(transformation.get('name'))
        if not raw_etl_data_tbls:
            raise RuntimeError('No raw ETL data found')

        # build study node and add to node collection
        src_recs: list[dict[str, any]] = list(petl.dicts(raw_etl_data_tbls.get(C3dcEtlModelNode.STUDY, petl.empty())))
        if len(src_recs) != 1:
            raise RuntimeError(f'Unexpected number of study records ({len(src_recs)}) found in source data')
        src_rec: dict[str, any] = src_recs[0]
        study: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.STUDY, src_rec)
        if len(study) != 1:
            raise RuntimeError(f'Unexpected number of study nodes built ({len(study)}), check mapping')
        study = study[0]
        study[pat_id_field_remote] = []
        study[ref_id_field_remote] = []

        # build reference file nodes and add to node collection
        reference_files: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.REFERENCE_FILE)
        reference_file: dict[str, any]
        for reference_file in reference_files:
            reference_file[study_id_field_remote] = study[study_id_field]
            study[ref_id_field_remote].append(reference_file[ref_id_field])
        nodes['reference_files'].extend(reference_files)

        # add participant, diagnosis, and survival records to match source data records
        pat_src_recs: list[dict[str, any]] = list(
            petl.dicts(raw_etl_data_tbls.get(C3dcEtlModelNode.PARTICIPANT, petl.empty()))
        )
        dx_src_recs: list[dict[str, any]] = list(
            petl.dicts(raw_etl_data_tbls.get(C3dcEtlModelNode.DIAGNOSIS, petl.empty()))
        )
        if not dx_src_recs:
            _logger.warning('No diagnosis data available for harmonization')
        surv_src_recs: list[dict[str, any]] = list(
            petl.dicts(raw_etl_data_tbls.get(C3dcEtlModelNode.SURVIVAL, petl.empty()))
        )
        if not surv_src_recs:
            _logger.warning('No survival data available for harmonization')
        pat_src_rec: dict[str, any]
        pat_ids: list[str] = dict.fromkeys(p.get(pat_id_field) for p in pat_src_recs)
        if len(pat_ids) != len(pat_src_recs):
            raise RuntimeError(
                f'Unique participant id count ({len(pat_ids)}) not equal to ' +
                f'participant record count ({len(pat_src_recs)})'
            )
        for pat_src_rec in pat_src_recs:
            participant_id: str = pat_src_rec.get(pat_id_field)
            participant_id = str(participant_id).strip() if participant_id is not None else participant_id
            try:
                if not (participant_id or '').strip():
                    _logger.warning(
                        '%s (%s): Participant id missing or invalid for "%s" source row %s, excluding',
                        transformation.get('name'),
                        study_id,
                        C3dcEtlModelNode.PARTICIPANT,
                        pat_src_rec['source_file_row_num']
                    )
                    continue
            except AttributeError:
                _logger.warning('Error checking participant id: %s', pat_src_rec)
                raise

            participant: list[dict[str, any]] = self._build_node(
                transformation,
                C3dcEtlModelNode.PARTICIPANT,
                pat_src_rec
            )
            if len(participant) != 1:
                _logger.warning(
                    '%s (%s): Unexpected number of participant nodes (%d) built for participant "%s", excluding',
                    transformation.get('name'),
                    study_id,
                    len(participant),
                    participant_id
                )
                participant = None
                continue

            diagnoses: list[dict[str, any]] = []
            if dx_src_recs:
                src_recs: list[dict[str, any]] = [
                    d for d in dx_src_recs if d.get(pat_id_field_remote) == participant_id
                ]
                for src_rec in src_recs:
                    # if anatomic_site has multiple values separated by semi-colons (;) then process as multiple
                    # diagnosis records for each distinct anatomic site with all other properties kept the same
                    dxes: list[dict[str, any]]
                    anatomic_sites: list[str]
                    src_rec_base: dict[str, any]
                    if ';' in src_rec.get('anatomic_site', ''):
                        anatomic_sites = list(set(s.strip() for s in src_rec['anatomic_site'].split(';') if s.strip()))
                        src_rec_base: dict[str, any] = json.loads(json.dumps(src_rec))
                        _logger.info(
                            (
                                '%s (%s): Diagnosis "%s" contains %d distinct delimited anatomic site(s), ' +
                                'processing as separate diagnosis records per site'
                            ),
                            transformation.get('name'),
                            study_id,
                            src_rec['diagnosis_id'],
                            len(anatomic_sites)
                        )
                    else:
                        anatomic_sites = [src_rec.get('anatomic_site')]
                        src_rec_base = src_rec

                    anatomic_site: str
                    for anatomic_site in anatomic_sites:
                        src_rec_base['anatomic_site'] = anatomic_site
                        dxes = self._build_node(transformation, C3dcEtlModelNode.DIAGNOSIS, src_rec_base)
                        if not dxes:
                            _logger.warning(
                                '%s (%s): Unable to build "%s" node for participant "%s"%s',
                                transformation.get('name'),
                                study_id,
                                C3dcEtlModelNode.DIAGNOSIS,
                                participant_id,
                                '' if len(anatomic_sites) <= 1 else f' (anatomic_site "{anatomic_site}")'
                            )
                        diagnoses.extend(dxes)

            survivals: list[dict[str, any]] = []
            if surv_src_recs:
                src_recs: list[dict[str, any]] = [
                    s for s in surv_src_recs if s.get(pat_id_field_remote) == participant_id
                ]
                if src_recs:
                    survival: dict[str, any] = self._get_latest_follow_up_record(src_recs)
                    survs: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.SURVIVAL, survival)
                    if not survs:
                        _logger.warning(
                            '%s (%s): Unable to build "%s" node for participant "%s", invalid source record(s)',
                            transformation.get('name'),
                            study_id,
                            C3dcEtlModelNode.SURVIVAL,
                            participant_id
                        )
                    survivals.extend(survs)
                else:
                    _logger.warning(
                        '%s (%s): Unable to build "%s" node for participant "%s", no matching source record(s)',
                        transformation.get('name'),
                        study_id,
                        C3dcEtlModelNode.SURVIVAL,
                        participant_id
                    )

            participant = participant[0]
            participant[dx_id_field_remote] = []
            diagnosis: dict[str, any]
            for diagnosis in diagnoses:
                diagnosis[pat_id_field_remote] = participant[pat_id_field]
                participant[dx_id_field_remote].append(diagnosis[dx_id_field])

            participant[surv_id_field_remote] = []
            survival: dict[str, any]
            for survival in survivals:
                survival[pat_id_field_remote] = participant[pat_id_field]
                participant[surv_id_field_remote].append(survival[surv_id_field])

            participant[study_id_field_remote] = study[study_id_field]
            study[pat_id_field_remote].append(participant[pat_id_field])

            nodes['diagnoses'].extend(diagnoses)
            nodes['survivals'].extend(survivals)
            nodes['participants'].append(participant)

        nodes['studies'].append(study)

        self._json_etl_data_sets[study_id] = self._json_etl_data_sets.get(study_id) or {}
        self._json_etl_data_sets[study_id][transformation.get('name')] = nodes

        _logger.info(
            (
                '1 study, %d diagnosis, %d survival, %d participant, %d reference file ' +
                'records built for transformation "%s"'
            ),
            len(nodes['diagnoses']),
            len(nodes['survivals']),
            len(nodes['participants']),
            len(nodes['reference_files']),
            transformation.get('name')
        )

        return self._json_etl_data_sets[study_id][transformation.get('name')]

    def _create_json_etl_file(self, study_id: str, transformation: dict[str, any]) -> None:
        """ Create JSON ETL data file for specified raw source data set """
        _logger.info('Creating JSON ETL data file for transformation "%s" (%s)', transformation.get('name'), study_id)
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

    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    config_file: str = sys.argv[1] if len(sys.argv) == 2 else '.env'
    if not c3dc_file_manager.file_exists(config_file):
        raise FileNotFoundError(f'Config file "{config_file}" not found')
    config: dict[str, str] = dotenv.dotenv_values(
        stream=io.StringIO(c3dc_file_manager.read_file(config_file).decode('utf-8'))
    )
    etl: C3dcEtl = C3dcEtl(config)
    etl.create_json_etl_files()
    etl.validate_json_etl_data()


if __name__ == '__main__':
    main()
