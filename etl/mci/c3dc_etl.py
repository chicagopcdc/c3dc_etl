""" C3DC ETL File Creator """
import csv
import hashlib
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

from c3dc_etl_model_node import C3dcEtlModelNode
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


class C3dcEtl:
    """ Build C3DC json ETL file from (XLSX) source file """
    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._json_schema_url: str = config.get('JSON_SCHEMA_URL')
        self._json_schema: dict[str, any] = {}
        self._json_schema_nodes: dict[str, any] = {}
        self._json_etl_data_sets: dict[str, any] = {}
        self._raw_etl_data_objects: dict[str, dict[str, list[dict[str, any]]]] = {}
        self._random: random.Random = random.Random()

        # Remote study config should contain env-agnostic info like mappings, local study config
        # should contain info specific to the local env like file paths; remote and local will be
        # merged together to form the final study configuration object
        self._study_configurations: list[dict[str, any]] = json.loads(config.get('STUDY_CONFIGURATIONS', '[]'))
        self._study_configurations = [sc for sc in self._study_configurations if sc.get('active', True)]
        self._source_field_output_types: dict[str, str] = {}

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
    def raw_etl_data_objects(self) -> dict[str, dict[str, list[dict[str, any]]]]:
        """ Get internal source data objects, loading if needed """
        if not self._raw_etl_data_objects:
            study_configuration: dict[str, any]
            for study_configuration in self._study_configurations:
                self._load_study_data(study_configuration)
        return self._raw_etl_data_objects

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
            with open(C3dcEtl.url_to_path(url), 'rb') as local_file:
                url_content = local_file.read()
        else:
            with requests.get(url, timeout=30) as response:
                response.raise_for_status()
                url_content = response.content

        return json.loads(url_content)

    @staticmethod
    def is_number(value: str):
        """ Determine whether specified string is number (float or int) """
        try:
            float(value)
        except (TypeError, ValueError):
            return False
        return True

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

                # populate internal cache of source field=>output type pairs (non-compound mappings only)
                mapping: dict[str, any]
                for mapping in remote_transform.get('mappings', []):
                    source_field: str = mapping.get('source_field')
                    if source_field == '[string_literal]':
                        continue
                    if source_field.startswith('[') and source_field.endswith(']'):
                        # composite/derived source field, strip extra spaces; note csv
                        # module parses "field1, field2" into ["field1", " field2"]
                        sub_fields: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
                        sub_field: str
                        for sub_field in sub_fields:
                            self._source_field_output_types[sub_field] = (
                                self._get_json_schema_node_property_type(mapping.get('output_field'))
                            )
                    else:
                        self._source_field_output_types[source_field] = (
                            self._get_json_schema_node_property_type(mapping.get('output_field'))
                        )

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
        self._json_schema = C3dcEtl.get_url_content(self._json_schema_url)
        if save_local_copy:
            with open(f'./{os.path.basename(urlparse(self._json_schema_url).path)}', 'wb') as file:
                json.dump(self._json_schema, file)

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
        with open(transformation.get('output_file_path'), 'w', encoding='utf-8') as output_file:
            _logger.info('Saving JSON ETL data to %s', transformation.get('output_file_path'))
            json.dump(self._json_etl_data_sets[study_id][transformation.get('name')], output_file, indent=2)

    def _load_source_data(self, study_id: str, transformation: dict[str, any]) -> list[dict[str, any]]:
        """ Load raw ETL data from source path specified in config """
        msg: str
        recs: list[dict[str, any]] = []
        rec: dict[str, any]

        # load source file metadata from manifest file
        manifests: dict[str, dict[str, any]] = {}
        if transformation.get('source_file_path') and os.path.exists(transformation.get('source_file_path')):
            with open(transformation.get('source_file_manifest_path'), encoding='utf-8') as fp:
                reader: csv.DictReader = csv.DictReader(fp, delimiter='\t')
                for rec in reader:
                    manifests[rec['file_name']] = dict(rec)

        # load source data from individual json files
        source_file_parent_path: str = transformation.get('source_file_path')
        source_file_names: list[str] = sorted(
            f for f in os.listdir(source_file_parent_path) if os.path.isfile(os.path.join(source_file_parent_path, f))
        )
        source_file_name: str
        for source_file_name in source_file_names:
            with open(os.path.join(source_file_parent_path, source_file_name), 'r', encoding='utf-8') as fp:
                # construct limited object from raw source data limited to source fields specified in mappings
                obj: any = json.load(fp)
                rec = {'source_file_name': source_file_name, 'manifest': manifests.get(source_file_name, {})}

                source_field_name: str
                source_field_output_type: str
                # retrieve root-level properties such as 'upi'
                for source_field_name, source_field_output_type in {
                    k:v for k,v in self._source_field_output_types.items() if k in obj
                }.items():
                    if source_field_name in rec and source_field_output_type != 'array':
                        msg = f'Duplicate source field "{source_field_name}" found in file "{source_file_name}"'
                        _logger.fatal(msg)
                        raise RuntimeError(msg)
                    rec[source_field_name] = obj.get(source_field_name)

                # get properties such as DEMOGRAPHY=>DM_BRTHDAT that are defined within forms
                form_id: str
                data_fields: list[dict[str, any]]
                for form_id, data_fields in {f.get('form_id'):f.get('data', {}) for f in obj.get('forms', [])}.items():
                    data_field: dict[str, any]
                    for data_field in [
                        d for d in data_fields
                            if d.get('form_field_id') in self._source_field_output_types or
                                f'{form_id}.{d.get("form_field_id")}' in self._source_field_output_types
                    ]:
                        source_field_name = data_field.get('form_field_id')
                        form_id_field_id = f'{form_id}.{source_field_name}'
                        # use the full form-qualified name if specified in mapping (e.g. to avoid dupes)
                        if form_id_field_id in self._source_field_output_types:
                            source_field_name = form_id_field_id
                        source_field_output_type = self._source_field_output_types[source_field_name]

                        if source_field_output_type == 'array':
                            rec[source_field_name] = rec.get(source_field_name, [])
                            rec[source_field_name].append(data_field.get('value'))
                        elif source_field_name in rec:
                            msg = (
                                f'Duplicate source field "{source_field_name}" ({form_id_field_id}) ' +
                                f'found in file "{source_file_name}"'
                            )
                            _logger.fatal(msg)
                            raise RuntimeError(msg)
                        else:
                            rec[source_field_name] = data_field.get('value')
                recs.append(rec)

        # add row numbers for logging/auditing
        self._raw_etl_data_objects[study_id] = self._raw_etl_data_objects.get(study_id, {})
        self._raw_etl_data_objects[study_id][transformation.get('name')] = recs
        return self._raw_etl_data_objects[study_id][transformation.get('name')]

    def _load_study_data(self, study_configuration: dict[str, any]) -> dict[str, list[dict[str, any]]]:
        """ Load raw ETL data from source files specified in study config """
        transformation: dict[str, any]
        for transformation in study_configuration.get('transformations', []):
            self._load_source_data(study_configuration.get('study'), transformation)
        return self._raw_etl_data_objects[study_configuration.get('study')]

    def _get_json_schema_node_property_type(self, node_type_dot_property_name: str) -> str:
        """ Get JSON schema property type for specified output field ('node_name.field_name') """
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
            # source field to be replaced will be specified as '[field: FIELD_NAME]
            if not (
                (macro_text.startswith('"') and macro_text.endswith('"')) or
                (macro_text.startswith("'") and macro_text.endswith("'")) or
                macro_text.lower() == 'uuid' or
                macro_text.lower() == 'sum' or
                macro_text.lower() == 'sum_abs_first' or
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
        study_id: str,
        transformation_name: str,
        mapping: dict[str, any]
    ) -> list[str]:
        """ Get errors for specified transformation mapping """
        errors: list[str] = []

        source_header: list[str] = list(self._source_field_output_types.keys())
        source_field: str = mapping.get('source_field')
        if not source_field:
            errors.append(f'{transformation_name} ({study_id}): mapping source field not specified: {mapping}')
        if source_field.startswith('[') and source_field.endswith(']'):
            # strip extra spaces; csv module parses "field 1, field 2" into ["field 1", " field 2"]
            source_fields: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
            if not {s for s in source_fields if s != 'string_literal'}.issubset(set(source_header)):
                errors.append(f'{transformation_name} ({study_id}): source field not present in source data: {mapping}')
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

        if transformation.get('source_file_path') and not os.path.isdir(transformation.get('source_file_path')):
            errors.append(
                f'{transformation.get("name")} ({study_id}): invalid source file parent dir ' +
                f'"{transformation.get("source_file_path")}"'
            )

        if (
            transformation.get('name')
            and
            not (self._raw_etl_data_objects or {}).get(study_id, {}).get(transformation.get('name'))
        ):
            if transformation.get('source_file_path') and os.path.isdir(transformation.get('source_file_path')):
                self._load_source_data(study_id, transformation)
            if not (self._raw_etl_data_objects or {}).get(study_id, {}).get(transformation.get('name')):
                errors.append(f'{transformation.get("name")}: unable to load source data')

        if not errors:
            mapping: dict[str, any]
            for mapping in transformation['mappings']:
                mapping_errors: list[str] = self._get_transformation_mapping_errors(
                    study_id, transformation.get('name'), mapping
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

    def _build_reference_file_fieldset_mapping(
        self,
        transformation: dict[str, any],
        participant: dict[str, any],
        source_file_manifest: dict[str, any],
        type_group_index: int
    ) -> list[dict[str, any]]:
        """
        Build reference file JSON transformation mapping for specified
        participant, transformation, and type group index
        """
        usi: str = participant.get('participant_id')
        if not usi:
            msg = 'Participant record missing "participant_id", unable to build reference file mapping'
            _logger.critical(msg)
            _logger.critical(participant)
            raise RuntimeError(msg)

        dcf_indexd_guid: str = source_file_manifest.get('guid')

        source_file_name: str = f'{usi}.json'
        source_file_parent_path: str = transformation.get('source_file_path')
        source_file_path: str = os.path.join(source_file_parent_path, source_file_name)
        if not os.path.exists(source_file_path):
            msg = 'Source file "{source_file_name}" not found'
            _logger.critical(msg)
            raise RuntimeError(msg)

        md5sum: str = source_file_manifest.get('md5')
        if not md5sum:
            with open(source_file_path, 'rb') as fp:
                md5sum = hashlib.md5(fp.read()).hexdigest()
        file_size: int = int(source_file_manifest.get('size', os.path.getsize(source_file_path)))
        url: str = source_file_manifest.get('url', '')

        output_field_new_values: dict[str, any] = {
            'reference_file.dcf_indexd_guid': dcf_indexd_guid,
            'reference_file.file_name': source_file_name,
            'reference_file.file_type': 'json',
            'reference_file.file_category': 'input source data',
            'reference_file.file_size': file_size,
            'reference_file.md5sum': md5sum,
            'reference_file.reference_file_url': url
        }

        reference_file_fieldset: list[dict[str, any]] = []

        source_field: str = '[string_literal]'
        type_group_index_str: str = str(type_group_index)
        old_value: str = '*'
        output_field: str
        new_value: str | int
        for output_field, new_value in output_field_new_values.items():
            replacement_values: list[dict[str, any]] = [{
                'old_value': old_value,
                'new_value': new_value
            }]

            reference_file_field: dict[str, any] = {
                'output_field': output_field,
                'source_field': source_field,
                'type_group_index': type_group_index_str,
                'replacement_values': replacement_values
            }
            reference_file_fieldset.append(reference_file_field)
        return reference_file_fieldset

    def _build_reference_file_fieldset_mappings(
        self,
        transformation: dict[str, any],
        participants: list[dict[str, any]],
        source_file_manifests: list[dict[str, any]]
    ) -> dict[str, any]:
        """
        Source data consists of individual JSON files per subject (SUBJECT1.json, SUBJECT2.json, etc) and
        not all will make it into the harmonized data output file due to properties required by output schema 
        being invalid/missing/null. This method will dynamically populate the necessary reference file mapping
        for the subject (participant) records that passed validation and were loaded by the ETL. The reference
        file mappings will then be processed, populating the actual reference file entries in the harmonized
        data output file
        """
        # determine starting type group index for input source file reference mappings; should be after
        # existing reference file entries (programmatic source code, schema, transformation/mapping)
        type_group_index: int = max(
            int(m.get('type_group_index', 0)) for m in transformation.get('mappings', [])
                if m.get('output_field', '') == 'reference_file.file_name'
        )
        if type_group_index == 1:
            raise RuntimeError('Unexpected reference file type group index value: 1')

        reference_file_fieldset_mappings: list[dict[str, any]] = []
        participant: dict[str, any]
        for participant in participants:
            source_file_manifest: dict[str, any] = [
                m for m in source_file_manifests if m.get('file_name') == f'{participant.get("participant_id")}.json'
            ]
            source_file_manifest = source_file_manifest[0] if source_file_manifest else {}
            type_group_index += 1
            reference_file_fieldset_mapping: list[dict[str, any]] = self._build_reference_file_fieldset_mapping(
                transformation,
                participant,
                source_file_manifest,
                type_group_index
            )
            reference_file_fieldset_mappings.extend(reference_file_fieldset_mapping)
        return reference_file_fieldset_mappings

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

        source_field: str = mapping.get('source_field').strip(' \'"')
        source_value: str = source_record.get(source_field, None)

        replacement_entry: dict[str, str]
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
                elif macro_text.lower() in ('sum', 'sum_abs_first'):
                    # source field should contain list of source fields to be added together
                    if not (source_field.startswith('[') and source_field.endswith(']')):
                        msg = (
                            f'Invalid source field "{source_field} for "sum" macro in source file ' +
                            f'{source_record["source_file_name"]}, must be comma-delimited ' +
                            '(csv) string within square brackets, e.g. "[field1, field2]"'
                        )
                        _logger.critical(msg)
                        raise RuntimeError(msg)
                    # strip extra spaces; csv module parses "field 1, field 2" into ["field 1", " field 2"]
                    source_field_names: list[str] = [s.strip() for s in next(csv.reader([source_field.strip(' []')]))]
                    addends: list[float | int] = []
                    source_field_name: str
                    for source_field_name in source_field_names:
                        addend: str = source_record.get(source_field_name)
                        if not str(addend or '').strip():
                            # set output sum to blank/null if any addend is invalid/blank/null
                            return None

                        if not C3dcEtl.is_number(addend):
                            msg = (
                                f'Invalid source field value "{addend}" for "sum" macro in source file ' +
                                f'{source_record["source_file_name"]}, must be a number"'
                            )
                            _logger.critical(msg)
                            raise RuntimeError(msg)
                        addend = float(addend)
                        if macro_text.lower() == 'sum_abs_first' and not addends:
                            # use absolute value of first addend for 'sum_abs_first' macro
                            addend = abs(addend)
                        addends.append(addend if not addend.is_integer() else int(addend))
                    new_val = sum(addends)
                new_vals[i] = new_val

            new_value = new_vals if isinstance(new_value, (list, set, tuple)) else new_vals[0]
            source_old_value_match: bool = False
            if not isinstance(source_value, (list, set, tuple)):
                source_old_value_match = (str(source_value or '')).lower() == (str(old_value or '')).lower()
            else:
                source_old_value_match = old_value in source_value

            if (
                source_field == '[string_literal]' or
                old_value == '*' or
                (old_value == '+' and source_value) or
                source_old_value_match
            ):
                if not isinstance(source_value, (list, set, tuple)):
                    # if output target isn't multi-valued then we don't need to check for further replacements
                    output_value = new_value
                    break

                # output multi-valued, continue replacing other items
                output_value = output_value or []
                if (
                    old_value in source_value
                    and
                    (
                        (isinstance(new_value, (list, set, tuple)) and not set(new_value).issubset(set(output_value)))
                        or
                        (not isinstance(new_value, (list, set, tuple)) and new_value not in output_value)
                    )
                ):
                    if isinstance(new_value, (list, set, tuple)):
                        output_value.extend(new_value)
                    else:
                        output_value.append(new_value)

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
        type_group_index_mappings = {}
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
                key=lambda item: 0 if item[0] == '*' else int(item[0])
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

        # a single source field can be mapped to multiple target fields, for example
        # 'First Event' => survival.first_event, survival.event_free_survival_status
        # so pre-load all allowed values for each source field to determine when a
        # source record contains invalid/unmapped values and should be skipped/ignored
        allowed_values: list[str]
        source_field_allowed_values: dict[str, list[str]] = {}
        type_group_index: str
        mappings: list[dict[str, any]]
        mapping: dict[str, any]
        for type_group_index, mappings in type_group_index_mappings.items():
            for mapping in mappings:
                source_field: str = mapping.get('source_field')
                replacement_entries: list[dict[str, str]] = mapping.get('replacement_values', [])
                allowed_values = [
                    r['old_value'] for r in replacement_entries
                        if r.get('old_value') not in ('+', '*', None) and r.get('new_value')
                ]
                # empty string ("") and None are treated equally for matching/comparision purposes
                if '' in allowed_values:
                    allowed_values.append(None)
                source_field_allowed_values[source_field] = source_field_allowed_values.get(source_field, [])
                source_field_allowed_values[source_field].extend(allowed_values)

        output_source_field_map: dict[str, str] = {}

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

                source_field: str = mapping.get('source_field')
                source_value: str = source_record.get(source_field, None)

                if output_field not in output_source_field_map:
                    output_source_field_map[output_field] = source_field

                # check source value against all of this source field's mapped replacement values
                # in case there are multiple output field mappings for this source field
                allowed_values = source_field_allowed_values.get(source_field, [])
                source_value_allowed: bool = (
                    allowed_values
                    and
                    (
                        (not isinstance(source_value, (list, set, tuple)) and source_value in allowed_values)
                        or
                        (
                            isinstance(source_value, (list, set, tuple))
                            and
                            set(source_value or {}).issubset(set(allowed_values))
                        )
                    )
                )
                if allowed_values and not source_value_allowed:
                    _logger.warning(
                        (
                            '"%s" not specified as allowed value (old_value) in transformation(s) for source field ' +
                            '"%s", source record "%s"'
                        ),
                        source_value,
                        source_field,
                        source_record.get('source_file_name')
                    )
                    continue

                # check source value against mapped replacement values for this particular output field mapping
                replacement_entries: list[dict[str, str]] = mapping.get('replacement_values', [])
                allowed_values = [
                    r['old_value'] for r in replacement_entries
                        if r.get('old_value') not in ('+', '*', None) and r.get('new_value')
                ]

                # empty string ("") and None are treated equally for matching/comparision purposes
                if '' in allowed_values:
                    allowed_values.append(None)
                source_value_allowed = (
                    allowed_values
                    and
                    (
                        (not isinstance(source_value, (list, set, tuple)) and source_value in allowed_values)
                        or
                        (
                            isinstance(source_value, (list, set, tuple))
                            and
                            set(source_value or {}).issubset(set(allowed_values))
                        )
                    )
                )
                if allowed_values and not source_value_allowed:
                    _logger.info('value "%s" not allowed for source field "%s"', source_value, source_field)
                    continue

                output_value: any = self._get_mapped_output_value(mapping, source_record)
                output_record[output_field_property] = output_value if output_value is not None else source_value

            # verify that record is valid and contains all required properties
            record_valid: bool = True
            required_properties: dict[str, any] = self._get_json_schema_node_required_properties(node_type)
            required_property: str
            for required_property in required_properties:
                schema_field: str = f'{node_type}.{required_property}'
                source_field: str = output_source_field_map[schema_field]
                if output_record.get(required_property, None) in ('', None):
                    record_valid = False
                    _logger.warning(
                        'Required output field "%s" (source field "%s") has null value for source record file "%s"',
                        schema_field,
                        source_field,
                        source_record.get("source_file_name")
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
            not self._raw_etl_data_objects.get(study_id, {}).get(transformation.get('name'), {})
            or
            not petl.nrows(self._raw_etl_data_objects[study_id][transformation.get('name')])
        ):
            self._load_source_data(study_id, transformation)
            if not self._raw_etl_data_objects[study_id][transformation.get('name')]:
                raise RuntimeError(f'No data loaded to transform for study {study_id}')

        nodes: dict[str, any] = {
            'diagnoses': [],
            'participants': [],
            'reference_files': [],
            'studies': [],
            'survivals': []
        }

        # build study node and add to node collection
        study: dict[str, any] = self._build_node(transformation, C3dcEtlModelNode.STUDY)
        if len(study) != 1:
            raise RuntimeError(f'Unexpected number of study nodes built ({len(study)}), check mapping')
        study = study[0]
        study['participant.participant_id'] = []
        study['reference_file.reference_file_id'] = []

        # add diagnosis, survival, and participant records to match source data records
        rec: dict[str, any]
        for rec in self._raw_etl_data_objects[study_id][transformation.get('name')]:
            diagnoses: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.DIAGNOSIS, rec)
            if not diagnoses:
                _logger.warning(
                    '%s (%s): Unable to build diagnosis node for source record %s',
                    transformation.get('name'),
                    study_id,
                    rec['source_file_name']
                )

            survivals: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.SURVIVAL, rec)
            if not survivals:
                _logger.warning(
                    '%s (%s): Unable to build survival node for source record %s',
                    transformation.get('name'),
                    study_id,
                    rec['source_file_name']
                )

            participant: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.PARTICIPANT, rec)
            if len(participant) != 1:
                _logger.warning(
                    '%s (%s): Unexpected number of participant nodes (%d) built for sourced record %d, excluding',
                    transformation.get('name'),
                    study_id,
                    len(participant),
                    rec['source_file_name']
                )
                participant = None
                continue

            participant = participant[0]
            participant['diagnosis.diagnosis_id'] = []
            diagnosis: dict[str, any]
            for diagnosis in diagnoses:
                diagnosis['participant.participant_id'] = participant['participant_id']
                participant['diagnosis.diagnosis_id'].append(diagnosis['diagnosis_id'])

            participant['survival.survival_id'] = []
            survival: dict[str, any]
            for survival in survivals:
                survival['participant.participant_id'] = participant['participant_id']
                participant['survival.survival_id'].append(survival['survival_id'])

            participant['study.study_id'] = study['study_id']
            study['participant.participant_id'].append(participant['participant_id'])

            nodes['diagnoses'].extend(diagnoses)
            nodes['survivals'].extend(survivals)
            nodes['participants'].append(participant)

        # get reference file mappings for harmonized participants then add to base transformation mappings
        if not any(
            m for m in transformation.get('mappings', [])
                if m.get('output_field') == 'reference_file.file_category' and
                    any(r for r in m.get('replacement_values', []) if r.get('new_value') == 'input source data')
        ):
            _logger.info('Building reference file fieldset mappings for source files')
            source_file_manifests: list[dict[str, any]] = [
                m.get('manifest') for m in
                    self._raw_etl_data_objects.get(study_id, {}).get(transformation.get('name'), {})
                        if m.get('manifest')
            ]
            reference_file_fieldset_mappings: list[dict[str, any]] = self._build_reference_file_fieldset_mappings(
                transformation,
                nodes['participants'],
                source_file_manifests
            )
            transformation.get('mappings').extend(reference_file_fieldset_mappings)
            _logger.info(
                'Built and appended %d reference file mapping entries for %d participants',
                len(reference_file_fieldset_mappings),
                len(nodes['participants'])
            )
            study_config: dict[str, any] = [c for c in self._study_configurations if c.get('study') == study_id]
            study_config = study_config[0] if study_config else {}
            remote_config: dict[str, any] = C3dcEtl.get_url_content(study_config.get('transformations_url'))
            remote_transform: dict[str, any] = [
                t for t in remote_config.get('transformations') if t.get('name') == transformation.get('name')
            ]
            remote_transform = remote_transform[0] if remote_transform else {}
            remote_transform.get('mappings').extend(reference_file_fieldset_mappings)
            orig_save_path: pathlib.Path = pathlib.Path(
                os.path.basename(urlparse(study_config.get("transformations_url")).path)
            )
            save_path = f'./{orig_save_path.stem}.ref_files{orig_save_path.suffix}'
            with open(save_path, 'w', encoding='utf-8') as fp:
                json.dump(remote_config, fp, indent=4)
            _logger.info('Saved updated remote transformation mapping to %s', save_path)
        else:
            # transformation mapping already has 'input source data' reference file entries, skip
            _logger.warning(
                'Existing "input source data" reference file transformation mapping entries found, ' +
                'skipping construction of reference file fieldset mappings'
            )

        reference_files: list[dict[str, any]] = self._build_node(transformation, C3dcEtlModelNode.REFERENCE_FILE)
        reference_file: dict[str, any]
        for reference_file in reference_files:
            reference_file['study.study_id'] = study['study_id']
            study['reference_file.reference_file_id'].append(reference_file['reference_file_id'])
        nodes['reference_files'].extend(reference_files)

        nodes['studies'].append(study)

        self._json_etl_data_sets[study_id] = self._json_etl_data_sets.get(study_id) or {}
        self._json_etl_data_sets[study_id][transformation.get('name')] = nodes

        # save updated transformation

        _logger.info(
            '1 study, %d diagnosis, %d survival, %d participant, %d reference file records built for transformation %s',
            len(nodes['diagnoses']),
            len(nodes['survivals']),
            len(nodes['participants']),
            len(nodes['reference_files']),
            transformation.get('name')
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
