""" C3DC Schema Creator """
from enum import Enum
import logging
import logging.config
import os
import pathlib
from urllib.parse import urlparse, urljoin, ParseResult
from urllib.request import url2pathname

import dotenv
import requests
from yaml_json_converter import YamlJsonConverter


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
            "filename": "schema_creator.log",
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


def is_numeric(value: any) -> bool:
    """ indicate whether specified value is int or float """
    try:
        float(value)
        return True
    except ValueError:
        return False


class SchemaRelMul(str, Enum):
    """
    Enum class for schema relationship multiplicity: one_to_one | many_to_one | many_to_many | one_to_many 
    """
    ONE_TO_ONE = 'one_to_one'
    MANY_TO_ONE = 'many_to_one'
    MANY_TO_MANY = 'many_to_many'
    ONE_TO_MANY= 'one_to_many'

    def __str__(self):
        return self.value


class SchemaCreator:
    """ Build C3DC json schema from yaml file source(s) """
    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._nodes_source_url: str = config.get('NODES_SOURCE_URL')
        self._nodes_source_file: str = f'./{os.path.basename(urlparse(self._nodes_source_url).path)}'
        self._props_source_url: str = config.get('PROPS_SOURCE_URL')
        self._props_source_file: str = f'./{os.path.basename(urlparse(self._props_source_url).path)}'
        self._nodes_source_data: dict[str, any] = {}
        self._props_source_data: dict[str, any] = {}
        self._schema_nodes: dict[str, any] = {}
        self._schema_props: dict[str, any] = {}
        self._node_props_required: dict[str, str] = {}
        self._schema_file_path: str = self._config.get('SCHEMA_FILE_PATH', './schema.json')
        self._schema: dict[str, any] = {}

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
            with open(SchemaCreator.url_to_path(url), 'rb') as local_file:
                url_content = local_file.read()
        else:
            with requests.get(url, timeout=30) as response:
                response.raise_for_status()
                url_content = response.content

        return url_content

    def download_source_files(self) -> None:
        """ Download source files from configured URLs """
        source_files: dict[str, str] = {
            self._nodes_source_url: self._nodes_source_file,
            self._props_source_url: self._props_source_file
        }
        download_url: str
        download_path: str
        for download_url, download_path in source_files.items():
            _logger.debug('Downloading %s to file %s', download_url, download_path)

            if not (
                os.path.exists(download_path) and
                download_url.startswith('file://') and
                os.path.samefile(download_path, SchemaCreator.url_to_path(download_url))
            ):
                url_content: str | bytes | bytearray = SchemaCreator.get_url_content(download_url)
                with open(download_path, 'wb') as file:
                    file.write(url_content)

    @property
    def schema(self) -> dict[str, any]:
        """
        Get internal schema object, building if needed
        """
        return self._schema if self._schema else self.build_schema()

    def convert_source_files_to_json(self) -> None:
        """ Convert downloaded source files from yaml to json """
        source_file_path_yaml: str
        source_file_path_json: str
        _logger.debug('Converting source files to json')
        if not os.path.exists(self._nodes_source_file) or not os.path.exists(self._props_source_file):
            _logger.debug('Source file %s not found, downloading')
            self.download_source_files()

        for source_file_path_yaml in (self._nodes_source_file, self._props_source_file):
            source_file_path_json = str(pathlib.Path(source_file_path_yaml).with_suffix('.json'))
            _logger.debug('Converting source file %s and saving as %s', source_file_path_yaml, source_file_path_json)
            YamlJsonConverter.convert(source_file_path_yaml, source_file_path_json)

    def save_schema_to_file(self) -> None:
        """ Save schema to json file """
        # transform/combine json source files to final schema file
        if not self._schema:
            self.build_schema()
        _logger.info('Saving schema to %s', self._schema_file_path)
        YamlJsonConverter.save_json(self._schema, self._schema_file_path)

    def build_schema(self) -> dict[str, any]:
        """ Build and return schema from yaml source files, downloading if needed """
        _logger.info('Building schema')
        # download yaml source files
        self.download_source_files()

        # load yaml source files
        _logger.debug('Loading yaml file %s', self._nodes_source_file)
        self._nodes_source_data = YamlJsonConverter.load_yaml(self._nodes_source_file)
        _logger.debug('Loading yaml file %s', self._props_source_file)
        self._props_source_data = YamlJsonConverter.load_yaml(self._props_source_file)

        # build internal json source representations of properties, nodes, and relationships
        self._build_and_populate_property_schemas()
        self._build_and_populate_node_schemas()
        self._build_and_populate_node_relationships()

        # build internal schema representation starting from root element
        self._schema.clear()
        schema: dict[str, any] = self._build_schema_root()
        self._schema.update(schema)
        return self._schema

    def _build_schema_root(self) -> dict[str, any]:
        """ Build and return schema root json """
        schema: dict[str, any] = {}
        schema['$id'] = urljoin(self._config['SCHEMA_ROOT_URL'], self._config['SCHEMA_ROOT_ID'])
        schema['$schema'] = self._config['META_SCHEMA_URL']
        schema['description'] = self._config['SCHEMA_ROOT_DESCRIPTION']
        schema['$comment'] = self._config['SCHEMA_ROOT_COMMENT']
        schema['$ref'] = urljoin(self._config['SCHEMA_ROOT_URL'], self._config['SCHEMA_ROOT_NODE'])
        schema['$defs'] = self._build_schema_root_defs()
        return schema

    def _build_schema_root_defs(self) -> dict[str, any]:
        """ Build and return schema root $defs json property """
        schema: dict[str, any] = {}
        node_name: str
        node_obj: dict[str, any]
        for node_name, node_obj in self._get_node_schemas_in_dependency_order().items():
            schema[node_name] = node_obj
        return schema

    def _get_node_schemas_in_dependency_order(self) -> dict[str, any]:
        """
        Get json node schemas in (increasing) dependency order such that nodes
        without dependencies are defined before others that reference them.
        Doesn't appear to be technically necessary, at least for most validation
        tools found online. Leaving for reference and in case of future need.
        """
        nodes_to_process: list[str] = sorted(self._schema_nodes.keys())
        ordered_nodes: dict[str, any] = {}
        ordered_node_ids: set[str] = {}
        while nodes_to_process:
            node_name: str = nodes_to_process.pop(0)
            node_deps: list[str] = self._get_node_dependencies(self._schema_nodes[node_name])
            # add to ordered node collection if no dependencies or dependencies have
            # been added otherwise move to back of processing queue to inspect later
            if not node_deps or set(node_deps).issubset(ordered_node_ids):
                ordered_nodes[node_name] = self._schema_nodes[node_name]
                ordered_node_ids: set[str] = {v['$id'] for v in ordered_nodes.values()}
            else:
                nodes_to_process.append(node_name)
        return ordered_nodes

    def _get_node_dependencies(self, node_obj: dict[str, any]) -> list[str]:
        """ Get the specified node's dependencies as a list of references ($id values in $ref properties) """
        node_deps: list[str] = []
        for key, value in node_obj.items():
            if key == '$ref':
                node_deps.append(value)
            elif isinstance(value, dict):
                node_deps.extend(self._get_node_dependencies(value))
        return node_deps

    def _build_and_populate_node_schemas(self) -> dict[str, any]:
        """ Build and return json schema for source nodes """
        _logger.debug('Building and populating node schemas')
        self._schema_nodes.clear()

        # build individual node schemas
        for node_name, node_obj in self._nodes_source_data['Nodes'].items():
            self._schema_nodes[node_name] = self._build_node_schema(node_name, node_obj)

        # build root node container schema
        if self._config['SCHEMA_ROOT_NODE'] in self._schema_nodes:
            log_msg: str = (
                f'Root node container name {self._config["SCHEMA_ROOT_NODE"]} already defined in YAML source schema'
            )
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)
        self._schema_nodes[self._config['SCHEMA_ROOT_NODE']] = self._build_root_node_schema()
        return self._schema_nodes

    def _build_root_node_schema(self) -> dict[str, any]:
        """ Build and return json schema for root container node that will contain arrays of all other node types """
        if not self._schema_nodes:
            log_msg: str = 'No node schemas found; node schemas must be built before root node schema'
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)

        schema: dict[str, any] = {}
        schema['$id'] = urljoin(self._config['SCHEMA_ROOT_URL'], self._config['SCHEMA_ROOT_NODE'])
        schema['$schema'] = self._config['META_SCHEMA_URL']
        schema['title'] = self._config['SCHEMA_ROOT_NODE'].capitalize()
        schema['description'] = 'Top-level node container'
        schema['properties'] = {}
        node_name: str
        node_schema: dict[str, any]
        for node_name, node_schema in dict(sorted(self._schema_nodes.items())).items():
            node_name_pluralized: str = SchemaCreator.pluralize_node_name(node_name)
            ref_schema: dict[str, any] = {
                'type': 'array',
                'items': {
                    '$ref': node_schema['$id']
                }
            }
            schema['properties'][node_name_pluralized] = ref_schema
        return schema

    def _build_node_schema(self, name: str, obj: dict[str, any]) -> dict[str, any]:
        """ Build and return json schema for specified source node """
        schema: dict[str, any] = {}
        schema['$id'] = urljoin(self._config['SCHEMA_ROOT_URL'], name)
        schema['$schema'] = self._config['META_SCHEMA_URL']
        schema['title'] = name.capitalize()
        schema['description'] = obj.get('Description', 'Not specified in source yaml')
        schema['type'] = 'object'
        schema['additionalProperties'] = False

        node_required_props: list[str] = self._get_node_required_properties(name)
        if node_required_props:
            schema['required'] = self._node_props_required[name]

        errors: list[str] = []
        schema['properties'] = {}
        prop: str
        for prop in obj['Props']:
            if prop not in self._schema_props:
                errors.append(f'Error building node "{name}": property "{prop}" not found in props schema file')
                continue
            schema['properties'][prop] = self._schema_props[prop]
        if errors:
            error: str
            for error in errors:
                _logger.critical(error)
            raise RuntimeError(f'One or more properties for node "{name}" not found in properties schema')

        return schema

    def _build_and_populate_node_relationships(self) -> None:
        """ Build relationships (as properties) for all nodes """
        _logger.debug('Building and populating node relationships')
        rel_name: str
        rel_obj: dict[str, any]
        for rel_name, rel_obj in self._nodes_source_data['Relationships'].items():
            src_mul: str = rel_obj['Mul'].split('_to_')[0]
            dst_mul: str = rel_obj['Mul'].split('_to_')[-1]
            src_dst_endpt: dict[str, any]
            for src_dst_endpt in rel_obj['Ends']:
                node_name: str
                for node_name in (src_dst_endpt['Src'], src_dst_endpt['Dst']):
                    if node_name not in self._schema_nodes:
                        log_msg: str = (
                            f'Error building relationship {rel_name}: node {node_name} not found in schema list'
                        )
                        _logger.critical(log_msg)
                        raise RuntimeError(log_msg)
                local_node_name: str
                remote_node_name: str
                remote_node_mul: str
                for local_node_name, remote_node_name, remote_node_mul in [
                    (src_dst_endpt['Src'], src_dst_endpt['Dst'], dst_mul.lower()),
                    (src_dst_endpt['Dst'], src_dst_endpt['Src'], src_mul.lower())
                ]:
                    local_rel_prop_name: str = f'{remote_node_name}.{remote_node_name}_id'
                    description: str = (
                        f'{rel_name}: {src_mul}_to_{dst_mul}, ' +
                        f'[src] {src_dst_endpt["Src"]} => [dst] {src_dst_endpt["Dst"]}'
                    )
                    local_rel_prop_obj: dict[str, any] = self._build_node_relationship(remote_node_mul, description)
                    self._schema_nodes[local_node_name]['properties'][local_rel_prop_name] = local_rel_prop_obj

    def _build_node_relationship(
        self,
        remote_node_mul: str,
        description: str = 'Not specified in source yaml'
    ) -> dict[str, any]:
        """ Build and return json schema for specified relationship """
        rel_prop_obj: dict[str, any] = {}
        if remote_node_mul == 'one':
            rel_prop_obj = {'type': 'string', 'description': description}
        elif remote_node_mul == 'many':
            rel_prop_obj = {'type': 'array', 'description': description, 'items': {'type': 'string'}}
        else:
            log_msg: str = f'Unsupported relationship multiplicity: {remote_node_mul}'
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)
        return rel_prop_obj

    def _get_node_required_properties(self, name: str) -> None:
        """ Get list of required properties for specified node """
        if name not in self._node_props_required:
            all_required_props: dict[str, str] = {
                k:v for k,v in self._props_source_data['PropDefinitions'].items() if v.get('Req', False)
            }
            self._node_props_required[name] = [
                p for p in self._nodes_source_data['Nodes'][name]['Props'] if p in all_required_props
            ]
        return self._node_props_required[name]

    def _build_and_populate_property_schemas(self) -> dict[str, any]:
        """ Build and return json schema for source properties """
        _logger.debug('Building and populating property schemas')
        self._schema_props.clear()
        for prop_name, prop_obj in self._props_source_data['PropDefinitions'].items():
            self._schema_props[prop_name] = self._build_property_schema(prop_name.lower(), prop_obj)
        return self._schema_props

    def _build_property_schema(self, name: str, obj: dict[str, any]) -> dict[str, any]:
        """ Build and return json schema for specified source property """
        schema: dict[str, any] = {}
        schema['type'] = self._get_property_type(name, obj)
        schema['description'] = obj.get('Desc', 'Not specified in source yaml')
        cdes: list[str] = []
        terms: list[dict[str, str]] = obj.get('Term', [])
        term: dict[str, str]
        for term in terms:
            term_origin: str = term.get('Origin', '').strip('\'"')
            term_code: str = term.get('Code', '').strip('\'"')
            if term_origin and term_code:
                cdes.append(f'{term_origin}:{term_code}')
        schema['cde'] = cdes
        if schema['type'] == 'integer':
            if name == 'file_size':
                schema['mininum'] = 0

            if name.startswith('age_at') or '_age_at_' in name:
                schema['maximum'] = 54750 # 365 * 150

        allowed_values: list[str] = self._get_property_allowed_values(name, obj)
        if allowed_values:
            if schema['type'] == 'array':
                schema['uniqueItems'] = True
                schema['items'] = {'type': 'string', 'enum': allowed_values}
            else:
                schema['enum'] = allowed_values
        return schema

    def _get_property_type(self, name: str, obj: dict[str, any]) -> str:
        """ Get json schema type for specified property """
        if 'Type' not in obj and 'Enum' in obj:
            return 'string'

        log_msg: str = None

        type_value: any = obj.get('Type', None)
        if isinstance(type_value, str):
            return type_value.lower()

        if isinstance(type_value, dict):
            value_type_name: str = type_value.get('value_type', '')
            if not value_type_name:
                log_msg = (
                    f'YAML property {name} sub-property Type does not have "value_type" ' +
                    'attribute defined, unable to determine JSON schema type'
                )
            elif value_type_name == 'list' and 'Enum' not in type_value:
                log_msg = (
                    f'YAML property {name} sub-property Type has "value_type" set to "list" ' +
                    'but does not have "Enum" defined, unable to determine JSON schema type'
                )

            if log_msg:
                _logger.critical(log_msg)
                _logger.critical(obj)
                raise RuntimeError(log_msg)
            return 'array' if value_type_name == 'list' else value_type_name

        log_msg = f'YAML property {name} does not have Type or Enum defined, unable to determine JSON schema type'
        _logger.critical(log_msg)
        raise RuntimeError(log_msg)

    def _get_property_allowed_values(self, name: str, obj: dict[str, any]) -> list[str]:
        """ Get list of allowed values for specified property """
        if 'Enum' in obj:
            return list(obj['Enum'])

        if 'Type' in obj and isinstance(obj['Type'], dict):
            if 'Enum' in obj['Type']:
                return obj['Type']['Enum']
            log_msg: str = (
                f'YAML property {name} is dict but does not have "value_type" set ' +
                'to "list" and "Enum" property containing allowed values'
            )
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)

        return []

    @staticmethod
    def pluralize_node_name(name: str) -> str:
        """ Return pluralized form of specified name """
        if name[-1] == 'y':
            # study => studies
            return f'{name[:-1]}ies'
        if name[-3:] == 'sis':
            # diagnosis => diagnoses
            return f'{name[:-3]}ses'
        return f'{name}s'


def main() -> None:
    """ Script entry point """
    config_file: str = '.env'
    if not os.path.exists(config_file):
        raise FileNotFoundError(f'Config file "{config_file}" not found')
    config: dict[str, str] = dotenv.dotenv_values(config_file)
    schema_creator: SchemaCreator = SchemaCreator(config)
    schema_creator.save_schema_to_file()

if __name__ == '__main__':
    main()