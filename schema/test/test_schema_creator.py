""" Test SchemaCreator """
import json
import logging
import logging.config
import os
import pathlib
from urllib.parse import urlparse

import dotenv
import pytest

from schema_creator import SchemaCreator


pytest.skip("test_schema_creator.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_logger.info(__name__)
_config: any = dotenv.dotenv_values('.env')


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


@pytest.mark.skip('test_url_to_path')
def test_url_to_path() -> None:
    """ test url_to_path """
    _logger.info(test_url_to_path.__name__)
    expected_path: str = '/path/to/file.extension'
    url: str = 'https://example.com/path/to/file.extension'
    path: str = SchemaCreator.url_to_path(url)
    _logger.info('Path: %s', path)
    assert path == expected_path

    url = 's3://bucket-name/path/to/file.extension'
    path = SchemaCreator.url_to_path(url)
    _logger.info('Path: %s', path)
    assert path == expected_path


@pytest.mark.skip('test_get_url_content')
def test_get_url_content() -> None:
    """ test get_url_content """
    _logger.info(test_get_url_content.__name__)
    url: str = 'https://raw.githubusercontent.com/CBIIT/c3dc-model/main/model-desc/c3dc-model.yml'
    content: any = SchemaCreator.get_url_content(url)
    #logger.info(content)
    assert content is not None and content.decode('utf-8').startswith('Handle')


@pytest.mark.skip('test_download_source_files')
def test_download_source_files() -> None:
    """ test_download_source_files """
    _logger.info(test_download_source_files.__name__)
    schema_creator: SchemaCreator = SchemaCreator(_config)

    nodes_source_url: str = _config.get('NODES_SOURCE_URL')
    nodes_source_file: str = f'./{os.path.basename(urlparse(nodes_source_url).path)}'
    props_source_url: str = _config.get('PROPS_SOURCE_URL')
    props_source_file: str = f'./{os.path.basename(urlparse(props_source_url).path)}'
    _logger.info('downloading source files')
    _logger.info(nodes_source_file)
    _logger.info(props_source_file)

    if os.path.exists(nodes_source_file):
        os.remove(nodes_source_file)
    if os.path.exists(props_source_file):
        os.remove(props_source_file)

    _logger.info('verifying source files downloaded')
    assert not os.path.exists(nodes_source_file) and not os.path.exists(props_source_file)
    schema_creator.download_source_files()
    assert os.path.exists(nodes_source_file) and os.path.exists(props_source_file)

    _logger.info('removing downloaded source files')
    os.remove(nodes_source_file)
    os.remove(props_source_file)


@pytest.mark.skip('test_convert_source_files_to_json')
def test_convert_source_files_to_json() -> None:
    """ test_convert_source_files_to_json """
    _logger.info(test_convert_source_files_to_json)
    schema_creator: SchemaCreator = SchemaCreator(_config)

    nodes_source_url: str = _config.get('NODES_SOURCE_URL')
    nodes_source_file: str = f'./{os.path.basename(urlparse(nodes_source_url).path)}'
    props_source_url: str = _config.get('PROPS_SOURCE_URL')
    props_source_file: str = f'./{os.path.basename(urlparse(props_source_url).path)}'

    source_file_path_yaml: str
    for source_file_path_yaml in (nodes_source_file, props_source_file):
        source_file_path_json: str = str(pathlib.Path(source_file_path_yaml).with_suffix('.json'))
        if os.path.exists(source_file_path_json):
            os.remove(source_file_path_json)
        assert not os.path.exists(source_file_path_json)

    _logger.info('converting source files to json')
    schema_creator.convert_source_files_to_json()

    for source_file_path_yaml in (nodes_source_file, props_source_file):
        source_file_path_json: str = str(pathlib.Path(source_file_path_yaml).with_suffix('.json'))
        assert os.path.exists(source_file_path_json)
        with open(source_file_path_json, mode='r', encoding='utf-8') as json_fd:
            _logger.info('verifying converted file can be parsed as json: %s', source_file_path_json)
            source_file: any = json.load(json_fd)
            assert source_file
            _logger.info(source_file.keys())


@pytest.mark.skip('test_build_schema')
def test_build_schema() -> None:
    """ test_build_schema """
    _logger.info(test_build_schema)
    schema_creator: SchemaCreator = SchemaCreator(_config)

    _logger.info('building schema')
    schema_creator.build_schema()

    schema = schema_creator.schema
    _logger.info((schema or {}).keys())
    assert schema


@pytest.mark.skip('test_save_schema_to_file')
def test_save_schema_to_file() -> None:
    """ test_save_schema_to_file """
    _logger.info(test_convert_source_files_to_json)
    schema_creator: SchemaCreator = SchemaCreator(_config)

    schema_file_path: str = _config.get('SCHEMA_FILE_PATH', './schema.json')
    if os.path.exists(schema_file_path):
        os.remove(schema_file_path)

    _logger.info('building schema and saving to file')
    schema_creator.save_schema_to_file()
    assert os.path.exists(schema_file_path)
