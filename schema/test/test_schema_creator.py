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
_config: any = dotenv.dotenv_values('.env_test')


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


@pytest.mark.skip('test_download_source_files')
def test_download_source_files() -> None:
    """ test_download_source_files """
    _logger.info(test_download_source_files.__name__)
    schema_creator: SchemaCreator = SchemaCreator(_config)

    nodes_source_url: str = _config.get('NODES_SOURCE_URL')
    nodes_source_file: str = f'./{os.path.basename(urlparse(nodes_source_url).path)}'
    props_source_url: str = _config.get('PROPS_SOURCE_URL')
    props_source_file: str = f'./{os.path.basename(urlparse(props_source_url).path)}'

    if os.path.exists(nodes_source_file):
        os.rename(nodes_source_file, f'{nodes_source_file}.test.tmp')
    if os.path.exists(props_source_file):
        os.rename(props_source_file, f'{props_source_file}.test.tmp')

    assert not os.path.exists(nodes_source_file) and not os.path.exists(props_source_file)
    _logger.info('downloading source files')
    schema_creator.download_source_files()
    _logger.info('verifying source files downloaded')
    assert os.path.exists(nodes_source_file) and os.path.exists(props_source_file)

    _logger.info('removing downloaded source files')
    if os.path.exists(f'{nodes_source_file}.test.tmp'):
        os.rename(f'{nodes_source_file}.test.tmp', nodes_source_file)
    if os.path.exists(f'{props_source_file}.test.tmp'):
        os.rename(f'{props_source_file}.test.tmp', props_source_file)


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
    if not str(schema_file_path if schema_file_path is not None else '').lower().startswith('s3://'):
        if os.path.exists(schema_file_path):
            os.remove(schema_file_path)

        _logger.info('building schema and saving to local file')
        schema_creator.save_schema_to_file()
        assert os.path.exists(schema_file_path)
    else:
        import sys # pylint: disable=import-outside-toplevel
        sys.path.append('../aws_s3')
        from aws_s3 import AwsS3 # pylint: disable=import-outside-toplevel
        aws_s3: AwsS3 = AwsS3()
        bucket_name: str
        object_path: str
        bucket_name, object_path = AwsS3.parse_s3_uri(schema_file_path)
        if aws_s3.file_exists(bucket_name, object_path):
            aws_s3.delete_file(bucket_name, object_path)
        _logger.info('building schema and uploading to S3')
        schema_creator.save_schema_to_file()
        assert aws_s3.file_exists(bucket_name, object_path)
