""" Test C3dcEtl """
import json
import logging
import logging.config
import os

import dotenv
import pytest

from c3dc_etl import C3dcEtl


pytest.skip("test_c3dc_etl.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_config: any = dotenv.dotenv_values('.env_test')


def setup_module() -> None:
    """ setup/teardown """
    _logger.info('%s: mci', __name__)


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
    path: str = C3dcEtl.url_to_path(url)
    #logger.info('Path: %s', path)
    assert path == expected_path

    url = 's3://bucket-name/path/to/file.extension'
    path = C3dcEtl.url_to_path(url)
    #logger.info('Path: %s', path)
    assert path == expected_path


@pytest.mark.skip('test_get_url_content')
def test_get_url_content() -> None:
    """ test get_url_content """
    _logger.info(test_get_url_content.__name__)
    url: str = 'https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json'
    content: any = C3dcEtl.get_url_content(url)
    assert content is not None and content.decode('utf-8').startswith('{')


@pytest.mark.skip('test_is_number')
def test_is_number() -> None:
    """ test is_number """
    _logger.info(test_is_number.__name__)
    values: dict[any, bool] = {
        '123': True,
        'x123': False,
        3: True,
        -7: True,
        3.14: True,
        0xDEADBEEF: True,
        '': False,
        None: False
    }
    value: any
    expected_status: bool
    for value, expected_status in values.items():
        is_num: bool = C3dcEtl.is_number(value)
        logging.info('is_number: %s => %s', value, is_num)
        assert is_num == expected_status


@pytest.mark.skip('test_is_allowed_value')
def test_is_allowed_value() -> None:
    """ test is_allowed_value """
    _logger.info(test_is_allowed_value.__name__)
    allowed_values: set[any] = {0, '1', '', None, 'test'}
    values: dict[any, bool] = {
        0: True,
        '0': False,
        '1': True,
        '2': False,
        '': True,
        'False': False,
        None: True,
        'test': True
    }
    value: any
    expected_status: bool
    for value, expected_status in values.items():
        is_num: bool = C3dcEtl.is_allowed_value(value, allowed_values)
        logging.info('is_allowed_value: %s => %s', value, is_num)
        assert is_num == expected_status


@pytest.mark.skip('test_load_transformations')
def test_load_transformations() -> None:
    """ test load_transformations """
    _logger.info(test_load_transformations.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    study_configurations_before: list[dict[str, any]] = json.loads(_config.get('STUDY_CONFIGURATIONS', '[]'))
    study_configurations_before = [sc for sc in study_configurations_before if sc.get('active', True)]
    assert study_configurations_before and isinstance(study_configurations_before, list)

    _logger.info('Loading remote study configurations and merging with local')
    study_configurations_after: list[dict[str, any]] = c3dc_etl.load_transformations()
    assert study_configurations_after and isinstance(study_configurations_after, list)
    assert study_configurations_before != study_configurations_after
    assert len(json.dumps(study_configurations_before)) < len(json.dumps(study_configurations_after))


@pytest.mark.skip('test_load_json_schema')
def test_load_json_schema() -> None:
    """ test load_json_schema """
    _logger.info(test_load_json_schema.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    _logger.info('Loading JSON schema')
    json_schema: dict[str, any] = c3dc_etl.load_json_schema()
    assert json_schema
    _logger.info(json_schema.keys())


@pytest.mark.skip('test_validate_json_etl_data')
def test_validate_json_etl_data() -> None:
    """ test validate_json_etl_data """
    _logger.info(test_validate_json_etl_data.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    _logger.info('Createing JSON ETL files')
    c3dc_etl.create_json_etl_files()

    _logger.info('Validating JSON ETL files')
    assert c3dc_etl.validate_json_etl_data()


@pytest.mark.skip('test_test_create_json_etl_files')
def test_create_json_etl_files() -> None:
    """ test create_json_etl_files """
    _logger.info(test_create_json_etl_files.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    _logger.info('Loading study configurations')
    study_configurations: list[dict[str, any]] = c3dc_etl.load_transformations()

    study_configuration: dict[str, any]
    for study_configuration in study_configurations:
        output_file_path: str = study_configuration.get('transformations', [])[0].get('output_file_path')
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        assert not os.path.exists(output_file_path)

    _logger.info('Creating JSON ETL files')
    c3dc_etl.create_json_etl_files()

    for study_configuration in study_configurations:
        transformation: dict[str, any]
        for transformation in study_configuration.get('transformations', []):
            _logger.info('Verifying transformation %s', transformation.get('name'))
            output_file_path: str = transformation.get('output_file_path')
            assert os.path.exists(output_file_path)
            with open(output_file_path, mode='r', encoding='utf-8') as fp:
                harmonized_data: dict[str, any] = json.load(fp)
                _logger.info(harmonized_data.keys())
