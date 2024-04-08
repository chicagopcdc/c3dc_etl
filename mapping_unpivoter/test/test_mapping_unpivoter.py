""" Test MappingUnpivoter """
import json
import logging
import logging.config
import os

import dotenv
import pytest

from mapping_unpivoter import MappingUnpivoter


pytest.skip("test_mapping_unpivoter.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_logger.info(__name__)
_config: any = dotenv.dotenv_values('.env_test')


def setup_module() -> None:
    """ module-wide test setup """
    _logger.info(__name__)


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


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
        is_num: bool = MappingUnpivoter.is_number(value)
        logging.info('is_number: %s => %s', value, is_num)
        assert is_num == expected_status


@pytest.mark.skip('test_url_to_path')
def test_url_to_path() -> None:
    """ test url_to_path """
    _logger.info(test_url_to_path.__name__)
    expected_path: str = '/path/to/file.extension'
    url: str = 'https://example.com/path/to/file.extension'
    path: str = MappingUnpivoter.url_to_path(url)
    #logger.info('Path: %s', path)
    assert path == expected_path

    url = 's3://bucket-name/path/to/file.extension'
    path = MappingUnpivoter.url_to_path(url)
    #logger.info('Path: %s', path)
    assert path == expected_path


@pytest.mark.skip('test_get_url_content')
def test_get_url_content() -> None:
    """ test get_url_content """
    _logger.info(test_get_url_content.__name__)
    url: str = 'https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json'
    content: any = MappingUnpivoter.get_url_content(url)
    assert content is not None and content.decode('utf-8').startswith('{')


@pytest.mark.skip('test_load_transformation_config_output_file')
def test_load_transformation_config_output_file() -> None:
    """ test_load_transformation_config_output_file """
    mapping_unpivoter: MappingUnpivoter
    with MappingUnpivoter(_config) as mapping_unpivoter:
        assert not mapping_unpivoter.transformation_config.get('transformations')

        _logger.info('loading transformation config output file %s', _config.get('OUTPUT_FILE'))
        mapping_unpivoter.load_transformation_config_output_file()
        assert mapping_unpivoter.transformation_config.get('transformations')


@pytest.mark.skip('test_save_transformation_config_output_file')
def test_save_transformation_config_output_file() -> None:
    """ test_save_transformation_config_output_file """
    mapping_unpivoter: MappingUnpivoter
    with MappingUnpivoter(_config) as mapping_unpivoter:
        output_file_path: str = _config.get('OUTPUT_FILE')

        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        assert not os.path.exists(output_file_path)

        _logger.info('saving transformation config output file')
        mapping_unpivoter.save_transformation_config_output_file()
        assert os.path.exists(output_file_path)

        with open(output_file_path, mode='r', encoding='utf-8') as json_fp:
            output_file_json: any = json.load(json_fp)
            assert output_file_json == json.loads('{"version": "20240401.1", "transformations": []}')


@pytest.mark.skip('test_get_transformation_mappings_file_records')
def test_get_transformation_mappings_file_records() -> None:
    """ test_get_transformation_mappings_file_records """
    mapping_unpivoter: MappingUnpivoter
    with MappingUnpivoter(_config) as mapping_unpivoter:
        transformation_mappings_files: list[dict[str, any]] = json.loads(
            _config.get('TRANSFORMATION_MAPPINGS_FILES', '[]')
        )

        transformation_mappings_file: dict[str, any]
        for transformation_mappings_file in transformation_mappings_files:
            _logger.info(
                'getting records for transformation mappings file %s', transformation_mappings_file.get('mappings_file')
            )
            records: list[dict[str, any]] = mapping_unpivoter.get_transformation_mappings_file_records(
                transformation_mappings_file
            )
            assert records


@pytest.mark.skip('test_unpivot_transformation_mappings')
def test_unpivot_transformation_mappings() -> None:
    """ test_unpivot_transformation_mappings """
    mapping_unpivoter: MappingUnpivoter
    with MappingUnpivoter(_config) as mapping_unpivoter:
        output_file_path: str = _config.get('OUTPUT_FILE')

        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        assert not os.path.exists(output_file_path)

        _logger.info('unpivoting transformation mappings')
        mapping_unpivoter.unpivot_transformation_mappings()
        assert os.path.exists(output_file_path)

        with open(output_file_path, mode='r', encoding='utf-8') as json_fp:
            output_file_json: any = json.load(json_fp)
            assert output_file_json != json.loads('{"version": "20240401.1", "transformations": []}')

            assert (
                output_file_json.get('transformations') and
                output_file_json.get('transformations')[0].get('mappings')
            )


@pytest.mark.skip('test_update_reference_file_mappings')
def test_update_reference_file_mappings() -> None:
    """ test_update_reference_file_mappings """
    mapping_unpivoter: MappingUnpivoter
    with MappingUnpivoter(_config) as mapping_unpivoter:
        output_file_path: str = _config.get('OUTPUT_FILE')

        _logger.info('unpivoting transformation mappings')
        mapping_unpivoter.unpivot_transformation_mappings()

        _logger.info('loading unpivoted transformation mappings from output file %s (before)', output_file_path)
        xform_json_before: any
        with open(output_file_path, mode='r', encoding='utf-8') as json_fp:
            xform_json_before = json.load(json_fp)

        mapping_unpivoter.update_reference_file_mappings()
        mapping_unpivoter.save_transformation_config_output_file()

        _logger.info('loading unpivoted transformation mappings from output file %s (after)', output_file_path)
        xform_json_after: any
        with open(output_file_path, mode='r', encoding='utf-8') as json_fp:
            xform_json_after = json.load(json_fp)

        assert json.dumps(xform_json_before) != json.dumps(xform_json_after)
