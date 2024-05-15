""" Test MappingUnpivoter """
import json
import logging
import logging.config
import os
import pathlib
import sys

import dotenv
import pytest

from mapping_unpivoter import MappingUnpivoter

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
from c3dc_file_manager import C3dcFileManager # pylint: disable=wrong-import-position,wrong-import-order # type: ignore


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

        c3dc_file_manager: C3dcFileManager = C3dcFileManager()
        if c3dc_file_manager.file_exists(output_file_path):
            c3dc_file_manager.delete_file(output_file_path)
        assert not c3dc_file_manager.file_exists(output_file_path)

        _logger.info('saving transformation config output file')
        mapping_unpivoter.save_transformation_config_output_file()
        assert c3dc_file_manager.file_exists(output_file_path)

        output_file_json: any = json.loads(c3dc_file_manager.read_file(output_file_path))
        assert output_file_json == json.loads('{"version": "20240401.1", "transformations": []}')

        # cleanup
        c3dc_file_manager.delete_file(output_file_path)


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
            _logger.info(records[0])


@pytest.mark.skip('test_unpivot_transformation_mappings')
def test_unpivot_transformation_mappings() -> None:
    """ test_unpivot_transformation_mappings """
    mapping_unpivoter: MappingUnpivoter
    with MappingUnpivoter(_config) as mapping_unpivoter:
        output_file_path: str = _config.get('OUTPUT_FILE')

        c3dc_file_manager: C3dcFileManager = C3dcFileManager()
        if c3dc_file_manager.file_exists(output_file_path):
            c3dc_file_manager.delete_file(output_file_path)
        assert not c3dc_file_manager.file_exists(output_file_path)

        _logger.info('unpivoting transformation mappings')
        mapping_unpivoter.unpivot_transformation_mappings()
        assert c3dc_file_manager.file_exists(output_file_path)

        output_file_json: any = json.loads(c3dc_file_manager.read_file(output_file_path).decode('utf-8'))
        assert output_file_json != json.loads('{"version": "20240401.1", "transformations": []}')

        assert (
            output_file_json.get('transformations') and
            output_file_json.get('transformations')[0].get('mappings')
        )

        # cleanup
        c3dc_file_manager.delete_file(output_file_path)


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

        c3dc_file_manager: C3dcFileManager = C3dcFileManager()
        c3dc_file_manager.file_exists(output_file_path)
        xform_json_before = json.loads(c3dc_file_manager.read_file(output_file_path))

        mapping_unpivoter.update_reference_file_mappings()
        mapping_unpivoter.save_transformation_config_output_file()

        _logger.info('loading unpivoted transformation mappings from output file %s (after)', output_file_path)
        xform_json_after: any
        xform_json_after = json.loads(c3dc_file_manager.read_file(output_file_path))

        assert json.dumps(xform_json_before) != json.dumps(xform_json_after)

        # cleanup
        c3dc_file_manager.delete_file(output_file_path)
