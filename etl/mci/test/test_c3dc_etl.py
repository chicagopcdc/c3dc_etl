""" Test C3dcEtl """
import json
import logging
import logging.config
import os
import pathlib
import sys
from typing import TextIO

import dotenv
import pytest

from c3dc_etl import C3dcEtl


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
from c3dc_file_manager import C3dcFileManager # pylint: disable=wrong-import-position # type: ignore


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


@pytest.mark.skip('test_is_number')
def test_is_number() -> None:
    """ test_is_number """
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
        _logger.info('is_number: %s => %s', value, is_num)
        assert is_num == expected_status


@pytest.mark.skip('test_to_float')
def test_to_float() -> None:
    """ test_to_float """
    _logger.info(test_to_float.__name__)
    values: dict[any, bool] = {
        '123': 123.0,
        3: 3.0,
        -7: -7.0,
        3.14: 3.14,
        -5.2: -5.2
    }
    value: any
    expected_result: float
    for value, expected_result in values.items():
        float_value: float = C3dcEtl.to_float(value)
        _logger.info('to_float: "%s" => "%s"', value, float_value)
        assert float_value == expected_result


@pytest.mark.skip('test_is_integer')
def test_is_integer() -> None:
    """ test_is_integer """
    _logger.info(test_is_integer.__name__)
    values: dict[any, bool] = {
        '123': True,
        'x123': False,
        3: True,
        -7: True,
        3.14: False,
        -5.2: False
    }
    value: any
    expected_status: bool
    for value, expected_status in values.items():
        is_int: bool = C3dcEtl.is_integer(value)
        _logger.info('is_integer: %s => %s', value, test_is_integer)
        assert is_int == expected_status


@pytest.mark.skip('test_to_integer')
def test_to_integer() -> None:
    """ test_to_integer """
    _logger.info(test_to_integer.__name__)
    values: dict[any, bool] = {
        '123': 123,
        3: 3,
        -7: -7,
        3.14: 3,
        -5.2: -5
    }
    value: any
    expected_result: int
    for value, expected_result in values.items():
        integer_value: int = C3dcEtl.to_integer(value)
        _logger.info('to_integer: %s => %s', value, integer_value)
        assert integer_value == expected_result


@pytest.mark.skip('test_is_allowed_value')
def test_is_allowed_value() -> None:
    """ test_is_allowed_value """
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
        _logger.info('is_allowed_value: %s => %s', value, is_num)
        assert is_num == expected_status


@pytest.mark.skip('test_is_replacement_match')
def test_is_replacement_match() -> None:
    """ test_is_replacement_match """
    _logger.info(test_is_replacement_match.__name__)

    test_cases: list[dict[str, any]] = [
        {
            'source_field': 'source_field1',
            'source_record': {'source_field1': 'source_value1', 'source_field2': 'source_value2'},
            'old_value': '*',
            'expected_outcome': True
        },
        {
            'source_field': 'source_field1',
            'source_record': {'source_field1': 'source_value1', 'source_field2': 'source_value2'},
            'old_value': 'source_value1;source_value3',
            'expected_outcome': False
        },
        {
            'source_field': '[source_field1, source_field2]',
            'source_record': {'source_field1': 'source_value1', 'source_field2': 'source_value2'},
            'old_value': '*',
            'expected_outcome': True
        },
        {
            'source_field': '[source_field1, source_field2]',
            'source_record': {'source_field1': 'source_value1', 'source_field2': 'source_value2'},
            'old_value': 'source_value1;source_value3',
            'expected_outcome': False
        },
        {
            'source_field': '[source_field1, source_field2]',
            'source_record': {'source_field1': 'source_value1', 'source_field2': 'source_value2'},
            'old_value': 'source_value1;source_value2',
            'expected_outcome': True
        }
    ]
    test_case: dict[str, any]
    for test_case in test_cases:
        assert test_case['expected_outcome'] == C3dcEtl.is_replacement_match(
            test_case['source_field'],
            test_case['source_record'],
            test_case['old_value']
        )


@pytest.mark.skip('test_collate_form_data')
def test_collate_form_data() -> None:
    """ test_test_collate_form_data """
    _logger.info(test_collate_form_data.__name__)
    source_file_path: str = (
        '/Users/schoi/Workspace/PED/PCDC/Projects/c3dc_etl/documents/source/20240815/Staging/MCI/phs002790/' +
        'MCI_COG_clinical_JSON_v3/PBBUDW.json'
    )
    fp: TextIO
    with open(source_file_path, encoding='utf-8') as fp:
        obj: any = json.load(fp, object_pairs_hook=C3dcEtl.collate_form_data)
        follow_up_forms: list[dict[str, any]] = [f for f in obj.get('forms', []) if f.get('form_id') == 'FOLLOW_UP']
        assert follow_up_forms and len(follow_up_forms) == 1
        follow_up_form: dict[str, any] = follow_up_forms[0]
        assert follow_up_form and len(follow_up_form.get('data', [])) == 2
        _logger.info('%d "FOLLOW_UP.data" elements found', len(follow_up_form.get('data', [])))


@pytest.mark.skip('test_get_mapping_macros')
def test_get_mapping_macros() -> None:
    """ test_get_mapping_macros """
    _logger.info(test_get_mapping_macros.__name__)
    mapping_macro: dict[str, any] = {
        'output_field': 'diagnosis.diagnosis',
        'source_field': 'MORPHO_ICDO',
        'type_group_index': '*',
        'default_value': 'Unknown, to be completed later',
        'replacement_values': [
            {
                'old_value': '0001/0',
                'new_value': 'Neoplasm, benign'
            },
            {
                'old_value': '8681/1',
                'new_value': 'Sympathetic paraganglioma'
            },
            {
                'old_value': '*',
                'new_value': '{diagnosis}'
            }
        ]
    }
    mapping_nonmacro: dict[str, any] = {
        'output_field': 'survival.last_known_survival_status',
        'source_field': 'PT_VST',
        'type_group_index': '*',
        'default_value': 'Not reported',
        'replacement_values': [
            {
                'old_value': 'Alive',
                'new_value': 'Alive'
            },
            {
                'old_value': 'Dead',
                'new_value': 'Dead'
            },
            {
                'old_value': 'Unknown',
                'new_value': 'Unknown'
            },
            {
                'old_value': 'Not reported',
                'new_value': 'Not reported'
            }
        ]
    }

    macros: list[str] = C3dcEtl.get_mapping_macros(mapping_macro)
    assert macros
    _logger.info('Macros: "%s"', macros)

    macros = C3dcEtl.get_mapping_macros(mapping_nonmacro)
    assert not macros


@pytest.mark.skip('test_load_transformations')
def test_load_transformations() -> None:
    """ test_load_transformations """
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
    """ test_load_json_schema """
    _logger.info(test_load_json_schema.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    _logger.info('Loading JSON schema')
    json_schema: dict[str, any] = c3dc_etl.load_json_schema()
    assert json_schema
    _logger.info(json_schema.keys())


@pytest.mark.skip('test_validate_json_etl_data')
def test_validate_json_etl_data() -> None:
    """ test_validate_json_etl_data """
    _logger.info(test_validate_json_etl_data.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    _logger.info('Createing JSON ETL files')
    c3dc_etl.create_json_etl_files()

    _logger.info('Validating JSON ETL files')
    assert c3dc_etl.validate_json_etl_data()


@pytest.mark.skip('test_test_create_json_etl_files')
def test_create_json_etl_files() -> None:
    """ test_create_json_etl_files """
    _logger.info(test_create_json_etl_files.__name__)
    c3dc_etl: C3dcEtl = C3dcEtl(_config)
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()

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
            assert c3dc_file_manager.file_exists(output_file_path)
            harmonized_data: dict[str, any] = json.loads(c3dc_file_manager.read_file(output_file_path).decode('utf-8'))
            _logger.info(harmonized_data.keys())
