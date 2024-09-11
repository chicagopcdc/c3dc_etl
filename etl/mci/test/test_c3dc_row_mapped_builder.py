""" Test C3dcRowMappedBuilder """
import logging
import logging.config
import os
import pathlib
import sys

import dotenv
import pytest

from c3dc_etl import C3dcEtl
from c3dc_etl_model_node import C3dcEtlModelNode
from c3dc_row_mapped_builder import C3dcRowMappedBuilder


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


pytest.skip("test_c3dc_row_mapped_builder.py", allow_module_level=True)

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
        is_num: bool = C3dcRowMappedBuilder.is_number(value)
        logging.info('is_number: %s => %s', value, is_num)
        assert is_num == expected_status


@pytest.mark.skip('test_get_instance')
def test_get_instance() -> None:
    """ test_get_instance """
    _logger.info(test_get_instance.__name__)
    assert C3dcRowMappedBuilder.NODE_SOURCE_VARIABLE_FIELDS

    builder: C3dcRowMappedBuilder

    node: C3dcEtlModelNode
    for node in C3dcRowMappedBuilder.NODE_SOURCE_VARIABLE_FIELDS:
        builder = C3dcRowMappedBuilder.get_instance(node)
        assert builder

    with pytest.raises(NotImplementedError):
        builder = C3dcRowMappedBuilder.get_instance(C3dcEtlModelNode.STUDY)


@pytest.mark.skip('test_sum_abs_first')
def test_sum_abs_first() -> None:
    """ test_sum_abs_first """
    addends_expected_results: dict[tuple[int | float | str | None, ...], int] = {
        (1,): 1,
        ('notanumber',): None,
        (1, 2): 3,
        (-3, -2): 1,
        ('', 1): None,
        (None, 1): None
    }
    addends: tuple[int | float | str | None]
    expected_result: int | None
    for addends, expected_result in addends_expected_results.items():
        actual_result: int | None = C3dcRowMappedBuilder.sum_abs_first(*addends)
        assert actual_result == expected_result

@pytest.mark.skip('test_get_records')
def test_get_records() -> None:
    """ test_get_records """
    # pylint: disable=protected-access
    c3dc_etl: C3dcEtl = C3dcEtl(_config)

    _logger.info('Loading study configurations')
    study_configuration: dict[str, any]
    for study_configuration in c3dc_etl._study_configurations:
        transformation: dict[str, any]
        for transformation in study_configuration.get('transformations', []):
            source_data: list[dict[str, any]] = c3dc_etl._load_source_data(
                study_configuration.get('study'),
                transformation
            )
            node: C3dcEtlModelNode
            for node in C3dcRowMappedBuilder.NODE_SOURCE_VARIABLE_FIELDS:
                total_records: int = 0

                # pylint: disable=no-member
                mappings: list[dict[str, any]] = c3dc_etl._get_row_mapped_node_mappings(transformation, node)
                c3dc_row_mapped_builder: C3dcRowMappedBuilder = C3dcRowMappedBuilder.get_instance(node)
                c3dc_row_mapped_builder.generate_uuid_callback = c3dc_etl._generate_uuid
                c3dc_row_mapped_builder.convert_output_value_callback = \
                    c3dc_etl._get_json_schema_node_property_converted_value
                c3dc_row_mapped_builder.is_output_property_required_callback = \
                    c3dc_etl._is_json_schema_node_property_required
                c3dc_row_mapped_builder.mappings = mappings
                c3dc_row_mapped_builder.logger=_logger
                index: int
                source_record: dict[str, any]
                processed: int = 0
                for index, source_record in enumerate(source_data):
                    processed += 1
                    index += 1
                    records: list[dict[str, any]] = c3dc_row_mapped_builder.get_records(source_record)
                    total_records += len(records)
                    _logger.info(
                        '%d "%s" records for source record %s',
                        len(records),
                        node,
                        source_record[C3dcRowMappedBuilder.SUBJECT_ID_FIELD]
                    )

                    if index > 5:
                        break

                _logger.info(
                    '%d total "%s" records retrieved for %d subjects (%d processed)',
                    total_records,
                    node,
                    len(source_data),
                    processed
                )
                assert total_records

def do_test() -> None:
    """ do_test """
    raise RuntimeError('do_test')
