""" Test C3dcEtl """
import logging

import pytest

from c3dc_etl_model_node import C3dcEtlModelNode


pytest.skip("test_c3dc_etl_model_node.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_logger.info('%s: ccdi', __name__)


def setup_module() -> None:
    """ setup/teardown """
    _logger.info('%s: ccdi', __name__)


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


@pytest.mark.skip('test_get_pluralized_node_name')
def test_get_pluralized_node_name() -> None:
    """ test_get_pluralized_node_name """
    _logger.info(test_get_pluralized_node_name.__name__)
    singular_plural_names: dict[str, str] = {
        'diagnosis': 'diagnoses',
        'genetic_analysis': 'genetic_analyses',
        'laboratory_test': 'laboratory_tests',
        'participant': 'participants',
        'reference_file': 'reference_files',
        'sample': 'samples',
        'study': 'studies',
        'survival': 'survivals',
        'synonym': 'synonyms',
        'treatment': 'treatments',
        'treatment_response': 'treatment_responses',
    }

    assert set(singular_plural_names.keys()) == {n.value for n in C3dcEtlModelNode}

    singular_name: str
    expected_plural_name: str
    for singular_name, expected_plural_name in singular_plural_names.items():
        plural_name:str = C3dcEtlModelNode.get_pluralized_node_name(singular_name)
        logging.info('get_pluralized_node_name: %s => %s', singular_name, plural_name)
        assert plural_name == expected_plural_name
