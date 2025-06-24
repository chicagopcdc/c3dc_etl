""" Test C3dcEtl """
import hashlib
import json
import logging
import os
import pathlib
import sys

import dotenv
import pytest

from c3dc_etl import C3dcEtl
from c3dc_etl_model_node import C3dcEtlModelNode

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
_logger.info('%s: target_nbl', __name__)
_config: any = dotenv.dotenv_values('.env_test')


def setup_module() -> None:
    """ setup/teardown """
    _logger.info('%s: target_nbl', __name__)


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
        logging.info('is_number: %s => %s', value, is_num)
        assert is_num == expected_status


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
        logging.info('is_allowed_value: %s => %s', value, is_num)
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


@pytest.mark.skip('test_is_macro_mapping')
def test_is_macro_mapping() -> None:
    """ test_is_macro_mapping """
    _logger.info(test_is_macro_mapping.__name__)
    mappings_expected_results: list[tuple[dict[str, any], bool]] = [
        (
            {
                "output_field": "study.study_id",
                "source_field": "[string_literal]",
                "type_group_index": "*",
                "default_value": None,
                "replacement_values": [
                    {
                        "old_value": "*",
                        "new_value": "phs000463"
                    }
                ]
            }, False
        ),
        (
            {
                "output_field": "survival.age_at_last_known_survival_status",
                "source_field": "[Age at Diagnosis in Days, Overall Survival Time in Days]",
                "type_group_index": "*",
                "default_value": -999,
                "replacement_values": [
                    {
                        "old_value": "*",
                        "new_value": "{sum}"
                    }
                ]
            }, True
        ),
        (
            {
                "output_field": "diagnosis.diagnosis_id",
                "source_field": "[string_literal]",
                "type_group_index": "*",
                "default_value": None,
                "replacement_values": [
                    {
                        "old_value": "*",
                        "new_value": "{uuid}"
                    }
                ]
            }, True
        )
    ]
    mapping: dict[str, any]
    expected_result: bool
    for (mapping, expected_result) in mappings_expected_results:
        assert C3dcEtl.is_macro_mapping(mapping) == expected_result


@pytest.mark.skip('test_sort_data')
def test_sort_data() -> None:
    """ test_sort_data """
    _logger.info(test_sort_data.__name__)
    data_expected_results: list[tuple[any, any]] = [
        (0, 0),
        ((0, 1), (0, 1)),
        ((1, 0), (0, 1)),
        ('1', '1'),
        (('0', '1'), ('0', '1')),
        (('1', '0'), ('0', '1')),
        (('a', 'b', 'c'), ('a', 'b', 'c')),
        (('a', 'c', 'b'), ('a', 'b', 'c')),
        ([0, 1], [0, 1]),
        ([1, 0], [0, 1]),
        (['0', '1'], ['0', '1']),
        (['1', '0'], ['0', '1']),
        (['a', 'b', 'c'], ['a', 'b', 'c']),
        ({'a': 1, 'b': 2}, {'a': 1, 'b': 2}),
        ({'b': 2, 'a': 1}, {'a': 1, 'b': 2}),
        ({1: 'a', 2: 'b'}, {1: 'a', 2: 'b'}),
        ({2: 'b', 1: 'a'}, {1: 'a', 2: 'b'}),
        (
            {
                'a': 1,
                'b': [0, 1],
                'c': (20, 30, 40),
                'd': {'x': 'x', 'y': 'y', 'z': 'z'},
            },
            {
                'a': 1,
                'b': [0, 1],
                'c': (20, 30, 40),
                'd': {'x': 'x', 'y': 'y', 'z': 'z'}
            }
        ),
        (
            {
                'd': {'z': 'z', 'y': 'y', 'x': 'x'},
                'c': (20, 30, 40),
                'a': 1,
                'b': [1, 0]
            },
            {
                'a': 1,
                'b': [0, 1],
                'c': (20, 30, 40),
                'd': {'x': 'x', 'y': 'y', 'z': 'z'}
            }
        )
    ]
    data: any
    expected_result: any
    for (data, expected_result) in data_expected_results:
        _logger.info('%s => %s', data, expected_result)
        assert C3dcEtl.sort_data(data) == expected_result
        assert json.dumps(C3dcEtl.sort_data(data), sort_keys=True) == json.dumps(expected_result, sort_keys=True)


@pytest.mark.skip('test_get_node_id_field_name')
def test_get_node_id_field_name() -> None:
    """ test_get_node_id_field_name """
    node: C3dcEtlModelNode
    for node in C3dcEtlModelNode:
        assert C3dcEtl.get_node_id_field_name(node) == f'{node}_id'
        assert C3dcEtl.get_node_id_field_name(node, True) == f'{node}.{node}_id'


@pytest.mark.skip('test_get_cache_key')
def test_get_cache_key() -> None:
    """ test_get_cache_key """
    node: C3dcEtlModelNode
    for node in C3dcEtlModelNode:
        _logger.info('testing "%s"', node)
        participant_id: str = 'participant id'
        record: dict[str, any] = { C3dcEtl.get_node_id_field_name(node): f'{node} id' }
        cacheable_record: dict[str, any] = C3dcEtl.get_cacheable_record(record, node)
        cache_key: tuple[str, str, str] = C3dcEtl.get_cache_key(record, participant_id, node)
        _logger.info(cacheable_record)
        assert cache_key[0] == hashlib.sha1(
            json.dumps(C3dcEtl.sort_data(cacheable_record), sort_keys=True).encode('utf-8')
        ).hexdigest()
        assert cache_key[1] == participant_id
        assert cache_key[2] == node
        _logger.info('pass: "%s"', node)


@pytest.mark.skip('test_get_cacheable_record')
def test_get_cacheable_record() -> None:
    """ test_get_cacheable_record """
    test_participant_record: dict[str, any] = {
        'participant_id': 'participant_id',
        'ethnicity': 'ethnicity',
        'race': 'race',
        'sex_at_birth': 'sex_at_birth',
        'study.study_id': 'study id'
    }
    expected_participant_record: dict[str, any] = {
        'participant_id': '',
        'ethnicity': 'ethnicity',
        'race': 'race',
        'sex_at_birth': 'sex_at_birth',
        'study.study_id': 'study id'
    }
    node: C3dcEtlModelNode
    for node in C3dcEtl.OBSERVATION_NODES:
        test_participant_record[C3dcEtl.get_node_id_field_name(node, True)] = [
            f'{node} id'
        ]
        expected_participant_record[C3dcEtl.get_node_id_field_name(node, True)] = []

    test_study_record: dict[str, any] = {
        'study_id': 'study id',
        'dbgap_accession': 'dbgap accession',
        'study_name': 'study name',
        'consent': 'consent',
        'consent_number': 1,
        'external_url': 'external url',
        'study_status': 'study status',
        'study_description': 'study description',
        'participant.participant_id': [ 'participant id' ],
        'reference_file.reference_file_id': [ 'reference file id' ]
    }
    expected_study_record: dict[str, any] = {
        'study_id': '',
        'dbgap_accession': 'dbgap accession',
        'study_name': 'study name',
        'consent': 'consent',
        'consent_number': 1,
        'external_url': 'external url',
        'study_status': 'study status',
        'study_description': 'study description',
        'participant.participant_id': [],
        'reference_file.reference_file_id': []
    }

    test_reference_file_record: dict[str, any] = {
        'reference_file_id': 'reference file id',
        'dcf_indexd_guid': 'dcf indexd guid',
        'file_name': 'file name',
        'file_type': 'file type',
        'file_category': "file category",
        'file_size': 1,
        'md5sum': 'md5 sum',
        'file_description': 'file description',
        'reference_file_url': 'reference file url',
        'study.study_id': 'study id'
    }
    expected_reference_file_record: dict[str, any] = {
        'reference_file_id': '',
        'dcf_indexd_guid': '',
        'file_name': 'file name',
        'file_type': 'file type',
        'file_category': "file category",
        'file_size': 1,
        'md5sum': 'md5 sum',
        'file_description': 'file description',
        'reference_file_url': 'reference file url',
        'study.study_id': 'study id'
    }

    test_diagnosis_record: dict[str, any] = {
        'diagnosis_id': 'diagnosis id',
        'disease_phase': 'disease phase',
        'diagnosis_classification_system': 'diagnosis classification system',
        'diagnosis_basis': 'diagnosis basis',
        'age_at_diagnosis': 1,
        'tumor_stage_clinical_t': 'tumor stage clinical t',
        'tumor_stage_clinical_n': 'tumor stage clinical n',
        'tumor_stage_clinical_m': 'tumor stage clinical m',
        'diagnosis_comment': 'diagnosis comment',
        'diagnosis': 'diagnosis',
        'year_of_diagnosis': 1,
        'anatomic_site': 'anatomic site',
        'laterality': 'laterality',
        'toronto_childhood_cancer_staging': 'toronto childhood cancer staging',
        'tumor_grade': 'tumor grade',
        'tumor_classification': 'tumor classification',
        'participant.participant_id': 'participant id'
    }
    expected_diagnosis_record: dict[str, any] = {
        'diagnosis_id': '',
        'disease_phase': 'disease phase',
        'diagnosis_classification_system': 'diagnosis classification system',
        'diagnosis_basis': 'diagnosis basis',
        'age_at_diagnosis': 1,
        'tumor_stage_clinical_t': 'tumor stage clinical t',
        'tumor_stage_clinical_n': 'tumor stage clinical n',
        'tumor_stage_clinical_m': 'tumor stage clinical m',
        'diagnosis_comment': 'diagnosis comment',
        'diagnosis': 'diagnosis',
        'year_of_diagnosis': 1,
        'anatomic_site': 'anatomic site',
        'laterality': 'laterality',
        'toronto_childhood_cancer_staging': 'toronto childhood cancer staging',
        'tumor_grade': 'tumor grade',
        'tumor_classification': 'tumor classification',
        'participant.participant_id': 'participant id'
    }

    nodes_test_expected_records: list[tuple[C3dcEtlModelNode, dict[str, any], dict[str, any]]] = [
        (C3dcEtlModelNode.PARTICIPANT, test_participant_record, expected_participant_record),
        (C3dcEtlModelNode.STUDY, test_study_record, expected_study_record),
        (C3dcEtlModelNode.REFERENCE_FILE, test_reference_file_record, expected_reference_file_record),
        (C3dcEtlModelNode.DIAGNOSIS, test_diagnosis_record, expected_diagnosis_record)
    ]
    node_test_expected_record: tuple[C3dcEtlModelNode, dict[str, any], dict[str, any]]
    for node_test_expected_record in nodes_test_expected_records:
        _logger.info('testing "%s"', node_test_expected_record[0])
        actual_cacheable_record: dict[str, any] = C3dcEtl.get_cacheable_record(
            node_test_expected_record[1],
            node_test_expected_record[0]
        )
        assert actual_cacheable_record == node_test_expected_record[2]
        _logger.info('pass: "%s"', node_test_expected_record[0])

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


@pytest.mark.skip('test_create_json_etl_files')
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
        if c3dc_file_manager.file_exists(output_file_path):
            c3dc_file_manager.delete_file(output_file_path)
        assert not c3dc_file_manager.file_exists(output_file_path)

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
