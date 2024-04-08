""" Test HarmonizedDataReporter """
import csv
import json
import logging
import logging.config
import os

import dotenv
import pytest

from harmonized_data_reporter import HarmonizedDataReporter


pytest.skip("test_harmonized_data_reporter.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_config: any = dotenv.dotenv_values('.env_test')


def setup_module() -> None:
    """ module-wide test setup """
    _logger.info(__name__)


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


@pytest.mark.skip('test_create_report')
def test_create_report() -> None:
    """ test test_create_report """
    _logger.info(test_create_report.__name__)
    harmonized_data_reporter: HarmonizedDataReporter = HarmonizedDataReporter(_config)
    harmonized_data_files: dict[str, str] = json.loads(
        _config.get('HARMONIZED_DATA_FILES', '{}')
    )
    assert not harmonized_data_reporter.harmonized_data_report
    harmonized_data_reporter.create_report()
    assert harmonized_data_reporter.harmonized_data_report
    assert harmonized_data_reporter.harmonized_data_report.keys() == harmonized_data_files.keys()


@pytest.mark.skip('test_save_report')
def test_save_report() -> None:
    """ test test_save_report """
    _logger.info(test_save_report.__name__)
    harmonized_data_reporter: HarmonizedDataReporter = HarmonizedDataReporter(_config)
    report_output_path: str = _config.get('REPORT_OUTPUT_PATH', './harmonized_data_report.csv')
    if os.path.exists(report_output_path):
        os.remove(report_output_path)
    harmonized_data_reporter.create_report()
    harmonized_data_reporter.save_report()
    assert os.path.exists(report_output_path)
    with open(report_output_path, mode='r', encoding='utf-8') as fp:
        reader: csv.DictReader = csv.DictReader(fp)
        assert {row['study'] for row in reader} == harmonized_data_reporter.harmonized_data_files.keys()
