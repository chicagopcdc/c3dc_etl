""" Test HarmonizedDataReporter """
import csv
import io
import json
import logging
import logging.config
import os
import pathlib
import sys

import dotenv
import pytest

from harmonized_data_reporter import HarmonizedDataReporter

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
from c3dc_file_manager import C3dcFileManager # pylint: disable=wrong-import-position; # type: ignore


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
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    report_output_path: str = _config.get('REPORT_OUTPUT_PATH', './harmonized_data_report.csv')

    if c3dc_file_manager.file_exists(report_output_path):
        c3dc_file_manager.delete_file(report_output_path)

    harmonized_data_reporter.create_report()
    harmonized_data_reporter.save_report()
    assert c3dc_file_manager.file_exists(report_output_path)

    reader: csv.DictReader = csv.DictReader(io.StringIO(c3dc_file_manager.read_file(report_output_path).decode()))
    assert {row['study'] for row in reader} == harmonized_data_reporter.harmonized_data_files.keys()
