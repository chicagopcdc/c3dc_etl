""" C3DC Harmonized Data Reporter """
import csv
import json
import logging
import logging.config
import os
import sys

import dotenv


_logger = logging.getLogger(__name__)
if _logger.hasHandlers():
    _logger.handlers.clear()

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s]: %(message)s"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "data_reporter.log",
            "mode": "w"
        }
    },
    "loggers": {
        "": { # root logger
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        },
        "__main__": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        }
    }
})


class HarmonizedDataReporter:
    """ Build report for harmonized data to be delivered to NCI """
    def __init__(self, config: dict[str, str]) -> None:
        self._config: dict[str, str] = config
        self._harmonized_data_files: dict[str, str] = json.loads(
            config.get('HARMONIZED_DATA_FILES', '{}')
        )
        self._harmonized_data_report: dict[str, dict[str, any]] = {}
        self._report_output_path: str = config.get('REPORT_OUTPUT_PATH', './harmonized_data_report.csv')

        if not self._harmonized_data_files:
            raise RuntimeError('No harmonized data files specified for inclusion in report')

    @property
    def harmonized_data_files(self) -> dict[str, str]:
        """ Get internal collection of harmonized data files specified in config for inclusion in report """
        return self._harmonized_data_files

    @property
    def harmonized_data_report(self) -> any:
        """ Get internal petl table representing harmonized data report """
        return self._harmonized_data_report

    def create_report(self) -> None:
        """ Create report for harmonized data files specified in confg """
        _logger.info('Creating harmonized data report')
        self.harmonized_data_report.clear()
        file_identifier: str
        file_path: str
        for file_identifier, file_path in self.harmonized_data_files.items():
            with open(file_path, mode='r', encoding='utf-8') as fp:
                file_data: dict[str, any] = json.load(fp)
                report_data: dict[str, any] = {'study': file_identifier}
                report_data.update({node_name:len(node_items) for node_name,node_items in file_data.items()})
                self.harmonized_data_report[file_identifier] = report_data

    def save_report(self) -> None:
        """ Save report to output (csv) file """
        _logger.info('Saving harmonized data report to %s', self._report_output_path)
        if not self.harmonized_data_report:
            _logger.error('No output path specified to save report')
            return

        with open(self._report_output_path, mode='w', encoding='utf-8') as fp:
            writer: csv.DictWriter = csv.DictWriter(
                fp,
                fieldnames=list(self._harmonized_data_report.values())[0].keys()
            )
            writer.writeheader()
            writer.writerows(self._harmonized_data_report.values())


def print_usage() -> None:
    """ Print script usage """ 
    print(f'usage: python {sys.argv[0]} [schema file] [data file]')


def main() -> None:
    """ Script entry point """
    config_file: str = '.env'
    if not os.path.exists(config_file):
        raise FileNotFoundError(f'Config file "{config_file}" not found')
    config: dict[str, str] = dotenv.dotenv_values(config_file)
    harmonized_data_reporter: HarmonizedDataReporter = HarmonizedDataReporter(config)
    harmonized_data_reporter.create_report()
    harmonized_data_reporter.save_report()

if __name__ == '__main__':
    main()
