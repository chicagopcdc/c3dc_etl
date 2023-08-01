"""
Convert YAML to JSON
"""
import json
import logging
import logging.config
import sys
import yaml


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
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "yaml_json_converter.log",
            "mode": "w"
        }
    },
    "loggers": { 
        "": {  # root logger
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        },
        "__main__": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        }
    }
})


class YamlJsonConverter:
    """ Convert yaml to/from json """
    @staticmethod
    def is_yaml_file(input_file_path: str) -> bool:
        """ Determine if file at specified path is yaml file """
        return input_file_path.lower().endswith(('.yml', '.yaml'))

    @staticmethod
    def is_json_file(input_file_path: str) -> bool:
        """ Determine if file at specified path is json file """
        return input_file_path.lower().endswith('.json')

    @staticmethod
    def is_supported_file_type(input_file_path: str) -> bool:
        """ Determine if file at specified path is supported type """
        return YamlJsonConverter.is_yaml_file(input_file_path) or YamlJsonConverter.is_json_file(input_file_path)

    @staticmethod
    def are_same_file_type(file_path1: str, file_path2: str) -> bool:
        """ Determine if input files are both yaml or json (by extension) """
        return (
            (YamlJsonConverter.is_yaml_file(file_path1) and YamlJsonConverter.is_yaml_file(file_path2))
            or
            (YamlJsonConverter.is_json_file(file_path1) and YamlJsonConverter.is_json_file(file_path2))
        )

    @staticmethod
    def load_yaml(input_file_path: str, input_encoding: str='utf-8') -> any:
        """ Load yaml content in specified input path """
        with open(input_file_path, 'r', encoding=input_encoding) as input_file:
            return yaml.safe_load(input_file)

    @staticmethod
    def save_yaml(input_data: any, output_file_path: str, output_encoding: str='utf-8') -> None:
        """ Save yaml content to specified output path """
        with open(output_file_path, 'w', encoding=output_encoding) as output_file:
            yaml.dump(input_data, output_file, indent=2)

    @staticmethod
    def load_json(input_file_path: str, input_encoding: str='utf-8') -> any:
        """ Load json content in specified input path """
        with open(input_file_path, 'r', encoding=input_encoding) as input_file:
            return json.load(input_file)

    @staticmethod
    def save_json(input_data: any, output_file_path: str, output_encoding: str='utf-8') -> None:
        """ Save json content to specified output path """
        with open(output_file_path, 'w', encoding=output_encoding) as output_file:
            json.dump(input_data, output_file, indent=2)

    @staticmethod
    def convert(
        input_file_path: str,
        output_file_path: str,
        input_encoding: str='utf-8',
        output_encoding: str='utf-8'
    ) -> None:
        """ Convert input file from yaml to json or vice-versa depending on specified file types (extensions) """
        log_msg: str
        if not YamlJsonConverter.is_supported_file_type(input_file_path):
            log_msg = f'Unsupported input file type specified: {input_file_path}'
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)

        if not YamlJsonConverter.is_supported_file_type(output_file_path):
            log_msg = f'Unsupported output file type type specified: {output_file_path}'
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)

        if YamlJsonConverter.are_same_file_type(input_file_path, output_file_path):
            log_msg = 'Input and output file are of same type, conversion not required'
            _logger.critical(log_msg)
            raise RuntimeError(log_msg)

        input_data: any = (
            YamlJsonConverter.load_yaml(input_file_path, input_encoding)
                if YamlJsonConverter.is_yaml_file(input_file_path)
                else YamlJsonConverter.load_json(input_file_path, input_encoding)
        )

        if YamlJsonConverter.is_yaml_file(input_file_path):
            YamlJsonConverter.save_json(input_data, output_file_path, output_encoding)
        else:
            YamlJsonConverter.save_yaml(input_data, output_file_path, output_encoding)


def print_usage() -> None:
    """ Print script usage """ 
    print(f'usage: {sys.argv[0]} [input file] [output file]')


def main() -> None:
    """ Script entry point """
    if len(sys.argv) != 3:
        print_usage()
        return

    YamlJsonConverter.convert(input_file_path=sys.argv[1], output_file_path=sys.argv[2])


if __name__ == '__main__':
    main()
