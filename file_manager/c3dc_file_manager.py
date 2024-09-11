""" C3DC file manager """
import argparse
from collections.abc import Iterator
import logging
import logging.config
import os
import pathlib
import sys
import tempfile
import typing
from urllib.parse import urlparse, ParseResult
from urllib.request import url2pathname

import requests


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
look_up_and_append_sys_path('aws_s3')
from aws_s3 import AwsS3 # pylint: disable=wrong-import-position


_logger = logging.getLogger(__name__)


class C3dcFileManager:
    """ Manage files that may be hosted locally or remotely via S3 or HTTP (read-only) """

    def __init__(self) -> None:
        self._aws_s3: AwsS3 = AwsS3()

    @staticmethod
    def get_basename(location: str) -> str:
        """ Get basename of specified local path or http(s)/S3 uri """
        scheme: str = str(location if location is not None else '').lower().partition('://')[0]
        if scheme == 's3':
            return os.path.basename(AwsS3.parse_s3_uri(location)[-1])

        return os.path.basename(
            urlparse(location).path if scheme in ('file', 'http', 'https')
            else location
        )

    @staticmethod
    def is_local_path(path_or_url: str) -> bool:
        """ Check if specified input is local path, including file:// URL """
        url_parts: ParseResult = urlparse(path_or_url)
        return (url_parts.scheme.lower() if url_parts.scheme else url_parts.scheme) in ('', None, 'file')

    @staticmethod
    def url_to_path(url: str) -> str:
        """ Convert specified URL to path specific to local platform """
        url_parts: ParseResult = urlparse(url)
        host = f"{os.path.sep}{os.path.sep}{url_parts.netloc}{os.path.sep}"
        return os.path.normpath(os.path.join(host, url2pathname(url_parts.path)))

    @staticmethod
    def get_url_content(url: str, local_save_path: str = None) -> bytes | bytearray:
        """ Retrieve and return contents from specified URL """
        scheme: str = str(url if url is not None else '').lower().partition('://')[0]
        url_content: bytes | bytearray
        if scheme == 'file':
            local_file: typing.BinaryIO
            with open(C3dcFileManager.url_to_path(url), 'rb') as local_file:
                url_content = local_file.read()
        elif scheme in ('http', 'https'):
            response: requests.Response
            with requests.get(url, stream=True, timeout=30) as response:
                response.raise_for_status()
                url_content = response.content
        else:
            raise RuntimeError(f'Unsupported URL type/format/protocol: {url}')

        if local_save_path:
            write_fp: typing.BinaryIO
            with open(local_save_path, 'wb') as write_fp:
                write_fp.write(url_content)

        return url_content

    @staticmethod
    def url_content_exists(url: str, follow_redirects: bool = False) -> any:
        """ Check if specified URL has content that can be downloaded """
        scheme: str = str(url if url is not None else '').lower().partition('://')[0]
        if scheme =='file':
            return os.path.exists(C3dcFileManager.url_to_path(url))

        if scheme not in ('http', 'https'):
            raise RuntimeError(f'Unsupported URL type/format/protocol: {url}')

        response: requests.Response
        with requests.get(url, stream=True, timeout=30, allow_redirects=follow_redirects) as response:
            if 300 <= response.status_code <= 399:
                return False
            try:
                response.raise_for_status()
                chunk: any
                for chunk in response.iter_content(chunk_size=8192):
                    return bool(chunk)
            except requests.exceptions.HTTPError:
                return False

    @staticmethod
    def join_location_paths(*paths: tuple[str, ...]) -> str:
        """
        Join specified location paths. '/' will be used as delimiter for URI paths (first path has format
        schema://path/to/location), else system path separator (os.path.sep) will be used
        """
        if not paths or all(p == '' for p in paths):
            raise RuntimeError('No non-empty paths to join specified')
        if len(paths) == 1:
            return paths[0]
        scheme: str = str(paths[0] if paths[0] is not None else '').lower().partition('://')[0]
        path_sep: str = '/' if scheme in ('file', 'http', 'https', 's3') else os.path.sep
        output_prefix: str = path_sep if paths[0].startswith(path_sep) else ''
        output_suffix: str = path_sep if paths[-1].endswith(path_sep) else ''
        return output_prefix + path_sep.join(p.strip(path_sep) for p in paths) + output_suffix

    @staticmethod
    def split_location_paths(location: str) -> tuple[str, ...]:
        """
        Split specified location into path components. For URI locations having format schema://path/to/location
        where schema is file/http(s)/s3, path components will consist of base or root path followed by remaining
        path components, for example (schema://path, to, location). Otherwise location will be assumed to be local
        filesystem path and string split will be used to separate path components (os.path.sep), with special
        consideration for system path separator as prefix/suffix. For example '/path/to/location/' will return
        ('/path', 'to', 'location/') instead of ('', 'path', 'to', 'location', '').
        """
        if not location:
            raise RuntimeError('No location to split specified')
        if location in ('/', '//'):
            return tuple(location)
        schema_parts: tuple[str, str, str] = str(location if location is not None else '').partition('://')
        if schema_parts[0].lower() not in ('file', 'http', 'https', 's3'):
            paths: list[str] = location.split(os.path.sep)
            while paths and paths[0] == '':
                paths.pop(0)
                paths[0] = f'{os.path.sep}{paths[0]}'
            while paths and paths[-1] == '':
                paths.pop()
                paths[-1] = f'{paths[-1]}{os.path.sep}'
            return tuple(paths)

        # location is file/http(s)/s3 uri
        non_scheme_path: str = schema_parts[-1]
        paths: list[str] = non_scheme_path.split('/')
        base_location: str = f'{schema_parts[0]}{schema_parts[1]}'
        while paths[0] == '':
            paths.pop(0)
            base_location = f'{base_location}/'
        base_location: str = f'{base_location}{paths[0]}'
        paths.pop(0)
        if paths:
            last_path_suffix: str = ''
            while paths and paths[-1] == '':
                paths.pop()
                last_path_suffix = f'{last_path_suffix}/'
            if paths:
                paths[-1] = f'{paths[-1]}{last_path_suffix}'
            else:
                base_location = f'{base_location}{last_path_suffix}'
        return (base_location,) + tuple(paths)

    def list_files(self, location: str) -> Iterator[str]:
        """ Get list of full paths for files in specified local directory or S3 uri """
        prefix: str = str(location if location is not None else '').lower().partition('://')[0]
        if prefix in ('http', 'https'):
            raise RuntimeError(f'Unsupported location type: {prefix}')

        if prefix == 's3':
            bucket_name: str
            object_path: str
            bucket_name, object_path = AwsS3.parse_s3_uri(location)
            yield from (
                AwsS3.compose_s3_uri(bucket_name, p) for p in
                    self._aws_s3.get_file_object_paths(bucket_name, object_path) if not p.endswith('/')
            )
        else:
            yield from (
                d.path for d in os.scandir(
                    location if prefix != 'file' else C3dcFileManager.url_to_path(location)
                ) if os.path.isfile(d.path)
            )

    def file_exists(self, location: str) -> bool:
        """ Check if file exists at specified local path or http(s)/S3 uri """
        prefix: str = str(location if location is not None else '').lower().partition('://')[0]
        if prefix == 's3':
            bucket_name: str
            object_path: str
            bucket_name, object_path = AwsS3.parse_s3_uri(location)
            return self._aws_s3.file_exists(bucket_name, object_path)

        if prefix in ('http', 'https'):
            # this is best avoided for files that aren't trivially small
            return bool(C3dcFileManager.url_content_exists(location))

        return os.path.exists(location if prefix != 'file' else C3dcFileManager.url_to_path(location))

    def read_file(self, location: str, local_save_path: str = None) -> bytes | bytearray:
        """ Get byte buffer containing content of file at specified local path or http(s)/S3 uri """
        prefix: str = str(location if location is not None else '').lower().partition('://')[0]
        if prefix == 's3':
            bucket_name: str
            object_path: str
            bucket_name, object_path = AwsS3.parse_s3_uri(location)
            file_content: bytes = self._aws_s3.get_file_content(bucket_name, object_path)
            if local_save_path:
                self.write_file(file_content, local_save_path)
            return file_content

        return C3dcFileManager.get_url_content(
            location if prefix in ('file', 'http', 'https') else pathlib.Path(os.path.abspath(location)).as_uri(),
            local_save_path
        )

    def get_file_size(self, location: str) -> int:
        """ Get size of file at specified local path or http(s)/S3 uri """
        prefix: str = str(location if location is not None else '').lower().partition('://')[0]
        if prefix == 's3':
            bucket_name: str
            object_path: str
            bucket_name, object_path = AwsS3.parse_s3_uri(location)
            return self._aws_s3.get_file_size(bucket_name, object_path)

        if prefix in ('http', 'https'):
            # content-length header unreliable, need to read entire file
            # set hard limit to require intervention when checking size of large files
            response: requests.Response = requests.head(location, allow_redirects=False, timeout=30)
            if 300 <= response.status_code <= 399:
                raise RuntimeError(f'Redirect returned for location "{location}"')
            content_length: int = int(response.headers.get('content-length', -1))
            if content_length < 0:
                raise RuntimeError(f'"Content-length" attribute not specified for location "{location}"')
            if content_length > 8 * 1024 * 1024:
                _logger.warning('Getting size of file with content-length %d via full download', content_length)
            return len(self.read_file(location))

        return os.path.getsize(location if prefix != 'file' else C3dcFileManager.url_to_path(location))

    def write_file(self, buffer: bytes, location: str) -> None:
        """ Write byte buffer containing content of file to specified local path or S3 uri """
        prefix: str = str(location if location is not None else '').lower().partition('://')[0]
        if prefix in ('http', 'https'):
            raise RuntimeError(f'Unsupported write location: {location}')

        fp: typing.BinaryIO
        if prefix == 's3':
            # save to temp file and then upload to S3
            bucket_name: str
            object_path: str
            bucket_name, object_path = AwsS3.parse_s3_uri(location)
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(buffer)
                tmp_file.flush()
                tmp_file.close()
                try:
                    self._aws_s3.upload_file(tmp_file.name, bucket_name, object_path)
                finally:
                    os.remove(tmp_file.name)
        else:
            with open (C3dcFileManager.url_to_path(location) if prefix == 'file' else location, 'wb') as fp:
                fp.write(buffer)

    def delete_file(self, location: str) -> None:
        """ Delete file at specified local path or S3 uri """
        prefix: str = str(location if location is not None else '').lower().partition('://')[0]
        if prefix in ('http', 'https'):
            raise RuntimeError(f'Unsupported delete location: {location}')

        if prefix == 's3':
            # save to temp file and then upload to S3
            bucket_name: str
            object_path: str
            bucket_name, object_path = AwsS3.parse_s3_uri(location)
            self._aws_s3.delete_file(bucket_name, object_path)
        else:
            os.remove(C3dcFileManager.url_to_path(location) if prefix == 'file' else location)


def main() -> None:
    """ Script entry point """
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    allowed_methods: list[str] = [
        c3dc_file_manager.file_exists.__name__,
        c3dc_file_manager.read_file.__name__,
        c3dc_file_manager.delete_file.__name__
    ]

    parser: argparse.ArgumentParser = argparse.ArgumentParser(description='C3DC file manager interface')
    parser.add_argument('--method', '-m', required=True)
    parser.add_argument('--location', '-l', required=True)
    args: argparse.Namespace = parser.parse_args()
    if args.method not in allowed_methods:
        raise RuntimeError(f'Invalid method specified, must be one of {allowed_methods}')

    c3dc_file_manager_method: any = getattr(c3dc_file_manager, args.method, lambda: None)
    ret_val: any = c3dc_file_manager_method(location=args.location)
    if ret_val is not None:
        _logger.info(ret_val)


if __name__ == '__main__':
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
                "filename": "c3dc_file_manager.log",
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

    main()
