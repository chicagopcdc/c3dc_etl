""" Test C3dcFileManager """
import logging
import logging.config
import os
import tempfile
import typing

import dotenv
import pytest

from c3dc_file_manager import C3dcFileManager


#pytest.skip("test_c3dc_file_manager.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_logger.info(__name__)
_config: any = dotenv.dotenv_values('.env')


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


@pytest.mark.skip('test_get_basename')
def test_get_basename() -> None:
    """ test_get_basename """
    _logger.info(test_get_basename.__name__)
    locations_expected_basenames: dict[str, str]  = {
        'S3://bucket/file.ext': 'file.ext',
        'https://example.com/endpoint': 'endpoint',
        'https://example.com/endpoint?parm=value': 'endpoint',
        'file:///path/to/file.ext': 'file.ext',
        '/path/to/file.ext': 'file.ext',
        '../file.ext': 'file.ext'
    }
    location: str
    expected_basename: str
    for location, expected_basename in locations_expected_basenames.items():
        assert C3dcFileManager.get_basename(location) == expected_basename


@pytest.mark.skip('test_is_local_path')
def test_is_local_path() -> None:
    """ test_is_local_path """
    _logger.info(test_is_local_path.__name__)
    urls_expected_disposition: dict[str, bool]  = {
        'S3://bucket/file.ext': False,
        'http://server/path/to/endpoint': False,
        'file:///path/to/location': True,
        '/absolute/path/to/location': True,
        'relative/path/to/location': True
    }
    url: str
    disposition: bool
    for url, disposition in urls_expected_disposition.items():
        assert C3dcFileManager.is_local_path(url) == disposition


@pytest.mark.skip('test_url_to_path')
def test_url_to_path() -> None:
    """ test_url_to_path """
    _logger.info(test_url_to_path.__name__)
    urls_expected_paths: dict[str, str]  = {
        'S3://bucket/file.ext': '/file.ext',
        'http://server/path/to/endpoint': '/path/to/endpoint',
        'file:///path/to/location': '/path/to/location'
    }
    url: str
    expected_path: str
    for url, expected_path in urls_expected_paths.items():
        assert C3dcFileManager.url_to_path(url) == expected_path


@pytest.mark.skip('test_get_url_content')
def test_get_url_content() -> None:
    """ test_get_url_content """
    _logger.info(test_get_url_content.__name__)
    urls_expected_contents: dict[str, str] = {
        'https://portal.pedscommons.org/login': '<!doctype html>',
        'file:///etc/zshrc': '# System-wide profile'
    }
    url: str
    expected_content: str
    for url, expected_content in urls_expected_contents.items():
        actual_content: bytes | bytearray = C3dcFileManager.get_url_content(url)
        assert actual_content.decode('utf-8').startswith(expected_content)


@pytest.mark.skip('test_url_content_exists')
def test_url_content_exists() -> None:
    """ test_url_content_exists """
    _logger.info(test_url_content_exists.__name__)
    urls_expected_results: dict[str, bool]  = {
        'https://portal.pedscommons.org/login': True,
        'file:///etc/zshrc': True,
        'http://portal.pedscommons.org/invalid/path/to/location': False,
        'file:///invalid/path/to/location': False
    }
    url: str
    expected_result: bool
    for url, expected_result in urls_expected_results.items():
        _logger.info('%s: %s', url, expected_result)
        assert C3dcFileManager.url_content_exists(url) == expected_result


@pytest.mark.skip('test_join_location_paths')
def test_join_location_paths() -> None:
    """ test_join_location_paths """
    _logger.info(test_join_location_paths.__name__)
    paths_locations: dict[tuple[str, ...], str]  = {
        ('/',): '/',
        ('root',): 'root',
        ('/root',): '/root',
        ('/root/',): '/root/',
        ('/root', 'path', 'to', 'location'): '/root/path/to/location',
        ('root', 'path', 'to', 'location'): 'root/path/to/location',
        ('/root/path', 'to', 'location'): '/root/path/to/location',
        ('/root/path', 'to', 'location/'): '/root/path/to/location/',
        ('root/path', 'to', 'location'): 'root/path/to/location',
        ('file:///root/path', 'to/location', 'dir', 'file.ext'):
            'file:///root/path/to/location/dir/file.ext',
        ('https://example.com', 'endpoint'): 'https://example.com/endpoint',
        ('http://example.com', 'path', 'to', 'endpoint'): 'http://example.com/path/to/endpoint',
        ('s3://bucket/dir1/dir2', 'file.ext'): 's3://bucket/dir1/dir2/file.ext',
        ('s3://bucket/dir1/dir2/', 'file.ext'): 's3://bucket/dir1/dir2/file.ext'
    }

    paths: tuple[str, ...]
    expected_location: str
    for paths, expected_location in paths_locations.items():
        _logger.info('%s: %s', paths, expected_location)
        location: str = C3dcFileManager.join_location_paths(*paths)
        assert location == expected_location


@pytest.mark.skip('test_split_location_paths')
def test_split_location_paths() -> None:
    """ test_split_location_paths """
    _logger.info(test_split_location_paths.__name__)
    location_paths: dict[str, tuple[str, ...]]  = {
        '/': ('/',),
        '/path': ('/path',),
        '/path/': ('/path/',),
        '/path/to': ('/path', 'to'),
        '/path/to/': ('/path', 'to/'),
        '/root/path/to/location': ('/root', 'path', 'to', 'location'),
        '/root/path/to/location/': ('/root', 'path', 'to', 'location/'),
        'file:///root/path/to/file.ext': ('file:///root', 'path', 'to', 'file.ext'),
        'https://example.com/endpoint': ('https://example.com', 'endpoint'),
        'http://example.com/path/to/location': ('http://example.com', 'path', 'to', 'location'),
        'http://example.com/path/to/location/': ('http://example.com', 'path', 'to', 'location/'),
        's3://bucket': ('s3://bucket',),
        's3://bucket/': ('s3://bucket/',),
        's3://bucket/dir1/dir2/file.ext': ('s3://bucket', 'dir1', 'dir2', 'file.ext'),
        's3://bucket/dir1/dir2//file.ext': ('s3://bucket', 'dir1', 'dir2', '', 'file.ext')
    }

    location: str
    expected_paths: tuple[str, ...]
    for location, expected_paths in location_paths.items():
        _logger.info('%s:', location)
        _logger.info('expected: %s', expected_paths)
        paths: tuple[str, ...] = C3dcFileManager.split_location_paths(location)
        _logger.info('actual: %s', paths)
        assert paths == expected_paths


@pytest.mark.skip('test_list_files')
def test_list_files() -> None:
    """ test_list_files """
    _logger.info(test_list_files.__name__)
    locations_counts: dict[str, int]  = {
        'file:///etc/security/': 6,
        's3://c3dc-test/data/source/phs002790/phs002790-MCI_COG_clinical_JSON_March24/': 3565,
        'https://portal.pedscommons.org/login': -1
    }
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    location: str
    expected_count: int
    for location, expected_count in locations_counts.items():
        _logger.info('%s: %s', location, expected_count)
        try:
            files: list[str] = list(c3dc_file_manager.list_files(location))
            assert len(files) == expected_count
            if files:
                _logger.info(len(files))
                _logger.info(files[0])
                _logger.info(files[-1])
        except RuntimeError:
            if not (location.startswith('http:s') or location.startswith('https://')):
                raise


@pytest.mark.skip('test_file_exists')
def test_file_exists() -> None:
    """ test_file_exists """
    _logger.info(test_file_exists.__name__)
    locations_expected_results: dict[str, bool]  = {
        'S3://c3dc-test/mapping/C3DC Mappings.xlsx': True,
        'https://portal.pedscommons.org/login': True,
        'file:///etc/zshrc': True,
        '/etc/zshrc': True,
        'S3://c3dc-test/non-existent-file': False,
        'http://portal.pedscommons.org/path/to/invalid/location': False,
        'file:///path/to/location': False,
        '/invalid/path/to/file': False,
    }
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    location: str
    expected_result: bool
    for location, expected_result in locations_expected_results.items():
        assert c3dc_file_manager.file_exists(location) == expected_result


@pytest.mark.skip('test_read_file')
def test_read_file() -> None:
    """ test_read_file """
    _logger.info(test_read_file.__name__)
    locations_expected_results: dict[str, str]  = {
        'S3://c3dc-test/schema/schema.json': '{',
        'https://portal.pedscommons.org/login': '<!doctype html>',
        'file:///etc/zshrc': '# System-wide profile',
        '/etc/zshrc': '# System-wide profile',
        './c3dc_file_manager.py': '"""',
        './missing_file': FileNotFoundError.__name__
    }
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    location: str
    expected_result: str
    for location, expected_result in locations_expected_results.items():
        try:
            buf: bytes = c3dc_file_manager.read_file(location)
            _logger.info(location)
            assert buf.decode('utf-8').startswith(expected_result)
        except FileNotFoundError:
            if expected_result != FileNotFoundError.__name__:
                raise

    # test local save
    local_save_path: str = './test.html'
    if c3dc_file_manager.file_exists(local_save_path):
        c3dc_file_manager.delete_file(local_save_path)
    assert not c3dc_file_manager.file_exists(local_save_path)
    c3dc_file_manager.read_file('https://portal.pedscommons.org/login', local_save_path)
    assert c3dc_file_manager.file_exists(local_save_path)
    c3dc_file_manager.delete_file(local_save_path)


@pytest.mark.skip('test_get_file_size')
def test_get_file_size() -> None:
    """ test_read_file """
    _logger.info(test_get_file_size.__name__)
    locations: list[str]  = [
        'S3://c3dc-test/schema/schema.json',
        #'https://portal.pedscommons.org/login',
        'https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json',
        'file:///etc/zshrc',
        '/etc/zshrc'
    ]
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    location: str
    for location in locations:
        _logger.info(location)
        tmp_file_data: bytes = c3dc_file_manager.read_file(location)
        tmp_file: any
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(tmp_file_data)
            tmp_file.flush()
            tmp_file.close()
            assert os.path.getsize(tmp_file.name) == c3dc_file_manager.get_file_size(location)
            if os.path.exists(tmp_file.name):
                os.remove(tmp_file.name)

@pytest.mark.skip('test_write_file')
def test_write_file() -> None:
    """ test_write_file """
    _logger.info(test_write_file.__name__)
    source_file: str = '/etc/zshrc'
    buffer: bytes
    fp: typing.BinaryIO
    with open(source_file, mode='rb') as fp:
        buffer = fp.read()
    locations: list[str]  = [
        'S3://c3dc-test/test_zshrc',
        'file:///tmp/zshrc',
        '/tmp/zshrc'
    ]
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    location: str
    for location in locations:
        if c3dc_file_manager.file_exists(location):
            c3dc_file_manager.delete_file(location)
        assert not c3dc_file_manager.file_exists(location)
        c3dc_file_manager.write_file(buffer, location)
        assert c3dc_file_manager.file_exists(location)
        buf: bytes = c3dc_file_manager.read_file(location)
        assert buf == buffer

@pytest.mark.skip('test_delete_file')
def test_delete_file() -> None:
    """ test_delete_file """
    _logger.info(test_delete_file.__name__)
    locations: list[str]  = [
        'S3://c3dc-test/zshrc',
        'file:///tmp/zshrc',
        '/tmp/zshrc'
    ]
    c3dc_file_manager: C3dcFileManager = C3dcFileManager()
    location: str
    for location in locations:
        if not c3dc_file_manager.file_exists(location):
            buf: bytes = c3dc_file_manager.read_file('/etc/zshrc')
            c3dc_file_manager.write_file(buf, location)

        assert c3dc_file_manager.file_exists(location)
        c3dc_file_manager.delete_file(location)
        assert not c3dc_file_manager.file_exists(location)
