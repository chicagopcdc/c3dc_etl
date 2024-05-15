""" Test AwsS3 """
import logging
import logging.config
import os

import dotenv
import pytest

from aws_s3 import AwsS3


pytest.skip("test_aws_s3.py", allow_module_level=True)

_logger: logging.Logger = logging.getLogger(__name__)
_logger.info(__name__)
_config: any = dotenv.dotenv_values('.env')


@pytest.mark.skip('test_adhoc')
def test_adhoc() -> None:
    """ test_adhoc """
    assert True


@pytest.mark.skip('test_parse_s3_uri')
def test_parse_s3_uri() -> None:
    """ test_parse_s3_uri """
    _logger.info(test_parse_s3_uri.__name__)
    s3_uri: str = 'S3://c3dc-test/path/to/.env'
    bucket_name: str
    object_path: str
    bucket_name, object_path = AwsS3.parse_s3_uri(s3_uri)
    assert bucket_name == 'c3dc-test' and object_path == 'path/to/.env'


@pytest.mark.skip('test_compose_s3_uri')
def test_compose_s3_uri() -> None:
    """ test_compose_s3_uri """
    _logger.info(test_compose_s3_uri.__name__)
    bucket_name: str = 'c3dc-test'
    object_path: str = '.env'
    s3_uri: str = AwsS3.compose_s3_uri(bucket_name, object_path)
    assert s3_uri == f'S3://{bucket_name}/{object_path}'


@pytest.mark.skip('test_bucket_exists')
def test_bucket_exists() -> None:
    """ test_bucket_exists """
    _logger.info(test_bucket_exists.__name__)
    s3: AwsS3 = AwsS3(_config)
    bucket_exists: bool = s3.bucket_exists('c3dc-test')
    assert bucket_exists

    bucket_exists = s3.bucket_exists('invalid-bucket')
    assert not bucket_exists


@pytest.mark.skip('test_get_buckets')
def test_get_buckets() -> None:
    """ test_get_buckets """
    _logger.info(test_get_buckets.__name__)
    s3: AwsS3 = AwsS3(_config)
    buckets: list[any] = s3.get_buckets()
    assert buckets
    bucket: any
    for bucket in buckets:
        _logger.info(bucket.get('Name', 'Name not found'))


@pytest.mark.skip('test_get_file_object_paths')
def test_get_file_object_paths() -> None:
    """ test_get_file_object_paths """
    _logger.info(test_get_file_object_paths.__name__)
    bucket_name: str = 'c3dc-test'
    prefix: str = 'data/source/phs002790/phs002790-MCI_COG_clinical_JSON_March24/'
    s3: AwsS3 = AwsS3(_config)
    object_paths: list[str] = list(s3.get_file_object_paths(bucket_name, prefix))
    assert object_paths
    _logger.info('%d object paths returned', len(object_paths))
    object_path: any
    for object_path in object_paths[-3:]:
        _logger.info(object_path)

@pytest.mark.skip('test_get_file_metadata')
def test_get_file_metadata() -> None:
    """ test_get_file_metadata """
    _logger.info(test_get_file_metadata.__name__)
    s3: AwsS3 = AwsS3(_config)
    file_metadata: any = s3.get_file_metadata('c3dc-test', 'schema/schema.json')
    assert file_metadata
    _logger.info(file_metadata)

    file_metadata = s3.get_file_metadata('c3dc-test', 'invalid-file')
    assert not file_metadata


@pytest.mark.skip('test_get_file_size')
def test_get_file_size() -> None:
    """ test_get_file_size """
    _logger.info(test_get_file_size.__name__)
    s3: AwsS3 = AwsS3(_config)
    file_size: int = s3.get_file_size('c3dc-test', 'schema/schema.json')
    assert file_size > 0
    _logger.info(file_size)


@pytest.mark.skip('test_get_file_content')
def test_get_file_content() -> None:
    """ test_get_file_content """
    _logger.info(test_get_file_content)
    s3: AwsS3 = AwsS3(_config)
    file_content: bytes = s3.get_file_content('c3dc-test', '.env')
    assert file_content
    _logger.info(file_content.decode('utf-8'))

    file_content: bytes = s3.get_file_content('c3dc-test', 'invalid-file')
    assert not file_content


@pytest.mark.skip('test_file_exists')
def test_file_exists() -> None:
    """ test_get_file_metadata """
    _logger.info(test_file_exists.__name__)
    s3: AwsS3 = AwsS3(_config)
    file_exists: bool = s3.file_exists('c3dc-test', '.env')
    assert file_exists

    file_exists = s3.file_exists('c3dc-test', 'invalid-file')
    assert not file_exists


@pytest.mark.skip('test_upload_file')
def test_upload_file() -> None:
    """ test_upload_file """
    _logger.info(test_upload_file.__name__)
    s3: AwsS3 = AwsS3(_config)
    s3.upload_file('.env', 'c3dc-test', '.env-test')


@pytest.mark.skip('test_download_file')
def test_download_file() -> None:
    """ test_download_file """
    _logger.info(test_download_file.__name__)
    s3: AwsS3 = AwsS3(_config)
    file_path: str = './.env_test_download_file'
    if os.path.isfile(file_path):
        os.remove(file_path)
    assert not os.path.exists(file_path)
    s3.download_file('c3dc-test', '.env', '.env_test_download_file')
    assert os.path.exists(file_path)
    os.remove(file_path)


@pytest.mark.skip('test_delete_file')
def test_delete_file() -> None:
    """ test_delete_file """
    _logger.info(test_delete_file.__name__)
    s3: AwsS3 = AwsS3(_config)
    bucket_name: str = 'c3dc-test'
    file_path: str = './.env'
    object_path: str = '.env-test'
    if not s3.file_exists(bucket_name, object_path):
        s3.upload_file(file_path, bucket_name, object_path)
    assert s3.file_exists(bucket_name, object_path)
    s3.delete_file(bucket_name, object_path)
    assert not s3.file_exists(bucket_name, object_path)
