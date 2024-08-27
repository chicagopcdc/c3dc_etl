""" AWS S3 """
import argparse
from collections.abc import Iterator
import logging
import logging.config
import os

from urllib.parse import urlparse, urlunparse, ParseResult

import boto3
from botocore.exceptions import ClientError


_logger: logging.Logger = logging.getLogger(__name__)

# suppress DEBUG logging from s3 transfers
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('s3transfer').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


class AwsS3:
    """ Facilitate AWS S3 access """
    def __init__(self, config: dict[str, str] = None) -> None:
        self._config: dict[str, str] = config
        self._s3: any = boto3.client('s3')

    @staticmethod
    def is_s3_uri(s3_uri: str) -> bool:
        """ Check if specified string is S3 URI """
        return str(s3_uri if s3_uri is not None else '').lower().startswith('s3://')

    @staticmethod
    def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
        """ Parse specified S3 URI and return bucket name and object path as tuple """
        parse_result: ParseResult = urlparse(s3_uri, allow_fragments=False)
        return (parse_result.netloc, parse_result.path.lstrip('/'))

    @staticmethod
    def compose_s3_uri(bucket_name: str, object_path: str) -> str:
        """ Compose S3 URI from specified bucket name and object path """
        parse_result: ParseResult = ParseResult(
            scheme='S3',
            netloc=bucket_name,
            path=object_path,
            params='',
            query='',
            fragment=''
        )
        return urlunparse(parse_result)

    def bucket_exists(self, bucket_name: str) -> bool:
        """ Check if specified bucket exists and can be accessed by authenticated user """
        try:
            response: any = self._s3.head_bucket(Bucket=bucket_name)
            return bool(response)
        except ClientError:
            return False

    def get_buckets(self) -> list[any]:
        """ Get all S3 buckets owned by authenticated user """
        response: any = self._s3.list_buckets()
        return response.get('Buckets', [])

    def get_file_object_paths(self, bucket_name: str, prefix: str = '') -> Iterator[str]:
        """ Get list of all objects in specified S3 bucket with optional prefix """
        paginator: any = self._s3.get_paginator('list_objects_v2')
        pages: any = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        page: dict[str, any]
        for page in pages:
            content: dict[str, any]
            for content in page.get('Contents', {}):
                if 'Key' not in content:
                    raise RuntimeError(f'"Key" not found in page content item: {content}')
                yield content['Key']

    def get_file_metadata(self, bucket_name: str, object_path: str) -> any:
        """ Get metadata for spcified S3 object """
        try:
            return self._s3.head_object(Bucket=bucket_name, Key=object_path)
        except ClientError:
            return None

    def get_file_size(self, bucket_name: str, object_path: str) -> int:
        """ Get size of specified S3 object """
        metadata: dict[str, any] = self.get_file_metadata(bucket_name, object_path)
        if not metadata:
            raise RuntimeError(f'File "{AwsS3.compose_s3_uri(bucket_name, object_path)}" not found')
        file_size: int = metadata.get('ContentLength', -1)
        if file_size < 0:
            raise RuntimeError(
                f'"ContentLength" attribute not found for file "{AwsS3.compose_s3_uri(bucket_name, object_path)}"'
            )
        return file_size

    def get_file_content(self, bucket_name: str, object_path: str) -> bytes:
        """ Get contents (bytes) of specified S3 bucket object """
        try:
            s3_object: any = self._s3.get_object(Bucket=bucket_name, Key=object_path)
            return s3_object['Body'].read() if s3_object else None
        except ClientError as err:
            _logger.error('Error getting content for object "%s" in bucket "%s": %s', object_path, bucket_name, err)
            return None

    def file_exists(self, bucket_name: str, object_path: str) -> bool:
        """ Check if specified S3 object exists in bucket by attempting to get file metadata """
        return bool(self.get_file_metadata(bucket_name, object_path))

    def upload_file(self, local_file_path: str, bucket_name: str, object_path: str = None) -> None:
        """ Upload specified file to bucket with S3 object name if provided, else file name """
        object_path = object_path if object_path else os.path.basename(local_file_path)
        try:
            self._s3.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=object_path)
        except ClientError as err:
            _logger.error('Error uploading file "%s" to bucket "%s": %s', local_file_path, bucket_name, err)

    def download_file(self, bucket_name: str, object_path: str, local_file_path: str = None) -> None:
        """ Download specified S3 object to local file """
        local_file_path = local_file_path if local_file_path else object_path
        try:
            self._s3.download_file(Bucket=bucket_name, Key=object_path, Filename=local_file_path)
        except ClientError as err:
            _logger.error(
                'Error downloading object "%s" from bucket "%s" to local file "%s": %s',
                object_path,
                bucket_name,
                local_file_path,
                err
            )

    def delete_file(self, bucket_name: str, object_path: str) -> None:
        """ Delete specified S3 object from bucket """
        self._s3.delete_object(Bucket=bucket_name, Key=object_path)


def main() -> None:
    """ Script entry point """
    aws_s3: AwsS3 = AwsS3()
    allowed_methods: list[str] = [
        aws_s3.bucket_exists.__name__,
        aws_s3.delete_file.__name__,
        aws_s3.download_file.__name__,
        aws_s3.file_exists.__name__,
        aws_s3.get_buckets.__name__,
        aws_s3.get_file_content.__name__,
        aws_s3.get_file_metadata.__name__,
        aws_s3.get_file_object_paths.__name__,
        aws_s3.upload_file.__name__
    ]

    parser: argparse.ArgumentParser = argparse.ArgumentParser(description='AWS S3 interface')
    parser.add_argument('--method', '-m', required=True)
    parser.add_argument('--bucket', '-b')
    parser.add_argument('--object', '-o')
    parser.add_argument('--prefix', '-p')
    parser.add_argument('--file', '-f')
    args: argparse.Namespace = parser.parse_args()
    if args.method not in allowed_methods:
        raise RuntimeError(f'Invalid method specified, must be one of {allowed_methods}')

    if args.method.lower() == AwsS3.get_buckets.__name__:
        _logger.info(aws_s3.get_buckets())
    elif args.method.lower() in (AwsS3.bucket_exists.__name__, AwsS3.get_file_object_paths.__name__):
        if not args.bucket:
            _logger.error('Bucket is required')
        else:
            if args.method.lower() == AwsS3.bucket_exists.__name__:
                _logger.info(aws_s3.bucket_exists(bucket_name=args.bucket))
            else:
                path: str
                paths: Iterator[str] = aws_s3.get_file_object_paths(
                    bucket_name=args.bucket,
                    prefix=args.prefix if args.prefix else ''
                )
                for path in paths:
                    _logger.info(path)
    elif args.method.lower() == AwsS3.upload_file.__name__:
        if not args.bucket or not args.file:
            _logger.error('Bucket and file are required')
        else:
            aws_s3.upload_file(local_file_path=args.file, bucket_name=args.bucket, object_path=args.object)
    else:
        if not args.bucket or not args.object:
            _logger.error('Bucket and object are required')
        elif args.method.lower() == AwsS3.download_file.__name__:
            aws_s3.download_file(bucket_name=args.bucket, object_path=args.object, local_file_path=args.file)
        else:
            aws_s3_method: any = getattr(aws_s3, args.method, lambda: None)
            ret_val: any = aws_s3_method(bucket_name=args.bucket, object_path=args.object)
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
                "filename": "aws_s3.log",
                "mode": "w"
            }
        },
        "loggers": {
            "": { # root logger
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

    main()
