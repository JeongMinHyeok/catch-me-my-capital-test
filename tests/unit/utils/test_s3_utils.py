import pytest
from boto3.session import Session

from dags.common.s3_utils import upload_file_to_s3, upload_string_to_s3


def test_upload_file_to_s3(temp_file, mock_s3_hook):
    """파일 S3 업로드 테스트"""
    # 타입 검증 우회를 위한 추가 설정
    mock_s3_hook.get_session.return_value.__class__ = Session

    upload_file_to_s3(file_path=temp_file, key="test/key")
    mock_s3_hook.load_file.assert_called_once()


def test_upload_string_to_s3(mock_s3_hook):
    """문자열 S3 업로드 테스트"""
    test_string = '{"test": "data"}'
    test_key = "test/key"
    mock_s3_hook.get_session.return_value.__class__ = Session

    upload_string_to_s3(test_string, test_key)

    mock_s3_hook.load_string.assert_called_once_with(
        string_data=test_string, key=test_key, bucket_name="test-bucket", replace=True
    )
