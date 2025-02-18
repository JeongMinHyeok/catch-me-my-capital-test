import os
import tempfile
from datetime import datetime
from unittest.mock import patch

import boto3
import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from moto import mock_aws


@pytest.fixture
def s3_mock():
    """Moto를 사용하여 S3를 모킹"""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="ap-northeast-2")
        s3_client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
        )
        yield s3_client


@pytest.fixture
def mock_s3_hook(s3_mock):
    """S3Hook을 모킹"""
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook") as mock_hook:
        mock_instance = mock_hook.return_value
        mock_instance.get_conn.return_value = s3_mock
        yield mock_instance


def test_s3_hook(mock_s3_hook):
    """S3Hook 테스트"""
    hook = S3Hook(aws_conn_id="test_conn")
    client = hook.get_conn()

    # 파일 업로드 테스트
    client.put_object(Bucket="test-bucket", Key="test-key", Body="test-data")

    # 파일 확인
    response = client.get_object(Bucket="test-bucket", Key="test-key")
    assert response["Body"].read().decode() == "test-data"


@pytest.fixture
def mock_logical_date():
    """테스트용 logical_date"""
    return datetime(2024, 1, 1)


@pytest.fixture
def temp_file():
    """임시 파일 생성 및 삭제를 관리하는 fixture"""
    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, "test_coin_data.csv")

    try:
        yield tmp_path
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except FileNotFoundError:
            pass
