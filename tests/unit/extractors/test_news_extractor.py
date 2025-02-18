import json

import pytest

from dags.brz_news_weekly.extractors import fetch_news_data


def test_news_data_transformation(mocker, temp_file, mock_logical_date):
    """뉴스 데이터 변환 테스트"""
    # S3Hook 모킹 누락 수정
    mocker.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")  # 추가

    # 테스트용 뉴스 데이터
    sample_response = {
        "response": {
            "docs": [
                {
                    "abstract": "Test article abstract",
                    "web_url": "https://test.com/article",
                    "headline": {"main": "Test Headline"},
                    "pub_date": "2024-01-01T00:00:00Z",
                    "section_name": "Business",
                    "byline": {"original": "By Test Author"},
                    "word_count": 500,
                    "keywords": [{"value": "Finance"}, {"value": "Technology"}],
                }
            ]
        }
    }

    # requests 모킹
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_response
    mocker.patch("requests.get", return_value=mock_response)

    # 테스트 실행
    fetch_news_data(
        temp_file_path=temp_file,
        api_key="test_key",
        news_data_s3_key="test/key",
        logical_date=mock_logical_date,
    )

    # JSON 파일 검증
    with open(temp_file, "r") as f:
        result_data = json.load(f)

    # 데이터 구조 검증
    assert isinstance(result_data, list)
    assert len(result_data) == 1

    first_record = result_data[0]
    assert "abstract" in first_record
    assert "web_url" in first_record
    assert "headline" in first_record
    assert "pub_date" in first_record
    assert "section_name" in first_record
    assert "byline" in first_record
    assert "keywords" in first_record

    # 데이터 값 검증
    assert first_record["headline"] == "Test Headline"
    assert first_record["section_name"] == "Business"
    assert isinstance(first_record["keywords"], list)
    assert "Finance" in first_record["keywords"]
