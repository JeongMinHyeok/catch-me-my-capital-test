import json

import pytest

from dags.brz_msci_index_daily.extractors import fetch_msci_indices_data


def test_msci_indices_data_transformation(
    mocker, temp_file, mock_logical_date, mock_s3_hook
):
    """MSCI 지수 데이터 변환 테스트"""
    # 테스트용 MSCI 데이터
    sample_response = [
        {
            "Date": "2024-01-01",
            "Open": "2800.50",
            "High": "2850.75",
            "Low": "2780.25",
            "Close": "2840.00",
            "Volume": "500000",
        }
    ]

    # requests 모킹
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_response
    mocker.patch("requests.get", return_value=mock_response)

    # 테스트용 URL 정보
    test_url_info = {
        "MSCI_WORLD_URL": "test_url/{month}.%20{day}%20{year}",
        "MSCI_EMERGING_URL": "test_url/{month}.%20{day}%20{year}",
    }

    # 테스트 실행
    fetch_msci_indices_data(
        msci_url_info=test_url_info,
        msci_index_tmp_file_path=temp_file,
        msci_index_data_s3_key="test/key",
        logical_date=mock_logical_date,
    )

    # JSON 파일 검증
    with open(temp_file.replace(".json", "_MSCI_World.json"), "r") as f:
        result_data = json.load(f)

    # 데이터 구조 검증
    assert isinstance(result_data, list)
    assert len(result_data) == 1

    first_record = result_data[0]
    assert "RecordDate" in first_record
    assert "Open" in first_record
    assert "High" in first_record
    assert "Low" in first_record
    assert "Close" in first_record
    assert "Volume" in first_record
    assert "Index_Name" in first_record

    # 데이터 값 검증
    assert first_record["Index_Name"] == "MSCI_World"
    assert first_record["Open"] == "2800.50"
    assert first_record["Close"] == "2840.00"
