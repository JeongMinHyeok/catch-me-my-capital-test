import pandas as pd
import pytest

from dags.brz_index_daily.extractors import fetch_index_data

# 테스트용 심볼
test_symbols = {"FTASEANAS": "961167"}


def test_index_data_transformation(mocker, temp_file, mock_logical_date, mock_s3_hook):
    """지수 데이터 변환 테스트"""
    # 테스트용 데이터프레임 생성
    test_data = pd.DataFrame({"Close": [3000.50, 3100.75]})
    mocker.patch("yfinance.download", return_value=test_data)

    # 테스트 실행
    fetch_index_data(
        symbols=test_symbols,
        index_tmp_file_path=temp_file,
        index_data_s3_key="test/key",
        logical_date=mock_logical_date,
    )


def test_index_empty_data_handling(mocker, temp_file, mock_logical_date, mock_s3_hook):
    """빈 지수 데이터 처리 테스트"""
    empty_df = pd.DataFrame(columns=["Close"])
    mocker.patch("yfinance.download", return_value=empty_df)

    with pytest.raises(ValueError) as exc_info:
        fetch_index_data(
            symbols=test_symbols,
            index_tmp_file_path=temp_file,
            index_data_s3_key="test/key",
            logical_date=mock_logical_date,
        )

    assert "No data fetched" in str(exc_info.value)


def test_index_data_error_handling(mocker, temp_file, mock_logical_date, mock_s3_hook):
    """지수 데이터 에러 처리 테스트"""
    # 에러 응답 모킹
    mock_scraper = mocker.Mock()
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_scraper.get.return_value = mock_response
    mocker.patch("cloudscraper.create_scraper", return_value=mock_scraper)

    # 예외 발생 검증
    with pytest.raises(Exception) as exc_info:
        fetch_index_data(
            symbols=test_symbols,
            index_tmp_file_path=temp_file,
            index_data_s3_key="test/key",
            logical_date=mock_logical_date,
        )

    assert "Error fetching data" in str(exc_info.value)
