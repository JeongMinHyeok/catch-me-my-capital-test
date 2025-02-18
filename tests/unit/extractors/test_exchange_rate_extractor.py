import pandas as pd
import pytest

from dags.brz_exchange_rate_daily.extractors import fetch_exchange_rates


def test_exchange_rate_transformation(
    mocker, temp_file, mock_logical_date, mock_s3_hook
):
    """환율 데이터 변환 테스트"""
    test_data = pd.DataFrame({"Close": [1200.50, 1300.75]})
    mocker.patch("yfinance.download", return_value=test_data)

    fetch_exchange_rates(
        currency_pairs=["USDKRW=X"],
        exchange_rate_tmp_file_path=temp_file,
        exchange_rate_data_s3_key="test/key",
        logical_date=mock_logical_date,
    )


def test_exchange_rate_empty_data_handling(
    mocker, temp_file, mock_logical_date, mock_s3_hook
):
    """빈 환율 데이터 처리 테스트"""
    empty_df = pd.DataFrame(columns=["Close"])
    mocker.patch("yfinance.download", return_value=empty_df)

    with pytest.raises(ValueError) as exc_info:
        fetch_exchange_rates(
            currency_pairs=["USDKRW=X"],
            exchange_rate_tmp_file_path=temp_file,
            exchange_rate_data_s3_key="test/key",
            logical_date=mock_logical_date,
        )

    assert "No data fetched" in str(exc_info.value)
