import csv

import pytest

from dags.brz_coin_daily.extractors import fetch_coin_data


def test_coin_data_transformation(mocker, temp_file, mock_logical_date, mock_s3_hook):
    """코인 데이터 변환 테스트"""
    sample_coin_data = [
        [
            1234567890000,  # Open time
            "50000",  # Open
            "51000",  # High
            "49000",  # Low
            "50500",  # Close
            "100",  # Volume
            1234567899999,  # Close time
            "5050000",  # Quote asset volume
            150,  # Number of trades
            "60",  # Taker buy base asset volume
            "3030000",  # Taker buy quote asset volume
            "0",  # Ignore
        ]
    ]

    sample_symbols = {
        "BTCUSDT": "Bitcoin",
        "ETHUSDT": "Ethereum",
    }

    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_coin_data
    mocker.patch("requests.get", return_value=mock_response)

    # S3 업로드 모킹 - 전체 경로로 수정
    mock_upload = mocker.patch(
        "dags.brz_coin_daily.extractors.upload_file_to_s3", return_value=None
    )

    fetch_coin_data(
        symbols=sample_symbols,
        coin_tmp_file_path=temp_file,
        coin_data_s3_key="test/key",
        logical_date=mock_logical_date,
    )

    # upload_file_to_s3가 호출되었는지 확인
    mock_upload.assert_called_once_with(
        key="test/key", file_path=temp_file, remove_after_upload=False
    )

    # 파일을 읽기 전에 복사본 생성
    temp_file_copy = f"{temp_file}.copy"
    with open(temp_file, "r") as original:
        with open(temp_file_copy, "w") as copy:
            copy.write(original.read())

    with open(temp_file_copy, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

        assert rows[0] == [
            "Open_time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close_time",
            "Quote_asset_volume",
            "Number_of_trades",
            "Taker_buy_base_asset_volume",
            "Taker_buy_quote_asset_volume",
            "Ignore",
            "Symbol",
            "Name",
        ]

        assert len(rows) > 1
        data_row = rows[1]
        assert data_row[0] == str(sample_coin_data[0][0])
        assert data_row[1] == sample_coin_data[0][1]
        assert data_row[-2] in sample_symbols.keys()
        assert data_row[-1] in sample_symbols.values()
