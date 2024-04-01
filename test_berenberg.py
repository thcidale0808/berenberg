import pandas as pd
from berenberg import (
    calculate_metrics,
    clean_executions_data,
    transform_and_enrich_data,
)
import pytest

executions_data = [
    {
        "TradeTime": "2023-01-01 12:00:00.123456",
        "Quantity": 100,
        "Price": 10.5,
        "listing_id": 1,
        "primary_mic": "XNYS",
        "Venue": "NYSE",
    },
    {
        "TradeTime": "2023-01-01 12:00:01.123789",
        "Quantity": -50,
        "Price": 10.7,
        "listing_id": 1,
        "primary_mic": "XNYS",
        "Venue": "NYSE",
    },
]
market_data = [
    {
        "event_timestamp": "2023-01-01 12:00:00",
        "best_bid_price": 10.4,
        "best_ask_price": 10.6,
        "market_state": "CONTINUOUS_TRADING",
        "listing_id": 1,
        "primary_mic": "XNYS",
    },
    {
        "event_timestamp": "2023-01-01 12:00:01",
        "best_bid_price": 10.6,
        "best_ask_price": 10.8,
        "market_state": "CONTINUOUS_TRADING",
        "listing_id": 1,
        "primary_mic": "XNYS",
    },
]


@pytest.fixture
def setup_data():
    executions_df = pd.DataFrame(executions_data)
    marketdata_df = pd.DataFrame(market_data)
    return executions_df, marketdata_df


def test_calculate_metrics_basic(setup_data):
    executions_df, marketdata_df = setup_data
    result_df = calculate_metrics(executions_df, marketdata_df)

    expected_columns = [
        "TradeTime",
        "Quantity",
        "Price",
        "listing_id",
        "primary_mic",
        "Venue",
        "best_bid",
        "best_ask",
        "mid_price",
        "slippage",
    ]
    assert all(
        [column in result_df.columns for column in expected_columns]
    ), "Missing one or more expected columns."

    assert (
        result_df["best_bid"].iloc[0] == 10.4
    ), "Incorrect best bid price calculation."
    assert (
        result_df["best_ask"].iloc[0] == 10.6
    ), "Incorrect best ask price calculation."
    assert result_df["mid_price"].iloc[0] == 10.5, "Incorrect mid price calculation."
    assert (
        result_df["slippage"].iloc[0] == 0.5
    ), "Incorrect slippage calculation for buy order."
    assert result_df["slippage"].iloc[1] == pytest.approx(
        0.5
    ), "Incorrect slippage calculation for sell order."


def test_calculate_metrics_missing_market_data(setup_data):
    executions_df, marketdata_df = setup_data
    marketdata_df = marketdata_df.head(0)

    result_df = calculate_metrics(executions_df, marketdata_df)

    assert (
        result_df["best_bid"].isnull().all()
    ), "Expected best bid to be NaN due to missing market data."
    assert (
        result_df["best_ask"].isnull().all()
    ), "Expected best ask to be NaN due to missing market data."


def test_clean_executions_data():
    sample_data = [
        {
            "TradeTime": "2023-01-01 12:00:00",
            "Phase": "CONTINUOUS_TRADING",
            "Price": 100,
        },
        {"TradeTime": "2023-01-01 12:01:00", "Phase": "AUCTION", "Price": 101},
        {
            "TradeTime": "2023-01-01 12:02:00",
            "Phase": "CONTINUOUS_TRADING",
            "Price": 102,
        },
        {"TradeTime": "2023-01-01 12:03:00", "Phase": "PRE_OPEN", "Price": 103},
    ]
    executions_df = pd.DataFrame(sample_data)

    result_df = clean_executions_data(executions_df)

    assert len(result_df) == 2, "Expected 2 rows in the filtered DataFrame."
    assert all(
        result_df["Phase"] == "CONTINUOUS_TRADING"
    ), "All rows should have Phase 'CONTINUOUS_TRADING'."

    expected_prices = [100, 102]
    assert all(
        result_df["Price"].isin(expected_prices)
    ), "The filtered DataFrame contains unexpected prices."


def test_transform_and_enrich_data():
    executions_data = [
        {
            "TradeTime": "2023-01-01 12:00:00",
            "Quantity": 100,
            "Price": 10.5,
            "ISIN": "ISIN123",
        },
        {
            "TradeTime": "2023-01-01 12:01:00",
            "Quantity": -50,
            "Price": 10.7,
            "ISIN": "ISIN456",
        },
    ]
    executions_df = pd.DataFrame(executions_data)

    refdata_data = [
        {
            "ISIN": "ISIN123",
            "primary_ticker": "Ticker1",
            "primary_mic": "XNYS",
            "id": 1,
        },
        {
            "ISIN": "ISIN456",
            "primary_ticker": "Ticker2",
            "primary_mic": "XLON",
            "id": 2,
        },
    ]
    refdata_df = pd.DataFrame(refdata_data)

    expected_columns = [
        "TradeTime",
        "Quantity",
        "Price",
        "ISIN",
        "side",
        "primary_ticker",
        "primary_mic",
        "listing_id",
    ]

    result_df = transform_and_enrich_data(executions_df, refdata_df)

    assert all(
        column in result_df.columns for column in expected_columns
    ), "Returned DataFrame is missing one or more expected columns."

    assert (
        result_df.iloc[0]["side"] == 1
    ), "Expected 'side' to be 1 for positive Quantity."
    assert (
        result_df.iloc[1]["side"] == 2
    ), "Expected 'side' to be 2 for negative Quantity."

    assert (
        result_df.iloc[0]["primary_ticker"] == "Ticker1"
    ), "Incorrect or missing 'primary_ticker' after enrichment."
    assert (
        result_df.iloc[0]["listing_id"] == 1
    ), "Column 'id' from refdata_df was not correctly renamed to 'listing_id' or is missing."
    assert (
        result_df.iloc[1]["primary_mic"] == "XLON"
    ), "Incorrect or missing 'primary_mic' after enrichment."
