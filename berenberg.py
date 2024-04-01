import os
import time
import pandas as pd
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def analyze_executions(executions_df: pd.DataFrame, market_df: pd.DataFrame):
    total_executions = len(executions_df)
    unique_venues = executions_df["Venue"].nunique()
    executions_df["Date"] = pd.to_datetime(executions_df["TradeTime"]).dt.date
    unique_dates = executions_df["Date"].nunique()

    min_trade_time = pd.to_datetime(executions_df["TradeTime"]).min()
    max_trade_time = pd.to_datetime(executions_df["TradeTime"]).max()

    logging.info(f"Total number of executions: {total_executions}")
    logging.info(f"Unique number of venues: {unique_venues}")
    logging.info(f"Unique dates of executions: {unique_dates}")
    logging.info(
        f"Executions TradeTime ranges from {min_trade_time} to {max_trade_time}"
    )

    market_df["event_timestamp"] = pd.to_datetime(market_df["event_timestamp"])
    min_event_time = market_df["event_timestamp"].min()
    max_event_time = market_df["event_timestamp"].max()

    logging.info(
        f"Market data event_timestamp ranges from {min_event_time} to {max_event_time}"
    )


def transform_and_enrich_data(
    executions_df: pd.DataFrame, refdata_df: pd.DataFrame
) -> pd.DataFrame:

    executions_df["side"] = executions_df["Quantity"].apply(lambda x: 2 if x < 0 else 1)

    refdata_df_renamed = refdata_df.rename(columns={"id": "listing_id"})

    enriched_executions_df = executions_df.merge(
        refdata_df_renamed[["ISIN", "primary_ticker", "primary_mic", "listing_id"]],
        on="ISIN",
        how="left",
    )

    logging.info("Data transformation and enrichment completed successfully.")

    return enriched_executions_df


def clean_executions_data(executions_df: pd.DataFrame) -> pd.DataFrame:
    continuous_trading_df = executions_df[
        executions_df["Phase"] == "CONTINUOUS_TRADING"
    ]
    logging.info(
        f"Number of CONTINUOUS_TRADING executions: {len(continuous_trading_df)}"
    )
    return continuous_trading_df


def load_data(file_path: str) -> pd.DataFrame:
    return pd.read_parquet(file_path)


def calculate_metrics(
    executions_df: pd.DataFrame, marketdata_df: pd.DataFrame
) -> pd.DataFrame:
    marketdata_filtered = marketdata_df[
        marketdata_df["market_state"] == "CONTINUOUS_TRADING"
    ]
    marketdata_filtered["event_timestamp"] = pd.to_datetime(
        marketdata_filtered["event_timestamp"]
    ).dt.floor("S")
    agg_marketdata_df = (
        marketdata_filtered.groupby(["event_timestamp", "listing_id", "primary_mic"])
        .agg(
            best_bid_price=("best_bid_price", "max"),
            best_ask_price=("best_ask_price", "min"),
        )
        .reset_index()
    )
    agg_marketdata_df.set_index(
        ["event_timestamp", "listing_id", "primary_mic"], inplace=True
    )

    def fetch_market_data(row, offset_seconds):
        time_for_lookup = pd.to_datetime(row["TradeTime"]).floor("S") + pd.Timedelta(
            seconds=offset_seconds
        )
        try:
            keys = (time_for_lookup, row["listing_id"], row["primary_mic"])
            data = agg_marketdata_df.loc[keys]
            return data["best_bid_price"], data["best_ask_price"]
        except KeyError:
            return None, None

    for offset, suffix in [(0, ""), (-1, "_min_1s"), (1, "_1s")]:
        executions_df[f"best_bid{suffix}"], executions_df[f"best_ask{suffix}"] = zip(
            *executions_df.apply(lambda row: fetch_market_data(row, offset), axis=1)
        )
        executions_df[f"mid_price{suffix}"] = (
            executions_df[f"best_bid{suffix}"] + executions_df[f"best_ask{suffix}"]
        ) / 2

    conditions = [executions_df["Quantity"] > 0, executions_df["Quantity"] < 0]
    choices = [
        (executions_df["best_ask"] - executions_df["Price"])
        / (executions_df["best_ask"] - executions_df["best_bid"]),
        (executions_df["Price"] - executions_df["best_bid"])
        / (executions_df["best_ask"] - executions_df["best_bid"]),
    ]
    executions_df["slippage"] = np.select(conditions, choices, default=0)

    logging.info("Calculation of metrics completed successfully.")

    return executions_df


def save_output(output_df: pd.DataFrame, output_path: str):
    output_dir = os.path.dirname(output_path)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    output_df.to_parquet(output_path)

    logging.info(f"Output saved to '{output_path}'.")


def run(
    executions_path: str, refdata_path: str, marketdata_path: str, output_path: str
):
    total_start_time = time.time()

    logging.info("Loading data...")
    executions_df = load_data(executions_path)
    refdata_df = load_data(refdata_path)
    marketdata_df = load_data(marketdata_path)
    logging.info(f"Executions data count: {executions_df.shape[0]}")
    logging.info(f"Refdata count: {refdata_df.shape[0]}")
    logging.info(f"Marketdata count: {marketdata_df.shape[0]}")

    logging.info("Starting analysis of executions data...")
    analyze_executions(executions_df, marketdata_df)

    logging.info("Starting data cleaning for CONTINUOUS_TRADING executions...")
    filtered_executions_df = clean_executions_data(executions_df)
    logging.info(f"Filtered executions data count: {filtered_executions_df.shape[0]}")

    logging.info("Starting data transformation and enrichment...")
    enriched_executions_df = transform_and_enrich_data(
        filtered_executions_df, refdata_df
    )
    logging.info(f"Enriched executions data count: {enriched_executions_df.shape[0]}")

    logging.info("Starting calculation of metrics...")
    output_df = calculate_metrics(enriched_executions_df, marketdata_df)

    logging.info(f"Saving file to {output_path}...")
    save_output(output_df, output_path)

    total_end_time = time.time()  # End timing for total execution time
    logging.info(
        f"Total execution time: {total_end_time - total_start_time:.2f} seconds."
    )


if __name__ == "__main__":
    executions_file_path = os.getenv("EXECUTIONS_FILE_PATH", "data/executions.parquet")
    refdata_file_path = os.getenv("REFDATA_FILE_PATH", "data/refdata.parquet")
    marketdata_file_path = os.getenv("MARKETDATA_FILE_PATH", "data/marketdata.parquet")
    output_file_path = os.getenv("OUTPUT_FILE_PATH", "output/trading_metrics.parquet")
    run(executions_file_path, refdata_file_path, marketdata_file_path, output_file_path)
