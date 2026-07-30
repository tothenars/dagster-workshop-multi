import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

import db


def build_daily_order_summary(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    exchange_rates: pd.DataFrame,
    predictions: pd.DataFrame,
) -> pd.DataFrame:
    merged = orders.merge(products, on="product_id", how="inner")
    merged["total_usd"] = merged["quantity"] * merged["price"]

    eur_rate = exchange_rates.loc[
        exchange_rates["quote_currency"] == "EUR", "rate"
    ].iloc[0]
    merged["total_eur"] = merged["total_usd"] * eur_rate

    merged = merged.merge(
        predictions[["order_id", "predicted_label", "probability"]],
        on="order_id",
        how="left",
    )

    return merged[
        [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "total_usd",
            "total_eur",
            "predicted_label",
            "probability",
        ]
    ]


@asset
def daily_order_summary() -> pd.DataFrame:
    # Cross-container read: these four tables are written by pipeline_products,
    # pipeline_fx, and pipeline_ml — three independent containers that only
    # share the warehouse Postgres as a landing zone.
    orders = db.read_table("orders")
    products = db.read_table("products")
    exchange_rates = db.read_table("exchange_rates")
    predictions = db.read_table("order_value_predictions")
    return build_daily_order_summary(orders, products, exchange_rates, predictions)


@asset
def daily_order_summary_table(daily_order_summary: pd.DataFrame) -> int:
    return db.load_table(daily_order_summary, "daily_order_summary")


@asset_check(asset=daily_order_summary)
def daily_order_summary_has_no_missing_totals(
    daily_order_summary: pd.DataFrame,
) -> AssetCheckResult:
    missing = int(daily_order_summary["total_eur"].isna().sum())
    return AssetCheckResult(passed=missing == 0, metadata={"missing_total_eur_rows": missing})


refresh_analytics_job = define_asset_job(name="refresh_analytics_job")

refresh_analytics_daily = ScheduleDefinition(
    name="refresh_analytics_daily",
    job=refresh_analytics_job,
    cron_schedule="0 7 * * *",  # after the 06:00 ingestion schedules
)

defs = Definitions(
    assets=[daily_order_summary, daily_order_summary_table],
    asset_checks=[daily_order_summary_has_no_missing_totals],
    jobs=[refresh_analytics_job],
    schedules=[refresh_analytics_daily],
)
