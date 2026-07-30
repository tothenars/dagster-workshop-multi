from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
from main import (
    build_daily_order_summary,
    daily_order_summary,
    daily_order_summary_has_no_missing_totals,
    daily_order_summary_table,
)

FAKE_ORDERS = pd.DataFrame(
    {"order_id": [1, 2], "customer_id": [100, 101], "product_id": [1, 2], "quantity": [2, 1]}
)
FAKE_PRODUCTS = pd.DataFrame(
    {"product_id": [1, 2], "name": ["Widget", "Gadget"], "price": [10.0, 20.0]}
)
FAKE_RATES = pd.DataFrame(
    {"base_currency": ["USD", "USD"], "quote_currency": ["EUR", "GBP"], "rate": [0.9, 0.8]}
)
FAKE_PREDICTIONS = pd.DataFrame(
    {"order_id": [1, 2], "predicted_label": [1, 0], "probability": [0.8, 0.3]}
)


def test_build_daily_order_summary_converts_to_eur_and_joins_predictions():
    result = build_daily_order_summary(FAKE_ORDERS, FAKE_PRODUCTS, FAKE_RATES, FAKE_PREDICTIONS)

    row = result.loc[result["order_id"] == 1].iloc[0]
    assert row["total_usd"] == 20.0
    assert row["total_eur"] == 18.0  # 20 * 0.9
    assert row["predicted_label"] == 1


def test_daily_order_summary_check_passes_when_no_missing_totals():
    summary = pd.DataFrame({"total_eur": [1.0, 2.0, 3.0]})

    result = daily_order_summary_has_no_missing_totals(summary)

    assert result.passed is True
    assert result.metadata["missing_total_eur_rows"].value == 0


def test_daily_order_summary_check_fails_when_totals_missing():
    summary = pd.DataFrame({"total_eur": [1.0, None, 3.0]})

    result = daily_order_summary_has_no_missing_totals(summary)

    assert result.passed is False
    assert result.metadata["missing_total_eur_rows"].value == 1


def test_analytics_pipeline_materializes_and_passes_check():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    def fake_read_table(table_name: str) -> pd.DataFrame:
        return {
            "orders": FAKE_ORDERS,
            "products": FAKE_PRODUCTS,
            "exchange_rates": FAKE_RATES,
            "order_value_predictions": FAKE_PREDICTIONS,
        }[table_name]

    with patch.object(db, "read_table", side_effect=fake_read_table), patch.object(
        db, "load_table", side_effect=fake_load_table
    ):
        result = materialize(
            [daily_order_summary, daily_order_summary_table, daily_order_summary_has_no_missing_totals]
        )

    assert result.success
    assert len(loaded["daily_order_summary"]) == 2

    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].passed is True
