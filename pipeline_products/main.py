import pandas as pd
from dagster import Definitions, ScheduleDefinition, asset, define_asset_job
from dagster import asset_check, AssetCheckResult

import db
import source


@asset
def raw_products() -> pd.DataFrame:
    products = source.fetch_products()
    return pd.DataFrame(products)[["id", "title", "category", "price"]].rename(
        columns={"id": "product_id", "title": "name"}
    )


@asset
def raw_orders() -> pd.DataFrame:
    carts = source.fetch_carts()
    rows = []
    for cart in carts:
        for item in cart["products"]:
            rows.append(
                {
                    "order_id": cart["id"],
                    "customer_id": cart["userId"],
                    "product_id": item["productId"],
                    "quantity": item["quantity"],
                    "order_date": cart["date"],
                }
            )
    return pd.DataFrame(rows)


@asset
def products_table(raw_products: pd.DataFrame) -> int:
    return db.load_table(raw_products, "products")


@asset
def orders_table(raw_orders: pd.DataFrame) -> int:
    return db.load_table(raw_orders, "orders")


@asset
def top_selling_products(
    raw_orders: pd.DataFrame, raw_products: pd.DataFrame
) -> pd.DataFrame:
    merged = raw_orders.merge(raw_products, on="product_id")
    return (
        merged.groupby(["product_id", "name"])["quantity"]
        .sum()
        .reset_index()
        .sort_values("quantity", ascending=False)
        .head(5)
    )


@asset_check(asset=raw_orders)
def orders_quantity_positive(raw_orders: pd.DataFrame) -> AssetCheckResult:
    bad_rows = raw_orders[raw_orders["quantity"] <= 0]
    return AssetCheckResult(
        passed=bad_rows.empty,
        metadata={"num_invalid_rows": len(bad_rows)},
    )

refresh_products_job = define_asset_job(name="refresh_products_job")

refresh_products_daily = ScheduleDefinition(
    name="refresh_products_daily",
    job=refresh_products_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[raw_products, raw_orders, products_table, orders_table,top_selling_products],
    asset_checks=[orders_quantity_positive],
    jobs=[refresh_products_job],
    schedules=[refresh_products_daily],
)
