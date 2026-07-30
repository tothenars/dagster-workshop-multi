# pipeline_analytics

A cross-pipeline analytics layer for the Dagster workshop: it joins order,
product, FX, and ML-prediction data that three independent pipelines already
loaded into the shared warehouse, and produces one daily summary table of
predicted high-value orders with their EUR-converted totals.

Built on top of [dagster-workshop-multi](https://github.com/DanielAdif/dagster-workshop-multi),
a multi-container Dagster workshop — see that repo's README for the base
architecture (`pipeline_products`, `pipeline_fx`, `pipeline_ml`).

## What I built

- **Track:** B: cross-pipeline analytics
- **Data source:** no external API — reads the `orders`, `products`,
  `exchange_rates`, and `order_value_predictions` tables that
  `pipeline_products`, `pipeline_fx`, and `pipeline_ml` already write to the
  shared `warehouse_postgresql` database.
- **Key assets:**
  - `daily_order_summary` — joins orders with products to compute each
    order's USD total, converts it to EUR using the latest FX rate, and
    attaches the ML pipeline's high-value prediction and probability.
  - `daily_order_summary_table` — writes `daily_order_summary` to the
    warehouse as the `daily_order_summary` table.
- **Quality gate:** `daily_order_summary_has_no_missing_totals` fails if any
  row is missing a `total_eur` value (i.e. the EUR conversion silently
  dropped rows during the join). Zero tolerance was chosen because a missing
  total means the row is unusable for the report, not just lower quality.

## Architecture

```
                     dagster_webserver (:3000)  <-- workspace.yaml -->  dagster_daemon
                              |                                              |
                              +---------------------+-----------------------+
                                                     |
                             dagster_postgresql  (Dagster's own run/schedule/event storage)

  pipeline_products (:4000)          pipeline_fx (:4001)          pipeline_ml (:4002)
  fakestoreapi.com ->                api.frankfurter.app ->       trains a classifier on
  raw_products/raw_orders            raw_exchange_rates           products+orders, writes
        |                                  |                      predictions back
        v                                  v                            |
  products, orders  ------------->  warehouse_postgresql  <-------------+
  tables                            (also: exchange_rates,
                                      order_value_predictions)
                                             |
                                             v
                                  pipeline_analytics (:4003)
                                  reads orders, products, exchange_rates,
                                  and order_value_predictions -> joins them
                                  into daily_order_summary
```

`pipeline_analytics` is a fully independent container, same as the other
three: its own `Dockerfile`, its own `requirements.txt`, its own `db.py`. It
doesn't declare a Dagster-level dependency on the other pipelines — it just
reads their output tables straight from `warehouse_postgresql`, the same
landing-zone pattern `pipeline_fx`'s exercise ② uses.

## Running it

```bash
docker compose up --build
```

Open http://localhost:3000, find `pipeline_analytics` under Deployment >
Code Locations, and materialize its assets — after materializing
`pipeline_products`, `pipeline_fx`, and `pipeline_ml` at least once, since
`daily_order_summary` reads their output tables.

## Demo

<A screenshot or short GIF of the Dagster UI with your pipeline's assets
materialized — the asset graph view or the run log both work well.>

## What I'd do differently in production

This does a full `if_exists="replace"` load every run instead of an
incremental/upsert write, so `daily_order_summary` is rebuilt from scratch
each time rather than only processing new orders. There's also no retry
logic or alerting if an upstream table is temporarily unavailable — in
production I'd add a sensor or freshness check so `pipeline_analytics`
doesn't silently run against stale `order_value_predictions` if `pipeline_ml`
failed earlier in the week.