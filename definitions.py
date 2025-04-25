import os
from pathlib import Path
import duckdb
import pandas as pd
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
import dagster as dg
import plotly.express as px
from dagster_dbt import get_asset_key_for_model
import plotly.express as px
from dagster import MetadataValue

duckdb_database_path = Path(__file__).parent / "dev.duckdb"

# @dg.asset(compute_kind="python")
# def raw_customers(context: dg.AssetExecutionContext) -> None:
#     # Pull customer data from a CSV
#     data = pd.read_csv("https://docs.dagster.io/assets/customers.csv")
#     connection = duckdb.connect(os.fspath(duckdb_database_path))

#     # Create a schema named raw
#     connection.execute("create schema if not exists raw")

#     # Create/replace table named raw_customers
#     connection.execute(
#         "create or replace table raw.raw_customers as select * from data"
#     )

#     # Log some metadata about the new table. It will show up in the UI.
#     context.add_output_metadata({"num_rows": data.shape[0]})



dbt_project_directory = Path(__file__).absolute().parent
dbt_project = DbtProject(project_dir=dbt_project_directory)
dbt_resource = DbtCliResource(project_dir=dbt_project)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@dg.asset(compute_kind="python")
def raw_customers(context: dg.AssetExecutionContext) -> None:
    data = pd.read_csv("data/raw_customers.csv")
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("create schema if not exists raw")
    connection.execute("create or replace table raw.raw_customers as select * from data")
    context.add_output_metadata({"num_rows": data.shape[0]})


@dg.asset(compute_kind="python")
def raw_orders(context: dg.AssetExecutionContext) -> None:
    data = pd.read_csv("data/raw_orders.csv")
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("create schema if not exists raw")
    connection.execute("create or replace table raw.raw_orders as select * from data")
    context.add_output_metadata({"num_rows": data.shape[0]})


@dg.asset(compute_kind="python")
def raw_payments(context: dg.AssetExecutionContext) -> None:
    data = pd.read_csv("data/raw_payments.csv")
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    connection.execute("create schema if not exists raw")
    connection.execute("create or replace table raw.raw_payments as select * from data")
    context.add_output_metadata({"num_rows": data.shape[0]})

@dg.asset(
    compute_kind="python",
    deps=[get_asset_key_for_model([dbt_models], "orders_by_day")],
)
def order_amount_histogram(context: dg.AssetExecutionContext):
    connection = duckdb.connect(os.fspath(duckdb_database_path))
    df = connection.sql("SELECT order_period, total_amount FROM orders_by_day").df()

    fig = px.histogram(df, x="order_period", y="total_amount", nbins=30)
    fig.update_layout(title="Order Amount Histogram by Date")

    # Save as HTML
    save_chart_path = Path(duckdb_database_path).parent / "order_amount_histogram.html"
    fig.write_html(save_chart_path, auto_open=False)

    # Add link to Dagster UI
    context.add_output_metadata({
        "plot_url": MetadataValue.url(f"file://{save_chart_path}")
    })

@dg.asset(deps=[get_asset_key_for_model([dbt_models], "orders_by_day")])
def debug_orders_by_date(context: dg.AssetExecutionContext):
    con = duckdb.connect(duckdb_database_path)
    df = con.sql("SELECT * FROM orders_by_day").df()
    context.log.info(df.to_string())

defs = dg.Definitions(
    assets=[raw_customers, raw_orders, raw_payments, dbt_models, order_amount_histogram,debug_orders_by_date],
    resources={"dbt": dbt_resource},
)

