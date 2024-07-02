# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String, UnixTimestamp

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
product = Entity(name="product", join_keys=["product_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
initem_order_source = FileSource(
    name="line_order_stats_source",
    path="/home/hjhwang/workspace/tpcx-ai-v1.0.3.1/feature_ineitem_order.parquet",
    timestamp_field="date",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
initem_order_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="initem_order",
    entities=[product],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="o_order_id", dtype=Int64),
        Field(name="o_customer_sk", dtype=Int64),
        Field(name="weekday", dtype=String),
        Field(name="date", dtype=UnixTimestamp),
        Field(name="store", dtype=Int64),
        Field(name="trip_type", dtype=Int64),
        Field(name="li_order_id", dtype=Int64),
        Field(name="product_id", dtype=Int64),
        Field(name="quantity", dtype=Int64),
        Field(name="price", dtype=Float64),
    ],
    online=True,
    source=initem_order_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "test"},
)







