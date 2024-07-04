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
from feast.types import Float32, Float64, Int64, String

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
user = Entity(name="user", join_keys=["user_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
user_source = FileSource(
    name="user_source",
    path="/home/hjhwang/workspace/tpcx-ai-v1.0.3.1/user_source.parquet",
    timestamp_field="event_timestamp",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
user_product_rating_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="user_product_rating_fv",
    entities=[user],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="product_id", dtype=Int64),
        Field(name="rating", dtype=Int64),
        Field(name="user_age", dtype=Int64),
        Field(name="user_location", dtype=String),
    ],
    online=True,
    source=user_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "recsys"},
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="user_behavior_data",
    schema=[
        Field(name="recent_viewed_product_id", dtype=Int64),
        Field(name="current_location", dtype=String),
        Field(name="current_time", dtype=String),
        Field(name="cart_size", dtype=Int64),
    ],
)
# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[user_product_rating_fv, input_request],
    schema=[
        Field(name="recent_viewed_product_id", dtype=Int64),
        Field(name="cart_size", dtype=Int64),
    ],
)
def user_recent_features(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()

    df["recent_viewed_product_id"] = inputs["recent_viewed_product_id"]

    # df["time_of_day"] = pd.to_datetime(inputs["current_time"]).dt.time
    df["cart_size"] = inputs["cart_size"]

    return df


# This groups features into a model version
user_recent_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        user_product_rating_fv[["product_id","rating"]],  # Sub-selects a feature from a feature view
        user_recent_features,  # Selects all features from the feature view
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path="/home/hjhwang/workspace/tpcx-ai-v1.0.3.1/feast_uc07/feature_repo/data")
    ),
)
driver_recent_activity_v2 = FeatureService(
    name="driver_activity_v2", features=[user_product_rating_fv ,  user_recent_features]
)

# Defines a way to push data (to be available offline, online or both) into Feast.
user_push_source = PushSource(
    name="user_push_source",
    batch_source=user_source,
)

