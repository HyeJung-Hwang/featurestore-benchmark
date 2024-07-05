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
order = Entity(name="order", join_keys=["o_order_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
line_order_product_source = FileSource(
    name="line_order_product_source",
    path="/home/hjhwang/workspace/tpcx-ai-v1.0.3.1/data.parquet",
    timestamp_field="event_timestamp",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
line_order_product_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="line_order_product",
    entities=[order],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="scan_count", dtype=Int64),
    Field(name="scan_count_abs", dtype=Int64),
    Field(name="Monday", dtype=Float32),
    Field(name="Tuesday", dtype=Float32),
    Field(name="Wednesday", dtype=Float32),
    Field(name="Thursday", dtype=Float32),
    Field(name="Friday", dtype=Float32),
    Field(name="Saturday", dtype=Float32),
    Field(name="Sunday", dtype=Float32),
    Field(name="FINANCIAL SERVICES", dtype=Float32),
    Field(name="SHOES", dtype=Float32),
    Field(name="PERSONAL CARE", dtype=Float32),
    Field(name="PAINT AND ACCESSORIES", dtype=Float32),
    Field(name="DSD GROCERY", dtype=Float32),
    Field(name="MEAT - FRESH & FROZEN", dtype=Float32),
    Field(name="DAIRY", dtype=Float32),
    Field(name="PETS AND SUPPLIES", dtype=Float32),
    Field(name="HOUSEHOLD CHEMICALS/SUPP", dtype=Float32),
    Field(name="IMPULSE MERCHANDISE", dtype=Float32),
    Field(name="PRODUCE", dtype=Float32),
    Field(name="CANDY, TOBACCO, COOKIES", dtype=Float32),
    Field(name="GROCERY DRY GOODS", dtype=Float32),
    Field(name="BOYS WEAR", dtype=Float32),
    Field(name="FABRICS AND CRAFTS", dtype=Float32),
    Field(name="JEWELRY AND SUNGLASSES", dtype=Float32),
    Field(name="MENS WEAR", dtype=Float32),
    Field(name="ACCESSORIES", dtype=Float32),
    Field(name="HOME MANAGEMENT", dtype=Float32),
    Field(name="FROZEN FOODS", dtype=Float32),
    Field(name="SERVICE DELI", dtype=Float32),
    Field(name="INFANT CONSUMABLE HARDLINES", dtype=Float32),
    Field(name="PRE PACKED DELI", dtype=Float32),
    Field(name="COOK AND DINE", dtype=Float32),
    Field(name="PHARMACY OTC", dtype=Float32),
    Field(name="LADIESWEAR", dtype=Float32),
    Field(name="COMM BREAD", dtype=Float32),
    Field(name="BAKERY", dtype=Float32),
    Field(name="HOUSEHOLD PAPER GOODS", dtype=Float32),
    Field(name="CELEBRATION", dtype=Float32),
    Field(name="HARDWARE", dtype=Float32),
    Field(name="BEAUTY", dtype=Float32),
    Field(name="AUTOMOTIVE", dtype=Float32),
    Field(name="BOOKS AND MAGAZINES", dtype=Float32),
    Field(name="SEAFOOD", dtype=Float32),
    Field(name="OFFICE SUPPLIES", dtype=Float32),
    Field(name="LAWN AND GARDEN", dtype=Float32),
    Field(name="SHEER HOSIERY", dtype=Float32),
    Field(name="WIRELESS", dtype=Float32),
    Field(name="BEDDING", dtype=Float32),
    Field(name="BATH AND SHOWER", dtype=Float32),
    Field(name="HORTICULTURE AND ACCESS", dtype=Float32),
    Field(name="HOME DECOR", dtype=Float32),
    Field(name="TOYS", dtype=Float32),
    Field(name="INFANT APPAREL", dtype=Float32),
    Field(name="LADIES SOCKS", dtype=Float32),
    Field(name="PLUS AND MATERNITY", dtype=Float32),
    Field(name="ELECTRONICS", dtype=Float32),
    Field(name="GIRLS WEAR, 4-6X  AND 7-14", dtype=Float32),
    Field(name="BRAS & SHAPEWEAR", dtype=Float32),
    Field(name="LIQUOR,WINE,BEER", dtype=Float32),
    Field(name="SLEEPWEAR/FOUNDATIONS", dtype=Float32),
    Field(name="CAMERAS AND SUPPLIES", dtype=Float32),
    Field(name="SPORTING GOODS", dtype=Float32),
    Field(name="PLAYERS AND ELECTRONICS", dtype=Float32),
    Field(name="PHARMACY RX", dtype=Float32),
    Field(name="MENSWEAR", dtype=Float32),
    Field(name="OPTICAL - FRAMES", dtype=Float32),
    Field(name="SWIMWEAR/OUTERWEAR", dtype=Float32),
    Field(name="OTHER DEPARTMENTS", dtype=Float32),
    Field(name="MEDIA AND GAMING", dtype=Float32),
    Field(name="FURNITURE", dtype=Float32),
    Field(name="OPTICAL - LENSES", dtype=Float32),
    Field(name="SEASONAL", dtype=Float32),
    Field(name="LARGE HOUSEHOLD GOODS", dtype=Float32),
    Field(name="1-HR PHOTO", dtype=Float32),
    Field(name="CONCEPT STORES", dtype=Float32),
    Field(name="HEALTH AND BEAUTY AIDS", dtype=Float32),
    ],
    online=True,
    source=line_order_product_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "test"},
)







