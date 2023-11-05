from dagster import asset, AssetIn, Output

from ..resources.spark_io_manager import init_spark_session
from pyspark.sql import DataFrame

COMPUTE_KIND = "PySpark"
LAYER = "gold"


@asset()
def gold_stock_join_aggtrades():
    pass


@asset()
def gold_stock_join_transtrades():
    pass