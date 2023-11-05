from dagster import asset, AssetIn, Output
import polars as pl

from pyspark.sql import DataFrame

from ..resources.spark_io_manager import init_spark_session
from pyspark.sql.functions import *

COMPUTE_KIND = "PySpark"
LAYER = "silver"

@asset(
        description="",
        ins={
            "bronze_stocks": AssetIn(
                key_prefix=["bronze", "stock"]
            ),
        },
        io_manager_key="spark_io_manager",
        key_prefix=["silver", "stock"],
        compute_kind=COMPUTE_KIND,
        group_name=LAYER
)
def silver_cleaned_stocks(context, bronze_stocks: pl.DataFrame) -> Output[DataFrame]:
    """
        Load stock table from bronze layer in MinIO, into a Spark dataframe, then clean data
    """

    context.log.debug("Start creating spark session")

    with init_spark_session() as spark:
        # Convert bronze_trade from polars DataFrame to Spark DataFrame
        pandas_df = bronze_stocks.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )

        spark_df = spark.createDataFrame(pandas_df)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        spark.sql("CREATE SCHEMA IF NOT EXISTS hive_prod.silver")
        spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_stocks",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )

@asset(
        description="",
        ins={
            "bronze_trades": AssetIn(
                key_prefix=["bronze", "trade"]
            ),
        },
        io_manager_key="spark_io_manager",
        key_prefix=["silver", "trade"],
        compute_kind=COMPUTE_KIND,
        group_name=LAYER
)
def silver_cleaned_trades(context, bronze_trades: pl.DataFrame) -> Output[DataFrame]:
    """
        Load trade table from bronze layer in MinIO, into a Spark dataframe, then clean data
    """

    context.log.debug("Start creating spark session")

    with init_spark_session() as spark:
        # Convert bronze_trade from polars DataFrame to Spark DataFrame
        pandas_df = bronze_trades.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )

        spark_df = spark.createDataFrame(pandas_df)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")
        spark_df = spark_df.withColumnRenamed("VWAP", "Volume_Weight_Avg_Price") \
            .withColumnRenamed("Open", "Open_Price") \
            .withColumnRenamed("High", "High_Price") \
            .withColumnRenamed("Low", "Low_Price") \
            .withColumnRenamed("Last", "Last_Price") \
            .withColumnRenamed("Close", "Close_Price") \
            .withColumn("Date", date_format("Date", "yyyy-MM-dd")) \
            .drop('ID')
        spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_trades",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
    
@asset(
        description="Transformation trades",
        ins={
            "silver_cleaned_trades": AssetIn(
                key_prefix=["silver", "trade"]
            ),
        },
        io_manager_key="spark_io_manager",
        key_prefix=["silver", "trade"],
        compute_kind=COMPUTE_KIND,
        group_name=LAYER
)
def silver_transformation_trades(context, silver_cleaned_trades: DataFrame) -> Output[DataFrame]:
    spark_df = silver_cleaned_trades
    context.log.debug("Caching spark_df ...")
    spark_df.cache()
    spark_df = spark_df.withColumn("Price_Change", col('Last_Price') - col('Prev_Close')) \
        .withColumn("Percent_Change", bround((col('Price_Change') / col('Last_Price'))*100, 2)) 
    spark_df.unpersist()

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_transformation_trades",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )

@asset(
        description="Aggregate trades",
        ins={
            "silver_cleaned_trades": AssetIn(
                key_prefix=["silver", "trade"]
            ),
        },
        io_manager_key="spark_io_manager",
        key_prefix=["silver", "trade"],
        compute_kind=COMPUTE_KIND,
        group_name=LAYER
)
def silver_agg_trades(context, silver_cleaned_trades: DataFrame) -> Output[DataFrame]:
    spark_df = silver_cleaned_trades
    context.log.debug("Caching spark_df ...")
    spark_df.cache()
    spark_df = spark_df.groupBy('Symbol') \
        .agg(
            min('Date').alias('Start'),
            max('Date').alias('End'),

            min('Open_Price').alias('Min_Open'),
            max('Open_Price').alias('Max_Open'),
            avg('Open_Price').alias('Avg_Open'),

            min('High_Price').alias('Min_High'),
            max('High_Price').alias('Max_High'),
            avg('High_Price').alias('Avg_High'),

            min('Low_Price').alias('Min_Low'),
            max('Low_Price').alias('Max_Low'),
            avg('Low_Price').alias('Avg_Low'),

            min('Last_Price').alias('Min_Last'),
            max('Last_Price').alias('Max_Last'),
            avg('Last_Price').alias('Avg_Last'),

            min('Close_Price').alias('Min_Close'),
            max('Close_Price').alias('Max_Close'),
            avg('Close_Price').alias('Avg_Close'),

            sum('Volume').alias('Sum_Volume')
        )
    
    return Output(
        value=spark_df,
        metadata={
            "table": "silver_agg_trades",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )