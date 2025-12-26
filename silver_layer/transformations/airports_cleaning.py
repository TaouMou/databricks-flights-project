from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date, upper, trim, initcap, date_format
from pyspark.sql.types import DoubleType


from pyspark.sql.functions import concat, date_format


@dp.view(
    name="airports_transform"
)
def airports_transform():
    return (
        spark.readStream.format("delta")
        .table("flights_project.bronze.airports")
        .select(
            upper(trim(col("airport_id"))).alias("airport_id"),
            initcap(trim(col("airport_name"))).alias("airport_name"),
            initcap(trim(col("city"))).alias("city"),
            initcap(trim(col("country"))).alias("country"),
            concat(
                date_format(col("_bronze_ingested_at"), "yyyyMMddHHmmssSSS"),
                col("_source_file")
            ).alias("_seq_key")
        )
    )


dp.create_streaming_table(
    name="airports_silver",
    schema="""
        airport_sk BIGINT GENERATED ALWAYS AS IDENTITY,
        airport_id STRING,
        airport_name STRING,
        city STRING,
        country STRING,
        _seq_key STRING,
        __START_AT STRING,
        __END_AT STRING
    """
)


dp.create_auto_cdc_flow(
    target="flights_project.silver.airports_silver",
    source="airports_transform",
    keys=["airport_id"],
    sequence_by=col("_seq_key"),
    stored_as_scd_type=2
)