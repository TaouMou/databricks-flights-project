from pyspark import pipelines as dp
from pyspark.sql.functions import col, upper, trim, initcap, concat, date_format, to_date

@dp.view(
    name="passengers_transform"
)
def passengers_transform():
    return (
        spark.readStream.format("delta")
        .table("flights_project.bronze.passengers")
        .select(
            upper(trim(col("passenger_id"))).alias("passenger_id"),
            initcap(trim(col("name"))).alias("name"),
            initcap(trim(col("gender"))).alias("gender"),
            initcap(trim(col("nationality"))).alias("nationality"),
            concat(
                date_format(col("_bronze_ingested_at"), "yyyyMMddHHmmssSSS"),
                col("_source_file")
            ).alias("_seq_key")
        )
    )

dp.create_streaming_table(
    name="passengers_silver",
    schema="""
        passenger_sk BIGINT GENERATED ALWAYS AS IDENTITY,
        passenger_id STRING,
        name STRING,
        gender STRING,
        nationality STRING,
        _seq_key STRING,
        __START_AT STRING,
        __END_AT STRING
    """
)

dp.create_auto_cdc_flow(
    target="flights_project.silver.passengers_silver",
    source="passengers_transform",
    keys=["passenger_id"],
    sequence_by=col("_seq_key"),
    stored_as_scd_type=2
)