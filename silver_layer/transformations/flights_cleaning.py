from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date, upper, trim, initcap, concat, date_format

@dp.view(
    name="flights_transform"
)
def flights_transform():
    return (
        spark.readStream.format("delta")
        .table("flights_project.bronze.flights")
        .select(
            upper(trim(col("flight_id"))).alias("flight_id"),
            initcap(trim(col("airline"))).alias("airline"),
            initcap(trim(col("origin"))).alias("origin"),
            initcap(trim(col("destination"))).alias("destination"),
            to_date(col("flight_date")).alias("flight_date"),
            concat(
                date_format(col("_bronze_ingested_at"), "yyyyMMddHHmmssSSS"),
                col("_source_file")
            ).alias("_seq_key")
        )
    )

dp.create_streaming_table(
    name="flights_silver",
    schema="""
        flight_sk BIGINT GENERATED ALWAYS AS IDENTITY,
        flight_id STRING,
        airline STRING,
        origin STRING,
        destination STRING,
        flight_date DATE,
        _seq_key STRING,
        __START_AT STRING,
        __END_AT STRING
    """
)

dp.create_auto_cdc_flow(
    target="flights_project.silver.flights_silver",
    source="flights_transform",
    keys=["flight_id"],
    sequence_by=col("_seq_key"),
    stored_as_scd_type=2
)