from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date, upper, trim, concat, date_format
from pyspark.sql.types import DoubleType

@dp.view(name="bookings_transform")
def bookings_transform():
    return (
        spark.readStream.format("delta")
        .table("flights_project.bronze.bookings")
        .select(
            upper(trim(col("booking_id"))).alias("booking_id"),
            upper(trim(col("passenger_id"))).alias("passenger_id"),
            upper(trim(col("flight_id"))).alias("flight_id"),
            upper(trim(col("airport_id"))).alias("airport_id"),
            col("amount").cast(DoubleType()).alias("amount"),
            to_date(col("booking_date")).alias("booking_date"),
            concat(
                date_format(col("_bronze_ingested_at"), "yyyyMMddHHmmssSSS"),
                col("_source_file")
            ).alias("_seq_key")
        )
    )

dp.create_streaming_table(
    name="bookings_silver",
    schema="""
        booking_sk BIGINT GENERATED ALWAYS AS IDENTITY,
        booking_id STRING,
        passenger_id STRING,
        flight_id STRING,
        airport_id STRING,
        amount DOUBLE,
        booking_date DATE,
        _seq_key STRING
    """
)

dp.create_auto_cdc_flow(
  target = "flights_project.silver.bookings_silver",
  source = "bookings_transform",
  keys = ["booking_id"],
  sequence_by = col("_seq_key"),
  stored_as_scd_type = 1
)
