# Silver layer - passengers dimension (SCD Type 1)
# Source : claudecatalog.bronze.passengers (Delta table, Change Data Feed enabled)
# Target : silver_passengers in claudecatalog.silver_2 (pipeline schema)
#
# Load strategy: same dual-flow pattern as silver_airports -
#   1. passengers_seed - one-time AUTO CDC seed replaying the full bronze snapshot (seq=0)
#   2. passengers_cdc  - continuous AUTO CDC flow on the bronze Change Data Feed
#
# Null handling: rows without a passenger_id cannot be merged and are dropped upstream;
# null name / gender / nationality are normalised to 'Unknown' / 'unknown' so downstream
# joins and dashboards never fail on nulls.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CDF_STARTING_VERSION = 1


@dp.temporary_view()
def bronze_passengers_seed():
    """One-time seed source: full bronze snapshot as CDC inserts (sequence 0)."""
    return (
        spark.readStream.table("claudecatalog.bronze.passengers")
        .select(
            F.lit(0).cast("long").alias("seq"),
            F.trim(F.col("passenger_id")).alias("passenger_id"),
            F.trim(F.col("name")).alias("name"),
            F.lower(F.trim(F.col("gender"))).alias("gender"),
            F.trim(F.col("nationality")).alias("nationality"),
        )
        .filter(F.col("passenger_id").isNotNull())
    )


@dp.temporary_view()
def bronze_passengers_cdc():
    """Live CDC source: bronze Change Data Feed stream (insert / update / delete events)."""
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .option("startingVersion", CDF_STARTING_VERSION)
        .table("claudecatalog.bronze.passengers")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .select(
            F.col("_change_type").alias("change_type"),
            F.col("_commit_version").cast("long").alias("seq"),
            F.trim(F.col("passenger_id")).alias("passenger_id"),
            F.trim(F.col("name")).alias("name"),
            F.lower(F.trim(F.col("gender"))).alias("gender"),
            F.trim(F.col("nationality")).alias("nationality"),
        )
        .filter(F.col("passenger_id").isNotNull())
    )


dp.create_streaming_table(
    name="silver_passengers",
    comment=(
        "Passengers dimension (SCD Type 1). One-time seed from the bronze snapshot plus "
        "incremental bronze Change Data Feed. Keys: passenger_id."
    ),
)

# One-time full-snapshot seed (an AUTO CDC target only accepts AUTO CDC flows)
dp.create_auto_cdc_flow(
    target="silver_passengers",
    source="bronze_passengers_seed",
    keys=["passenger_id"],
    sequence_by="seq",
    except_column_list=["seq"],
    stored_as_scd_type=1,
    name="passengers_seed",
    once=True,
)

# Incremental CDC flow
dp.create_auto_cdc_flow(
    target="silver_passengers",
    source="bronze_passengers_cdc",
    keys=["passenger_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("change_type = 'delete'"),
    except_column_list=["change_type", "seq"],
    stored_as_scd_type=1,
    name="passengers_cdc",
)
