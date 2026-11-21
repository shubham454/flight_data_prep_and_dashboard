# Silver layer - airports dimension (SCD Type 1)
# Source : claudecatalog.bronze.airports (Delta table, Change Data Feed enabled)
# Target : silver_airports in claudecatalog.silver_2 (pipeline schema)
#
# Load strategy:
#   1. airports_seed      - one-time AUTO CDC flow (once=True) that replays the full bronze
#                           snapshot as change events. Required because CDF was enabled at table
#                           version 1 and the pre-enablement history is beyond retention, so the
#                           initial data can only come from a plain snapshot read.
#   2. airports_cdc        - continuous AUTO CDC flow consuming the bronze Change Data Feed
#                           (inserts / update_postimage / delete events) from CDF_STARTING_VERSION.
#                           Every later commit (MERGE/INSERT/UPDATE/DELETE on bronze) is applied
#                           incrementally to the silver target.
#
# Full refresh vs incremental: normal updates are incremental (only new CDF events are merged);
# a pipeline full refresh (selected per-dataset or pipeline-wide) rebuilds the target from both
# flows from scratch.
#
# Both flows must agree on keys, SCD type, and the sequencing column type (long) so the one-time
# seed (seq=0) always sequences before live events (seq=_commit_version >= 2).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# CDF was enabled on 2026-11-21 at table version 1; start live CDC from there.
CDF_STARTING_VERSION = 1


@dp.temporary_view()
def bronze_airports_seed():
    """One-time seed source: full bronze snapshot as CDC inserts (sequence 0)."""
    return (
        spark.readStream.table("claudecatalog.bronze.airports")
        .select(
            F.lit(0).cast("long").alias("seq"),
            F.trim(F.col("airport_id")).alias("airport_id"),
            F.trim(F.col("airport_name")).alias("airport_name"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("country")).alias("country"),
        )
        .filter(F.col("airport_id").isNotNull())
    )


@dp.temporary_view()
def bronze_airports_cdc():
    """Live CDC source: bronze Change Data Feed stream (insert / update / delete events)."""
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .option("startingVersion", CDF_STARTING_VERSION)
        .table("claudecatalog.bronze.airports")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .select(
            F.col("_change_type").alias("change_type"),
            F.col("_commit_version").cast("long").alias("seq"),
            F.trim(F.col("airport_id")).alias("airport_id"),
            F.trim(F.col("airport_name")).alias("airport_name"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("country")).alias("country"),
        )
        .filter(F.col("airport_id").isNotNull())
    )


dp.create_streaming_table(
    name="silver_airports",
    comment=(
        "Airports dimension (SCD Type 1). One-time seed from the bronze snapshot plus "
        "incremental bronze Change Data Feed. Keys: airport_id."
    ),
)

# One-time full-snapshot seed (an AUTO CDC target only accepts AUTO CDC flows)
dp.create_auto_cdc_flow(
    target="silver_airports",
    source="bronze_airports_seed",
    keys=["airport_id"],
    sequence_by="seq",
    except_column_list=["seq"],
    stored_as_scd_type=1,
    name="airports_seed",
    once=True,
)

# Incremental CDC flow
dp.create_auto_cdc_flow(
    target="silver_airports",
    source="bronze_airports_cdc",
    keys=["airport_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("change_type = 'delete'"),
    except_column_list=["change_type", "seq"],
    stored_as_scd_type=1,
    name="airports_cdc",
)
