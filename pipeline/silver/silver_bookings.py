# Silver layer - bookings fact (SCD Type 1)
# Source : claudecatalog.bronze.bookings (Delta table, Change Data Feed enabled)
# Target : silver_bookings in claudecatalog.silver_2 (pipeline schema)
#
# Load strategy: one-time AUTO CDC seed of the full snapshot + continuous AUTO CDC flow
# on the bronze Change Data Feed (see silver_airports.py for the rationale).
#
# Type conversion and null handling:
#   - rows without a booking_id cannot be merged and are dropped in the source views
#   - amount     DOUBLE -> DECIMAL(10,2) (invalid values become NULL, row is kept)
#   - booking_date STRING 'yyyy-MM-dd' -> DATE (invalid values become NULL, row is kept)
#   - foreign keys (passenger_id, airport_id) may be NULL; the gold view left-joins
#     dimensions and coalesces missing attributes so reports never break
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CDF_STARTING_VERSION = 1


def _clean_bookings(df, with_change_columns):
    """Shared cleaning for the seed and CDC sources: one select, all expressions."""
    base_cols = [
        F.trim(F.col("booking_id")).alias("booking_id"),
        F.trim(F.col("passenger_id")).alias("passenger_id"),
        F.trim(F.col("flight_id")).alias("flight_id"),
        F.trim(F.col("airport_id")).alias("airport_id"),
        F.col("amount").cast("decimal(10,2)").alias("amount"),
        F.to_date(F.col("booking_date"), "yyyy-MM-dd").alias("booking_date"),
    ]
    if with_change_columns:
        cols = [F.col("_change_type").alias("change_type"),
                F.col("_commit_version").cast("long").alias("seq")] + base_cols
    else:
        cols = [F.lit(0).cast("long").alias("seq")] + base_cols
    return df.select(*cols).filter(F.col("booking_id").isNotNull())


@dp.temporary_view()
def bronze_bookings_seed():
    """One-time seed source: full bronze snapshot as CDC inserts (sequence 0)."""
    return _clean_bookings(spark.readStream.table("claudecatalog.bronze.bookings"), False)


@dp.temporary_view()
def bronze_bookings_cdc():
    """Live CDC source: bronze Change Data Feed stream (insert / update / delete events)."""
    cdf = (
        spark.readStream
        .option("readChangeFeed", "true")
        .option("startingVersion", CDF_STARTING_VERSION)
        .table("claudecatalog.bronze.bookings")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    )
    return _clean_bookings(cdf, True)


dp.create_streaming_table(
    name="silver_bookings",
    comment=(
        "Bookings fact (SCD Type 1). One-time seed from the bronze snapshot plus incremental "
        "bronze Change Data Feed. Keys: booking_id. amount DECIMAL(10,2), booking_date DATE."
    ),
    cluster_by=["booking_date"],
)

# One-time full-snapshot seed (an AUTO CDC target only accepts AUTO CDC flows)
dp.create_auto_cdc_flow(
    target="silver_bookings",
    source="bronze_bookings_seed",
    keys=["booking_id"],
    sequence_by="seq",
    except_column_list=["seq"],
    stored_as_scd_type=1,
    name="bookings_seed",
    once=True,
)

# Incremental CDC flow
dp.create_auto_cdc_flow(
    target="silver_bookings",
    source="bronze_bookings_cdc",
    keys=["booking_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("change_type = 'delete'"),
    except_column_list=["change_type", "seq"],
    stored_as_scd_type=1,
    name="bookings_cdc",
)
