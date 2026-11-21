# Gold layer - enriched booking insights (insight layer for the dashboard)
# Sources : pipeline datasets silver_bookings / silver_airports / silver_passengers
#           (published in claudecatalog.silver_2)
# Target  : gold_booking_insights in claudecatalog.silver_2
#
# Materialized view (batch read) that left-joins the bookings fact with the airport and
# passenger dimensions and normalises missing dimension attributes, so every dashboard
# aggregate works even when a booking references an unknown airport or passenger.
#
# Refresh: incremental on every pipeline update; full refresh rebuilds it from the silver tables.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_booking_insights",
    comment=(
        "Enriched booking fact: bookings + airport attributes + passenger attributes. "
        "Drives the Booking Insights dashboard (revenue, trends, top airports/passengers)."
    ),
)
def gold_booking_insights():
    bookings = spark.read.table("silver_bookings")
    airports = spark.read.table("silver_airports")
    passengers = spark.read.table("silver_passengers")

    return (
        bookings
        .join(airports, bookings["airport_id"] == airports["airport_id"], "left")
        .join(passengers, bookings["passenger_id"] == passengers["passenger_id"], "left")
        .select(
            bookings["booking_id"],
            bookings["flight_id"],
            bookings["booking_date"],
            bookings["amount"],
            bookings["passenger_id"],
            F.coalesce(passengers["name"], F.lit("Unknown")).alias("passenger_name"),
            F.coalesce(passengers["gender"], F.lit("unknown")).alias("gender"),
            F.coalesce(passengers["nationality"], F.lit("Unknown")).alias("nationality"),
            bookings["airport_id"],
            F.coalesce(airports["airport_name"], F.lit("Unknown Airport")).alias("airport_name"),
            F.coalesce(airports["city"], F.lit("Unknown City")).alias("city"),
            F.coalesce(airports["country"], F.lit("Unknown Country")).alias("country"),
        )
    )
