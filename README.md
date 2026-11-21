# flight_data_prep_and_dashboard

Bronze → Silver (silver_2) → Gold ETL with CDC validation, insights dashboard, and a scheduled job.

- Lakeflow pipeline: `flight_data_prep_silver_pipeline` (triggered, incremental by default)
- Lakeflow job: `flight_data_prep_daily_silver_update` (daily, runs the pipeline)

## Architecture

```
claudecatalog.bronze (bookings, passengers, airports; Delta + Change Data Feed)
        │
        │  Lakeflow Spark Declarative Pipeline (triggered, incremental by default)
        │    - one-time AUTO CDC seed  : full bronze snapshot replay (once=True)
        │    - live AUTO CDC flow      : bronze Change Data Feed events, SCD Type 1
        │      (insert / update_postimage / delete → upsert / delete in silver)
        ▼
claudecatalog.silver_2
    - silver_airports      (SCD1, key airport_id)
    - silver_passengers     (SCD1, key passenger_id)
    - silver_bookings       (SCD1, key booking_id; amount DECIMAL(10,2), booking_date DATE)
    - gold_booking_insights (MV: bookings LEFT JOIN airports + passengers, null-safe)
        │
        ├── Booking Insights Dashboard (.lvdash.json in this folder)
        └── Lakeflow Job (daily schedule, pipeline task)
```

## Folder layout

- `pipeline/silver/` — silver dataset definitions (one file per dataset)
- `pipeline/gold/` — gold insight view
- `01_generate_test_bronze_records` — idempotent test data inserts into bronze (CDC test)
- `02_validate_cdc_flow` — reconciliation: bronze vs silver counts + new-record checks
- `Booking Insights Dashboard.lvdash.json` — AI/BI dashboard over silver_2

## Incremental vs full refresh

- **Incremental (default)**: each pipeline update merges only new bronze Change Data Feed
  events (commits since the last update) into silver.
- **Full refresh**: select one dataset or the whole pipeline in the pipeline UI / job
  (or `sparkDeclarativePipelines_startPipelineUpdate` with `fullRefresh: true`) to rebuild
  the targets from scratch — the seed flow replays the snapshot and the CDC flow re-reads
  the change feed from `CDF_STARTING_VERSION`.

## CDC validation flow

1. Run `01_generate_test_bronze_records` (MERGE-inserts new airports/passengers/bookings
   into bronze; safe to re-run).
2. Run the pipeline update (job, UI, or `02_validate_cdc_flow` guidance).
3. Run `02_validate_cdc_flow` — expects bronze row counts to match silver and the new
   booking IDs to be present in `gold_booking_insights`.

## Notes

- Change Data Feed was enabled on the bronze tables on 2026-11-21 (table version 1);
  `delta.enableChangeDataFeed=true` can be unset to revert (metadata-only change).
- Bronze history before version 1 is beyond file retention, so the initial load uses a
  one-time snapshot seed; live changes come from the change feed.
