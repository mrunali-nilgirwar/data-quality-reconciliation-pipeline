# Data Quality Reconciliation Pipeline

A small end-to-end pipeline built to practice data validation and reconciliation between two datasets.

## Architecture



Python data generator → Kafka (Docker) → S3 → Glue Data Catalog → Athena (SQL validation)



1. **Data generation** (`source.json`, `target.json`) — synthetic order data (20 orders). `target.json` has deliberately injected issues: missing rows, mismatched values, and duplicates, so the validation logic can be tested against known failure cases.
2. **Kafka** (`producer.py`, `consumer.py`) — source data is streamed through a Kafka topic (`orders-topic`), running locally via Docker, to simulate a continuous data flow rather than a static file drop. Consumer output saved to `consumed_orders.json`.
3. **JSON Lines conversion** (`convert_to_jsonlines.py`) — converts the JSON array files into newline-delimited JSON, since Athena/Glue parse files line-by-line and can't efficiently split a single JSON array.
4. **S3 + Glue + Athena** — files are uploaded to S3 (`upload_to_s3.py`), one subfolder per file type (`raw/source/`, `raw/target/`, `raw/consumed/`). Glue tables are defined with explicit `CREATE TABLE` DDL rather than relying on crawler auto-detection (see note below), then queried via Athena for SQL-based reconciliation.
5. **PySpark on Databricks** (`pyspark_reconciliation.py`) — the same reconciliation logic reimplemented in PySpark, reading the real project data directly from S3 (via boto3), to demonstrate the same checks running in a distributed-processing tool rather than a single SQL query.

## A real debugging story

Initially used a Glue crawler pointed at a single flat S3 folder containing all three file types. The crawler couldn't reconcile the differing schemas across files and collapsed the table into a single generic `array`-typed column instead of parsing individual fields. Fixed by separating each file type into its own S3 subfolder and writing explicit `CREATE TABLE` DDL with defined columns/types instead of depending on auto-inference.

## Reconciliation logic

Implemented in both SQL (Athena) and PySpark (Databricks), covering:
- **Missing records** — SQL: `LEFT JOIN` / `NOT IN`. PySpark: `left_anti` join.
- **Duplicates** — SQL: `GROUP BY ... HAVING COUNT(*) > 1`, or `ROW_NUMBER() OVER (PARTITION BY ...)`. PySpark: `groupBy().count().filter()`.
- **Mismatched values** — SQL: `JOIN` + `CASE WHEN`. PySpark: `join` + `withColumn` + `F.when().otherwise()`.

### Real findings from running the PySpark checks against the actual project data

Running the reconciliation logic against the real `source`/`target` data in S3 surfaced a useful lesson: the raw row count difference (20 vs. 19) looked like only one record was off, but the actual breakdown was:
- **3 missing records** (order_id 3, 10, 16)
- **2 duplicate records** (order_id 11, 14)
- **3 mismatched values** (order_id 7, 9, 18)

The missing and duplicate counts happened to roughly cancel out in the total row count, which is a concrete example of why row-count comparisons alone aren't sufficient for data quality checks — missing, duplicate, and mismatched records need to be checked independently.

## Status

- Kafka producer/consumer pipeline, verified end-to-end
- S3 → Glue → Athena layer, verified queryable with correct schema
- SQL reconciliation logic (Athena)
- PySpark reconciliation logic (Databricks), run against real project data with findings documented above
