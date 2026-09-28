\# Data Quality Reconciliation Pipeline



A small end-to-end pipeline built to practice data quality validation and reconciliation — the same core skill (finding missing, mismatched, and duplicate records between two datasets) used at production scale in real data engineering pipelines.



\## Architecture



Python data generator → Kafka (Docker) → S3 → Glue Data Catalog → Athena (SQL validation)



1\. \*\*Data generation\*\* (`source.json`, `target.json`) — synthetic order data (20 orders). `target.json` has deliberately injected issues: missing rows, mismatched values, and duplicates, so the validation logic can be tested against known failure cases.

2\. \*\*Kafka\*\* (`producer.py`, `consumer.py`) — source data is streamed through a Kafka topic (`orders-topic`), running locally via Docker, to simulate a continuous data flow rather than a static file drop. Consumer output saved to `consumed\_orders.json`.

3\. \*\*JSON Lines conversion\*\* (`convert\_to\_jsonlines.py`) — converts the JSON array files into newline-delimited JSON, since Athena/Glue parse files line-by-line and can't efficiently split a single JSON array.

4\. \*\*S3 + Glue + Athena\*\* — files are uploaded to S3 (`upload\_to\_s3.py`), one subfolder per file type (`raw/source/`, `raw/target/`, `raw/consumed/`). Glue tables are defined with explicit `CREATE TABLE` DDL rather than relying on crawler auto-detection (see note below), then queried via Athena for SQL-based reconciliation.



\## A real debugging story



Initially used a Glue crawler pointed at a single flat S3 folder containing all three file types. The crawler couldn't reconcile the differing schemas across files and collapsed the table into a single generic `array`-typed column instead of parsing individual fields. Fixed by separating each file type into its own S3 subfolder and writing explicit `CREATE TABLE` DDL with defined columns/types instead of depending on auto-inference.



\## Reconciliation logic



Implemented in SQL (Athena), covering:

\- \*\*Missing records\*\* — `LEFT JOIN` / `NOT IN`

\- \*\*Duplicates\*\* — `GROUP BY ... HAVING COUNT(\*) > 1`, and `ROW\_NUMBER() OVER (PARTITION BY ...)`

\- \*\*Mismatched values\*\* — `JOIN` on key + `CASE WHEN` to label each row MATCHED / MISMATCHED / MISSING



\## Status / next steps



\- Kafka producer/consumer pipeline, verified end-to-end

\- S3 → Glue → Athena layer, verified queryable with correct schema

\- SQL reconciliation logic

\- Next: PySpark reconciliation job on Databricks, to demonstrate the same logic at scale using distributed processing

## Part 2: Real-World Data Validation — Airline On-Time Performance

Extended this project with real government data to practice validation on messier, real-world data rather than only synthetic examples.

**Data source:** US DOT Bureau of Transportation Statistics, 2015 Flight Delays and Cancellations (via Kaggle: `usdot/flight-delays`), filtered to 5 major carriers (WN, DL, AA, UA, F9) and June 2015 specifically — chosen based on comparing null-value rates across several candidate months, rather than an arbitrary pick. Final dataset: 285,368 flights.

**Problem being solved:** raw flight performance data needs to be validated for schema consistency, duplicates, and completeness before it can be trusted for downstream reporting (e.g., on-time performance rates by carrier).

### Real findings during this work

1. **Schema drift:** October 2015 records in the full dataset use numeric `AIRPORT_ID` codes instead of the standard 3-letter IATA codes used in every other month — a real, verified example of a mid-year upstream schema change that would silently break joins against a standard airport reference table. Excluded from the main pipeline (which uses the clean June data) and documented separately, with a validation query that would catch it (`LENGTH(origin_airport) != 3`, grouped by month).
2. **False duplicate alarm:** an initial duplicate check (same date + airline + flight number) flagged 59% of rows as "duplicates." Investigation showed the same flight number often operates round-trip legs on the same day (e.g., AA 23 flying LAX→LAS and LAS→LAX). Fixed by including origin and destination airport in the uniqueness definition, which correctly showed zero true duplicates.
3. **NaN vs. null in JSON:** an initial JSON Lines conversion using `json.dumps()` produced invalid JSON (literal `NaN` instead of `null`) for missing numeric values. Fixed by switching to pandas' `to_json(orient='records', lines=True)`, which outputs proper `null` values.
4. **Verified expected behavior (not a bug):** delay-reason columns (weather, airline, air system, security, late aircraft) are null for exactly 217,877 rows each. Confirmed these correspond to flights with little or no arrival delay, so the nulls reflect business logic (no delay reason needed), not missing data.

### Pipeline

pandas (local profiling) → JSON Lines → S3 (`flights_raw/` prefix) → Glue (`flights_source_raw` table, explicit DDL) → Athena (validation queries) → **Redshift Serverless** (COPY load + reconciliation)

The same `convert_to_jsonlines.py` script from Part 1 was refactored into one function handling both JSON and CSV input, rather than duplicating logic across two scripts.

### Redshift load

Loaded the S3 data into Redshift Serverless using `COPY ... FORMAT AS JSON 'auto ignorecase'` (see `redshift_flights_validation.sql`).

- **Cost and security controls:** base and max capacity capped at 8 RPUs; IAM role scoped to the single project bucket rather than all S3.
- **Type choice:** nullable numeric columns use `DOUBLE PRECISION`, since pandas stores any column containing missing values as floats (e.g. `2354.0`), which Redshift rejects for `INTEGER`.
- **Cross-region error:** the first COPY failed with a `301 PermanentRedirect` because the bucket (us-east-2) and the workgroup (us-east-1) were in different regions. Fixed by adding the `REGION` parameter. In production, co-locating storage and compute avoids this and the cross-region transfer cost.

### Cross-system reconciliation

| Check | pandas | Athena | Redshift |
|---|---|---|---|
| Row count | 285,368 | 285,368 | 285,368 |
| Non-standard airport codes | 0 | 0 | 0 |
| True duplicates (origin/destination-aware key) | 0 | 0 | 0 |
| Delay-reason nulls (per column) | 217,877 | — | 217,877 |

All checks agree across the three systems, confirming no data was lost, duplicated, or altered in transit, and that nulls were preserved correctly end to end.

