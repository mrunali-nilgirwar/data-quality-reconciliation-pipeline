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

