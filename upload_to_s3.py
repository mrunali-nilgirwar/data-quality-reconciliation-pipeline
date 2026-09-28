import boto3

BUCKET_NAME = "mrunali-dqe-project-20260913"
s3 = boto3.client("s3")

def upload_file(local_filename, s3_key):
    s3.upload_file(local_filename, BUCKET_NAME, s3_key)
    print(f"Uploaded {local_filename} to s3://{BUCKET_NAME}/{s3_key}")

# Original reconciliation project (matches the subfolder structure we already fixed)
upload_file("source_lines.json", "raw/source/source_lines.json")
upload_file("target_lines.json", "raw/target/target_lines.json")
upload_file("consumed_orders_lines.json", "raw/consumed/consumed_orders_lines.json")

# New airline flights project — separate top-level prefix, own namespace
upload_file("source_flights_lines.json", "flights_raw/source_flights_lines.json")