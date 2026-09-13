import boto3

BUCKET_NAME = "mrunali-dqe-project-20260913"

s3 = boto3.client("s3")

files_to_upload = ["source.json", "target.json", "consumed_orders.json"]

for filename in files_to_upload:
    s3.upload_file(filename, BUCKET_NAME, f"raw/{filename}")
    print(f"Uploaded {filename} to s3://{BUCKET_NAME}/raw/{filename}")

print("All files uploaded.")