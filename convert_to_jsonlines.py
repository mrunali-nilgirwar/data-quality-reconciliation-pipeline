import pandas as pd
import json

def convert_to_jsonlines(input_path, output_path):
    if input_path.endswith(".csv"):
        df = pd.read_csv(input_path)
        df.to_json(output_path, orient='records', lines=True)
        print(f"Converted {len(df)} rows from {input_path} to {output_path}")
        return
    elif input_path.endswith(".json"):
        with open(input_path) as f:
            records = json.load(f)
    else:
        raise ValueError(f"Unsupported file type: {input_path}")

    with open(output_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"Converted {len(records)} rows from {input_path} to {output_path}")

# Original project files
for filename in ["source.json", "target.json", "consumed_orders.json"]:
    convert_to_jsonlines(filename, filename.replace(".json", "_lines.json"))

# New airline project file
convert_to_jsonlines("source_flights.csv", "source_flights_lines.json")