import json

files = ["source.json", "target.json", "consumed_orders.json"]

for filename in files:
    with open(filename) as f:
        data = json.load(f)

    output_filename = filename.replace(".json", "_lines.json")
    with open(output_filename, "w") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")

    print(f"Converted {filename} -> {output_filename}")