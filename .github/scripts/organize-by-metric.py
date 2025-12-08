#!/usr/bin/env python3
"""
Organize benchmark results by metric type.

Creates separate JSON files for each metric type with all queries.
"""

import json
import sys
import os


def main():
    if len(sys.argv) != 3:
        print("Usage: organize-by-metric.py <processed-dir> <output-dir>")
        sys.exit(1)

    processed_dir = sys.argv[1]
    output_dir = sys.argv[2]

    queries_file = os.path.join(processed_dir, "queries.txt")
    if not os.path.exists(queries_file):
        print(f"Error: {queries_file} not found")
        sys.exit(1)

    # Read all query files
    with open(queries_file, 'r') as f:
        queries = [line.strip() for line in f if line.strip()]

    # Organize by metric
    metrics = {}

    for query in queries:
        query_file = os.path.join(processed_dir, f"{query}.json")
        if not os.path.exists(query_file):
            print(f"Warning: {query_file} not found, skipping")
            continue

        with open(query_file, 'r') as f:
            benches = json.load(f)

        # Group by metric name
        for bench in benches:
            metric_name = bench['name']
            if metric_name not in metrics:
                metrics[metric_name] = []

            metrics[metric_name].append({
                'name': query,
                'value': bench['value'],
                'unit': bench['unit']
            })

    # Write metric files
    os.makedirs(output_dir, exist_ok=True)

    metric_files = {}
    for metric_name, benches in metrics.items():
        # Create safe filename
        safe_name = metric_name.lower().replace(' ', '-').replace('(', '').replace(')', '')
        output_file = os.path.join(output_dir, f"{safe_name}.json")

        with open(output_file, 'w') as f:
            json.dump(benches, f, indent=2)

        metric_files[metric_name] = safe_name

        print(f"{metric_name}: {len(benches)} queries -> {safe_name}.json")

    # Write metric list for workflow
    metrics_list_file = os.path.join(output_dir, "metrics.txt")
    with open(metrics_list_file, 'w') as f:
        for metric_name, safe_name in metric_files.items():
            f.write(f"{safe_name}|{metric_name}\n")

    print(f"\nCreated {len(metrics)} metric files in {output_dir}")


if __name__ == "__main__":
    main()
