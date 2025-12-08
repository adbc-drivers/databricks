#!/usr/bin/env python3
"""
Create a combined benchmark JSON file from individual query files.

This combines all queries into a single file that benchmark-action can process,
with each query's metrics as separate benchmark entries.
"""

import json
import sys
import os


def main():
    if len(sys.argv) != 3:
        print("Usage: create-combined-benchmark.py <processed-dir> <output-file>")
        sys.exit(1)

    processed_dir = sys.argv[1]
    output_file = sys.argv[2]

    queries_file = os.path.join(processed_dir, "queries.txt")
    if not os.path.exists(queries_file):
        print(f"Error: {queries_file} not found")
        sys.exit(1)

    # Read all query files and combine
    all_benches = []

    with open(queries_file, 'r') as f:
        queries = [line.strip() for line in f if line.strip()]

    for query in queries:
        query_file = os.path.join(processed_dir, f"{query}.json")
        if not os.path.exists(query_file):
            print(f"Warning: {query_file} not found, skipping")
            continue

        with open(query_file, 'r') as f:
            benches = json.load(f)

        # Prefix each benchmark name with the query name
        for bench in benches:
            bench['name'] = f"{query} - {bench['name']}"
            all_benches.append(bench)

    # Write combined file
    with open(output_file, 'w') as f:
        json.dump(all_benches, f, indent=2)

    print(f"Combined {len(queries)} queries into {output_file}")
    print(f"Total benchmarks: {len(all_benches)}")


if __name__ == "__main__":
    main()
