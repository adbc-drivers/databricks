#!/usr/bin/env python3
"""
Process BenchmarkDotNet results and format for GitHub Pages visualization.

Extracts metrics for each benchmark query:
- Mean Execution Time (ms)
- Peak Memory (MB) - from custom column
- Allocated Memory (MB)
- Gen2 Collections

Creates separate data files for each query to enable dropdown selection.
"""

import json
import sys
import os
import csv
from pathlib import Path


def load_peak_memory_from_csv(csv_path):
    """Load Peak Memory values from CSV file."""
    peak_memory_map = {}

    if not os.path.exists(csv_path):
        return peak_memory_map

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_name = row.get('benchmarkQuery', 'unknown')
            peak_memory_str = row.get('Peak Memory (MB)', '')

            if peak_memory_str:
                try:
                    peak_memory_map[query_name] = float(peak_memory_str)
                except ValueError:
                    pass

    return peak_memory_map


def extract_peak_memory(benchmark, peak_memory_map):
    """Extract Peak Memory from CSV data."""
    query_name = extract_query_name(benchmark)
    return peak_memory_map.get(query_name)


def extract_query_name(benchmark):
    """Extract query name from benchmark parameters."""
    params = benchmark.get("Parameters", "")

    # Parameters can be a string like "ReadDelayMs=5&benchmarkQuery=catalog_sales"
    if isinstance(params, str):
        for param in params.split("&"):
            if "benchmarkQuery=" in param:
                return param.split("=")[1]

    # Or Parameters can be an object
    if isinstance(params, dict):
        return params.get("benchmarkQuery", "unknown")

    return "unknown"


def process_benchmark_file(json_path, csv_path, output_dir):
    """Process a BenchmarkDotNet JSON file and extract metrics per query."""

    with open(json_path, 'r') as f:
        data = json.load(f)

    benchmarks = data.get("Benchmarks", [])

    # Load peak memory data from CSV
    peak_memory_map = load_peak_memory_from_csv(csv_path)

    # Group benchmarks by query name
    queries = {}

    for benchmark in benchmarks:
        query_name = extract_query_name(benchmark)

        # Extract metrics
        stats = benchmark.get("Statistics", {})
        memory = benchmark.get("Memory", {})

        # Mean execution time (convert from ns to ms)
        mean_time_ms = stats.get("Mean", 0) / 1_000_000

        # Allocated memory (convert from bytes to MB)
        allocated_bytes = memory.get("BytesAllocatedPerOperation", 0)
        allocated_mb = allocated_bytes / (1024 * 1024) if allocated_bytes else 0

        # GC collections
        gen2_collections = memory.get("Gen2Collections", 0)

        # Peak memory from CSV
        peak_memory_mb = extract_peak_memory(benchmark, peak_memory_map)

        queries[query_name] = {
            "mean_time_ms": mean_time_ms,
            "allocated_mb": allocated_mb,
            "gen2_collections": gen2_collections,
            "peak_memory_mb": peak_memory_mb,
        }

    # Create output for each query in benchmark-action format
    results = {}
    for query_name, metrics in queries.items():
        benches = [
            {
                "name": "Mean Execution Time",
                "value": round(metrics["mean_time_ms"], 2),
                "unit": "ms"
            },
            {
                "name": "Allocated Memory",
                "value": round(metrics["allocated_mb"], 2),
                "unit": "MB"
            },
            {
                "name": "Gen2 Collections",
                "value": int(metrics["gen2_collections"]),
                "unit": "collections"
            }
        ]

        # Add Peak Memory if available (insert at position 1, after execution time)
        if metrics["peak_memory_mb"] is not None:
            benches.insert(1, {
                "name": "Peak Memory",
                "value": round(metrics["peak_memory_mb"], 2),
                "unit": "MB"
            })

        results[query_name] = benches

    return results


def main():
    if len(sys.argv) != 4:
        print("Usage: process-benchmarks.py <input-json> <input-csv> <output-dir>")
        sys.exit(1)

    json_path = sys.argv[1]
    csv_path = sys.argv[2]
    output_dir = sys.argv[3]

    if not os.path.exists(json_path):
        print(f"Error: Input JSON file not found: {json_path}")
        sys.exit(1)

    if not os.path.exists(csv_path):
        print(f"Warning: CSV file not found: {csv_path}. Peak Memory will not be available.")

    results = process_benchmark_file(json_path, csv_path, output_dir)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Write individual JSON files for each query
    query_files = []
    for query_name, benches in results.items():
        # Sanitize query name for filename
        safe_name = query_name.replace('/', '_').replace(' ', '_')
        output_path = os.path.join(output_dir, f"{safe_name}.json")

        with open(output_path, 'w') as f:
            json.dump(benches, f, indent=2)

        query_files.append((query_name, safe_name, output_path))

    # Write query list for workflow to use
    query_list_path = os.path.join(output_dir, "queries.txt")
    with open(query_list_path, 'w') as f:
        for query_name, safe_name, _ in query_files:
            f.write(f"{safe_name}\n")

    print(f"Processed {len(results)} queries")
    print(f"Output directory: {output_dir}")
    print(f"Query list: {query_list_path}")

    # Print summary
    for query_name, benches in results.items():
        print(f"\n{query_name}:")
        for bench in benches:
            print(f"  {bench['name']}: {bench['value']} {bench['unit']}")


if __name__ == "__main__":
    main()
