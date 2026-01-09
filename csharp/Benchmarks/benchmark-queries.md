<!---
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# CloudFetch Benchmark Query Suite

This document describes the optimized benchmark query suite for testing CloudFetch performance with the Databricks C# driver using TPC-DS dataset.

## Overview

The benchmark suite consists of **7 focused queries** designed to test CloudFetch performance across different data characteristics:
- All queries return > 100K rows (ensuring CloudFetch is utilized)
- Complete coverage of size, width, and data type variations
- No redundancy - each query tests unique characteristics
- Real-world TPC-DS workloads

## Query Categories

### Size Categories
- **Small**: < 1M rows
- **Medium**: 1M - 10M rows
- **Large**: > 10M rows

### Width Categories
- **Narrow**: < 10 columns
- **Medium**: 10-20 columns
- **Wide**: 20-50 columns
- **X-Wide**: > 50 columns

## Benchmark Queries

### 1. catalog_sales (medium-wide)
```sql
select * from main.tpcds_sf1_delta.catalog_sales
```
- **Rows**: 1.4M
- **Columns**: 34
- **Purpose**: Default benchmark query, tests medium-sized result sets with wide schema

### 2. inventory (large-narrow)
```sql
select * from main.tpcds_sf1_delta.inventory
```
- **Rows**: 11.7M (largest!)
- **Columns**: 5
- **Purpose**: Stress test with massive row count but narrow schema

### 3. web_sales (small-wide)
```sql
select * from main.tpcds_sf1_delta.web_sales
```
- **Rows**: 719K
- **Columns**: 34
- **Purpose**: Tests smaller result sets with wide schema

### 4. customer (small-medium)
```sql
select * from main.tpcds_sf1_delta.customer
```
- **Rows**: 100K
- **Columns**: 18
- **Purpose**: Tests minimum viable CloudFetch size

### 5. store_sales_numeric (medium-medium)
```sql
select ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_quantity,
       ss_wholesale_cost, ss_list_price, ss_sales_price,
       ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost,
       ss_ext_list_price, ss_ext_tax, ss_coupon_amt,
       ss_net_paid, ss_net_paid_inc_tax, ss_net_profit
from main.tpcds_sf1_delta.store_sales
```
- **Rows**: 2.8M
- **Columns**: 16
- **Purpose**: Tests numeric-heavy data (decimals, floats)

### 6. sales_with_timestamps (medium-medium)
```sql
select ss.ss_sold_date_sk, ss.ss_item_sk, ss.ss_customer_sk,
       ss.ss_quantity, ss.ss_sales_price,
       cast(d.d_date as timestamp) as sale_timestamp,
       cast(d.d_date as timestamp) + interval 1 hour as processing_timestamp,
       cast(d.d_date as timestamp) + interval 1 day as shipment_timestamp,
       cast(d.d_date as timestamp) + interval 3 days as delivery_timestamp,
       cast(d.d_date as timestamp) + interval 7 days as return_deadline,
       cast(d.d_date as timestamp) + interval 30 days as warranty_start,
       cast(d.d_date as timestamp) + interval 365 days as warranty_end,
       current_timestamp() as record_created_at
from main.tpcds_sf1_delta.store_sales ss
join main.tpcds_sf1_delta.date_dim d on ss.ss_sold_date_sk = d.d_date_sk
```
- **Rows**: 2.8M
- **Columns**: 13
- **Purpose**: Tests timestamp-heavy data with 8 timestamp columns

### 7. wide_sales_analysis (medium-x-wide)
```sql
select ss.ss_sold_date_sk, ss.ss_item_sk, ss.ss_customer_sk, ss.ss_quantity,
       ss.ss_wholesale_cost, ss.ss_list_price, ss.ss_sales_price,
       ss.ss_ext_discount_amt, ss.ss_ext_sales_price, ss.ss_ext_wholesale_cost,
       ss.ss_ext_list_price, ss.ss_ext_tax, ss.ss_coupon_amt,
       ss.ss_net_paid, ss.ss_net_paid_inc_tax, ss.ss_net_profit,
       c.c_customer_id, c.c_salutation, c.c_first_name, c.c_last_name,
       c.c_birth_country, c.c_login, c.c_email_address,
       i.i_item_id, i.i_item_desc, i.i_brand, i.i_class, i.i_category,
       i.i_product_name, i.i_color, i.i_units, i.i_size, i.i_manager_id,
       cast(d.d_date as timestamp) as sale_date,
       cast(d.d_date as timestamp) + interval 1 hour as processing_time,
       cast(d.d_date as timestamp) + interval 2 hours as quality_check_time,
       cast(d.d_date as timestamp) + interval 4 hours as packaging_time,
       cast(d.d_date as timestamp) + interval 1 day as shipment_time,
       cast(d.d_date as timestamp) + interval 2 days as in_transit_time,
       cast(d.d_date as timestamp) + interval 3 days as delivery_time,
       cast(d.d_date as timestamp) + interval 5 days as customer_received_time,
       cast(d.d_date as timestamp) + interval 7 days as return_window_start,
       cast(d.d_date as timestamp) + interval 14 days as return_window_mid,
       cast(d.d_date as timestamp) + interval 30 days as return_window_end,
       cast(d.d_date as timestamp) + interval 60 days as warranty_start,
       cast(d.d_date as timestamp) + interval 180 days as warranty_mid,
       cast(d.d_date as timestamp) + interval 365 days as warranty_end,
       cast(d.d_date as timestamp) + interval 730 days as extended_warranty_end,
       current_timestamp() as record_created_at,
       current_timestamp() + interval 1 year as record_expires_at
from main.tpcds_sf1_delta.store_sales ss
join main.tpcds_sf1_delta.customer c on ss.ss_customer_sk = c.c_customer_sk
join main.tpcds_sf1_delta.item i on ss.ss_item_sk = i.i_item_sk
join main.tpcds_sf1_delta.date_dim d on ss.ss_sold_date_sk = d.d_date_sk
```
- **Rows**: 2.8M
- **Columns**: 54
- **Purpose**: Tests x-wide schemas with balanced data types (16 numeric, 18 string, 18 timestamp)

## Coverage Summary

| Category | Count | Queries |
|----------|-------|---------|
| **Size** |
| Small (<1M) | 2 | web_sales, customer |
| Medium (1-10M) | 4 | catalog_sales, store_sales_numeric, sales_with_timestamps, wide_sales_analysis |
| Large (>10M) | 1 | inventory |
| **Width** |
| Narrow (<10) | 1 | inventory |
| Medium (10-20) | 3 | customer, store_sales_numeric, sales_with_timestamps |
| Wide (20-50) | 2 | catalog_sales, web_sales |
| X-Wide (>50) | 1 | wide_sales_analysis |
| **Data Types** |
| Numeric-heavy | 1 | store_sales_numeric |
| Timestamp-heavy | 1 | sales_with_timestamps |
| Balanced x-wide | 1 | wide_sales_analysis |

**Total Test Data**: 21.5M rows across 7 queries

## Running Benchmarks

### Trigger on PR
Add the `benchmark` label to any PR to run the full suite on both .NET 8.0 and .NET Framework 4.7.2.

### Manual Trigger
Use workflow_dispatch to run specific queries:
```bash
gh workflow run benchmarks.yml --field queries="all"
gh workflow run benchmarks.yml --field queries="inventory,catalog_sales"
```

### Expected Runtime
- **Per platform**: ~6-8 minutes
- **Both platforms**: ~12-16 minutes total
- Single build + 7 queries (vs previous 15 rebuilds + 15 queries)

## Viewing Results

### GitHub Pages Dashboard
Benchmark results are automatically published to GitHub Pages for historical trend analysis:
- **URL**: https://adbc-drivers.github.io/databricks/bench/
- **Updated**: Automatically on every commit to main

The dashboard provides interactive charts for four key metrics:

1. **Mean Execution Time (ms)**: Average query execution time including CloudFetch downloads and decompression
2. **Peak Memory (MB)**: Maximum working set memory during execution (lower is better)
3. **Allocated Memory (MB)**: Total managed memory allocated during execution
4. **Gen2 Collections**: Number of full garbage collections (lower indicates less memory pressure)

Each metric page includes:
- Dropdown to select which query to visualize (all 7 queries available)
- Interactive charts showing performance trends over time
- Separate views for .NET 8.0 and .NET Framework 4.7.2
- Hover tooltips with exact values and commit information
- Comparison with baseline (previous run) when available

### Console Output and Artifacts
Benchmark results are also available in GitHub Actions console output and uploaded as artifacts (retained for 90 days).

Console results show:
- **Query Name**: Clear identification of each query
- **Columns**: Number of columns in result
- **Total Rows**: Actual row count processed
- **Mean/Median/Min/Max**: Execution time statistics
- **Peak Memory (MB)**: Maximum memory usage
- **Total Batches**: Number of Arrow batches processed
- **GC Time %**: Percentage of time in garbage collection

## Notes

- All queries use TPC-DS SF1 (Scale Factor 1) dataset
- Requires access to `main.tpcds_sf1_delta` catalog in Databricks
- CloudFetch chunk size: typically 100MB compressed
- Benchmark includes 5ms read delay per 10K rows to simulate Power BI consumption
