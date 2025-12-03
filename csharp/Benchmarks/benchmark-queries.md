# Benchmark Query Suite - TPC-DS Dataset

This document contains a curated set of queries for benchmarking CloudFetch performance across different data characteristics.

## Query Categories

### 1. Size Variations (Row Count)

#### Small (< 100K rows)
```sql
-- Query 1a: Store dimension (12 rows, 29 columns) - Minimal data
select * from main.tpcds_sf1_delta.store
```

```sql
-- Query 1b: Item dimension (~18K rows, 22 columns) - Small dataset
select * from main.tpcds_sf1_delta.item
```

```sql
-- Query 1c: Customer (~100K rows, 18 columns) - Medium-small
select * from main.tpcds_sf1_delta.customer
```

#### Medium (100K - 1M rows)
```sql
-- Query 2a: Date dimension (~73K rows, 28 columns) - Calendar data
select * from main.tpcds_sf1_delta.date_dim
```

```sql
-- Query 2b: Time dimension (~86K rows, 10 columns) - Time of day
select * from main.tpcds_sf1_delta.time_dim
```

```sql
-- Query 2c: Web sales (~720K rows, 34 columns) - Large web transactions
select * from main.tpcds_sf1_delta.web_sales
```

#### Large (> 1M rows)
```sql
-- Query 3a: Catalog sales (~1.4M rows, 34 columns) - CURRENT DEFAULT
select * from main.tpcds_sf1_delta.catalog_sales
```

```sql
-- Query 3b: Store sales (~2.8M rows, 23 columns) - Largest fact table
select * from main.tpcds_sf1_delta.store_sales
```

```sql
-- Query 3c: Inventory (~11.7M rows, 5 columns) - Very large, narrow
select * from main.tpcds_sf1_delta.inventory
```

### 2. Column Count Variations

#### Narrow Tables (< 10 columns)
```sql
-- Query 4a: Inventory (5 columns: date_sk, item_sk, warehouse_sk, quantity_on_hand)
select * from main.tpcds_sf1_delta.inventory
```

```sql
-- Query 4b: Store returns (20 columns, ~288K rows)
select * from main.tpcds_sf1_delta.store_returns limit 500000
```

#### Medium Tables (10-20 columns)
```sql
-- Query 5a: Customer (18 columns, ~100K rows)
select * from main.tpcds_sf1_delta.customer
```

```sql
-- Query 5b: Store sales (23 columns, ~2.8M rows)
select * from main.tpcds_sf1_delta.store_sales limit 1000000
```

#### Wide Tables (> 25 columns)
```sql
-- Query 6a: Store dimension (29 columns, 12 rows) - Widest table
select * from main.tpcds_sf1_delta.store
```

```sql
-- Query 6b: Date dimension (28 columns, ~73K rows)
select * from main.tpcds_sf1_delta.date_dim
```

```sql
-- Query 6c: Web sales (34 columns, ~720K rows)
select * from main.tpcds_sf1_delta.web_sales
```

```sql
-- Query 6d: Catalog sales (34 columns, ~1.4M rows) - Wide + Large
select * from main.tpcds_sf1_delta.catalog_sales
```

### 3. Data Type Variations

#### Numeric-Heavy
```sql
-- Query 7a: Sales with prices, costs, discounts (many decimals)
select
  ss_sold_date_sk, ss_item_sk, ss_customer_sk,
  ss_quantity, ss_wholesale_cost, ss_list_price,
  ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price,
  ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax,
  ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit
from main.tpcds_sf1_delta.store_sales
```

#### String-Heavy
```sql
-- Query 7b: Customer demographics and addresses (many strings)
select
  c_customer_id, c_salutation, c_first_name, c_last_name,
  c_preferred_cust_flag, c_birth_country, c_login,
  c_email_address, c_last_review_date_sk,
  ca_street_number, ca_street_name, ca_street_type,
  ca_suite_number, ca_city, ca_county, ca_state,
  ca_zip, ca_country, ca_location_type
from main.tpcds_sf1_delta.customer c
join main.tpcds_sf1_delta.customer_address ca on c.c_current_addr_sk = ca.ca_address_sk
```

#### Date/Timestamp-Heavy
```sql
-- Query 7c: Time dimensions (dates, quarters, years, days of week)
select
  d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq,
  d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy,
  d_fy_year, d_fy_quarter_seq, d_fy_week_seq,
  d_day_name, d_quarter_name, d_holiday, d_weekend,
  d_following_holiday, d_first_dom, d_last_dom,
  d_same_day_ly, d_same_day_lq, d_current_day,
  d_current_week, d_current_month, d_current_quarter, d_current_year
from main.tpcds_sf1_delta.date_dim
```

### 4. Filtered Queries (Smaller Result Sets)

```sql
-- Query 8a: Recent catalog sales (filtered by date)
select * from main.tpcds_sf1_delta.catalog_sales
where cs_sold_date_sk >= 2451545  -- Year 2000+
```

```sql
-- Query 8b: High-value transactions (filtered by amount)
select * from main.tpcds_sf1_delta.store_sales
where ss_net_paid > 100
```

```sql
-- Query 8c: Specific customer segment (filtered)
select * from main.tpcds_sf1_delta.customer
where c_birth_country = 'UNITED STATES'
and c_customer_sk < 50000
```

### 5. Aggregation Queries (Testing Different Path)

```sql
-- Query 9a: Daily sales summary
select
  ss_sold_date_sk,
  count(*) as transaction_count,
  sum(ss_quantity) as total_quantity,
  sum(ss_sales_price) as total_sales,
  avg(ss_sales_price) as avg_price,
  min(ss_sales_price) as min_price,
  max(ss_sales_price) as max_price
from main.tpcds_sf1_delta.store_sales
group by ss_sold_date_sk
```

```sql
-- Query 9b: Top selling items
select
  i_item_sk, i_item_id, i_item_desc,
  sum(ss_quantity) as total_sold,
  sum(ss_net_profit) as total_profit
from main.tpcds_sf1_delta.store_sales ss
join main.tpcds_sf1_delta.item i on ss.ss_item_sk = i.i_item_sk
group by i_item_sk, i_item_id, i_item_desc
order by total_sold desc
limit 100
```

## Recommended Test Suite

### Quick Test (< 5 minutes)
1. Query 1a: Store (12 rows) - Minimal
2. Query 1b: Item (18K rows) - Small
3. Query 2c: Web sales (720K rows) - Medium
4. Query 4a: Inventory (11.7M rows, 5 cols) - Narrow + Large

### Standard Test (< 15 minutes)
1. Query 1c: Customer (100K rows, 18 cols)
2. Query 2c: Web sales (720K rows, 34 cols)
3. Query 3a: Catalog sales (1.4M rows, 34 cols) - **CURRENT DEFAULT**
4. Query 3b: Store sales (2.8M rows, 23 cols)
5. Query 4a: Inventory (11.7M rows, 5 cols)

### Comprehensive Test (< 30 minutes)
All queries from Size Variations (Queries 1-3) + Column Variations (Queries 4-6)

### Stress Test
1. Query 3c: Inventory (11.7M rows) - Largest dataset
2. Query 3b: Store sales (2.8M rows) - Second largest
3. Query 6d: Catalog sales (34 cols, 1.4M rows) - Wide + Large
4. Run all three consecutively to test sustained performance

## Usage in CI/CD

### Current Default
```bash
# Default query (if not specified)
export BENCHMARK_QUERY="select * from main.tpcds_sf1_delta.catalog_sales"
```

### Manual Trigger with Custom Query
1. Go to Actions → Performance Benchmarks → Run workflow
2. Select branch
3. Enter custom SQL query from above
4. Click "Run workflow"

### Query Selection Guidelines

**For daily CI runs:**
- Use Query 3a (catalog_sales) - Current default
- Good balance of size (1.4M rows) and column count (34 cols)
- ~30-60 seconds execution time

**For PR benchmarks:**
- Use Query 2c (web_sales) - Faster, still representative
- 720K rows, 34 columns
- ~15-30 seconds execution time

**For release testing:**
- Run Standard Test suite
- Covers small, medium, large datasets
- Covers narrow and wide schemas

**For performance regression investigation:**
- Use Query 3c (inventory) for pure throughput testing (11.7M rows)
- Use Query 6d (catalog_sales) for complex schema testing (34 cols)
- Use Query 7a-7c for data type-specific testing

## Table Reference

| Table | Rows (SF1) | Columns | Characteristics |
|-------|-----------|---------|-----------------|
| store | 12 | 29 | Widest, minimal rows |
| item | ~18K | 22 | Small dimension |
| customer | ~100K | 18 | Medium dimension |
| date_dim | ~73K | 28 | Calendar data |
| time_dim | ~86K | 10 | Time of day |
| web_sales | ~720K | 34 | Web transactions |
| catalog_sales | ~1.4M | 34 | **Current default** |
| store_sales | ~2.8M | 23 | Largest sales fact |
| inventory | ~11.7M | 5 | Largest table, narrow |

## Notes

- All queries assume `main.tpcds_sf1_delta` catalog/schema exists
- TPC-DS SF1 = Scale Factor 1 (smallest standard size)
- Actual row counts may vary slightly by Databricks cluster
- Query execution time depends on warehouse size and network speed
- CloudFetch chunk size is typically 100MB compressed
