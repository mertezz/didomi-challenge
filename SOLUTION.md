

## Project overview

## How to Run Your Solution

   - Any additional dependencies you added and why
   - Step-by-step commands to run your data ingestion script(s)
   - Step-by-step commands to run your dbt models
   - (Optional) Example queries to verify results

## Data Quality Observations
   - What data quality issues did you encounter?
   - How did you handle them?

## Architecture Decisions
   - Your data ingestion strategy and rationale
   - Your dbt model organization approach
   - Any trade-offs or important choices you made
   - -----------------
environemnts: introduciton of a new docker-compose variable to support both local development and container runs (RUN_MODE). As it was only one variable introducition of docker-compose profiles was not deemed necesary

loader: 
- concept load data first and then clean it in one place (reduce disperse logic across several systems, support slim/simple loader -> less margin for errors because of cleansing)
- in search for a generic approach that would allow peformanc, robustness and flexibliltiy (eg. various formats)
- considered: 
  - dataframes (eg. Pandas/Polars)
  - bulk loads via postgres extensions (eg. pg_parquet)
  - bulk loads via DuckDB
DuckDB was selected because of its fast perfrmance (eg. parallel read on parquet file row groups and parallel processing and over multiple Parquet files at the same time using the glob syntax), portability, broad file supports and good integration with Postgres. It was the most suitable candidate because is it not only support loading data but also enables exploration on raw files without the intorduciton other libraries/tools (so used for loading, potential transformations and data exploration of raw data). 

## Key Insights

   - 2-3 interesting findings from the metrics you calculated about consent behavior, company performance, or other patterns in the data

## Architecture Decisions
   - Anything else the reviewer should know about your solution





## Appendix
### Possible next actions
**DuckDB loader performance**

The loader relies on DuckDB’s built-in readers, which parallelize file reads whenever possible (eg, based on the number of row groups in the Parquet file).

In addition to increasing the number of row groups in the exported Parquet files, a potential improvement to further enhance parallelization would be to leverage glob patterns or the `read_csv()` / `read_parquet()` functions that can accept multiple files directly in the `FROM` clause. 

This functionality was intentionally omitted in the current version to keep the loader simple. Files are currently processed iteratively.

### DuckDB 
#### Benchmark INSERT INTO vs COPY TO
A quick benchmark comparing the INSERT INTO and COPY commands showed no significant difference in load performance when using DuckDB to load data from Parquet files into Postgres. Therefore, due to greater flexibility (e.g., the option to add timestamp and filename columns), the solution is implemented using the INSERT INTO command.
```sql 
-- 1. Generate synthetic data: 10M rows with 5 random columns
create table bench_data as
select
    range as id,
    random() as col_a,
    random() as col_b,
    random() as col_c,
    md5(random()::varchar) as col_d,
    (random() * 1000)::int as col_e
from range(10000000);

-- 2. create tables
create table pg.raw.bench_insert as select * from bench_data where 1=0;
create table pg.raw.bench_copy   as select * from bench_data where 1=0;

-- 3. benchmark 1: insert into … select * from read_parquet
-- 10,000,000 rows affected in 22 s 263 ms
-- 10,000,000 rows affected in 25 s 46 ms
insert into pg.raw.bench_insert select * from read_parquet('/tmp/gen_data_10m.parquet');

-- 3. benchmark 2: copy … from parquet
-- 10,000,000 rows affected in 21 s 479 ms
-- 10,000,000 rows affected in 25 s 154 ms
copy pg.raw.bench_copy from '/tmp/gen_data_10m.parquet' (format 'parquet');

-- 4. sanity check: row counts
select
  (select count(*) from pg.raw.bench_insert) as insert_count,
  (select count(*) from pg.raw.bench_copy) as copy_count;

```
#### DuckDB database access limitation

DuckDB does not support simultaneous access to the same database file from multiple processes (eg. load_metadata table).
When one process holds a write connection (eg, the Python loader), any additional process even in read-only mode (eg, DataGrip) will trigger a file lock conflict.

This behavior is intentional: DuckDB enforces exclusive file-level locking to preserve transactional integrity, meaning only a single process can safely read or write to a `.duckdb` file at a time.  
