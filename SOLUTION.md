

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

## Key Insights

   - 2-3 interesting findings from the metrics you calculated about consent behavior, company performance, or other patterns in the data

## Architecture Decisions
   - Anything else the reviewer should know about your solution





## Appendix
### Possible next actions
**DuckDB loader**

A possible extension of the loader could include tracking of already loaded files. To achieve this, metadata about the loaded files should be maintained.  

### DuckDB loader performance: INSERT INTO vs COPY TO
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