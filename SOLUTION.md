# todo
ui.action purpose changed.. ce je kaj zadodtaj..


--- finindgs (pa sej ne vem ce so vsi)
- event data -> user can create multiple consents whether be it because of changes in purpose or allowed vendors
- TODO ANALYSIS thorgouht.. use based..
- dbt
  - profiles.yml was move direcly to project because we do not have muliptle profiles..
  - seeds: countries -> https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
  - tags (za schedule + area), 
  - uq macro, 
  - incremental = (today set v variablo - poglej tudi druge),
  - on schema change dodaj.. pri incrementih
  - data checks
  - unit tests

- duckdb..      
- dotakni se semantci models (v dbt or snowfalke)
- CHECK how it works adding a field.. scham change
- test
  - data checke
  - custom tests
  - unit test..



## Project overview

## How to Run Your Solution

   - Any additional dependencies you added and why
   - Step-by-step commands to run your data ingestion script(s)
   - Step-by-step commands to run your dbt models
   - (Optional) Example queries to verify results

## Data Quality Observations
   - What data quality issues did you encounter?
   - How did you handle them?
-----
todo: dodaj quality iz vprasanj mail
experiment column left out because it was empty. 

`fct_event` : Event caount values were rounded using round() to remove floating-point noise and ensure consistent value grouping for analysis and aggregation.
columns, normalised.. 




## Architecture Decisions
   - Your data ingestion strategy and rationale
   - Your dbt model organization approach
   - Any trade-offs or important choices you made
   - -----------------
environemnts: introduciton of a new docker-compose variable to support both local development and container runs (RUN_MODE). As it was only one variable introducition of docker-compose profiles was not deemed necesary
local development:
- export DBT_TARGET=local 
- export RUN_MODE=local

loader: 
- concept load data first and then clean it in one place (reduce disperse logic across several systems, support slim/simple loader -> less margin for errors because of cleansing)
- in search for a generic approach that would allow peformanc, robustness and flexibliltiy (eg. various formats)
- local developmnet possible with an additional system varible (export run_mode=local)
- considered: 
  - dataframes (eg. Pandas/Polars)
  - bulk loads via postgres extensions (eg. pg_parquet)
  - bulk loads via DuckDB
DuckDB was selected because of its fast perfrmance (eg. parallel read on parquet file row groups and parallel processing and over multiple Parquet files at the same time using the glob syntax), portability, broad file supports and good integration with Postgres. It was the most suitable candidate because is it not only support loading data but also enables exploration on raw files without the intorduciton other libraries/tools (so used for loading, potential transformations and data exploration of raw data). 

File columns names case is preserved (quoted). missing file header results in generic column namings (column01, ..) 

### dbt

#### Structure
Project structure follows standard dbt practices, with separate folders representing data layers (raw, staging, marts).  
Each folder maps directly to a physical database schema, making it easy to locate corresponding tables and models.

In larger organizations with several department (eg. marketing, sales, consent-management), an additional subfolder level could be introduced in the `staging` and `mart` folder. To keep a flat structure, such organizational context can also be encoded in model names (eg. `fct_cm_event`), improving table uniqueness across departments. The `raw` layer can similarly be organized by source systems when dealing with a large number of sources.

Schema mapping logic is customized via [`generate_schema_name.sql`](dbt_project/macros/generate_schema_name.sql).  

For simplicity, `profiles.yml`, `dbt_project.yml`, and `packages.yml` are all stored in the project root.

Due to small project size, all **schemas, seeds,** and **sources** are defined in single files (`schema.yml`, `seeds.yml`, `source.yml`) under `/models`.

#### Macros
Jinja templating adds flexibility but can reduce readability and maintainability. 
Macros were used **only when necessary** to keep the solution simple and maintainable.  

The project reuses **community packages** (eg. `dbt_utils.generate_surrogate_key()`) instead of re-implementing standard utilities.

#### Materialisation TODO
Materialisaiton are crucial for efficient ... For simplicy and easier maintenace the default materialisation has been changed from `view` to `table`. An excepiton to these are facts where incremental materialisation is used.

Incremantal materialisation can support different strategies. In the project there are two examples of such strategies:
- `delete+insert` - suitable for staging tables
- `microbatch` - suitable for large dataset and iterative reloads  

Both are dbt's built in strategies that are implemented with an additional step (dbt managed _temp tables). While loading the target an additional temporary table is build from which then the load reads and inserts the data into the target. Unfortunatelly this intermediate step cannot be skipped so the only way to improve is to potentially try to create your own materialisation type (which comes with a maintenance overhead).


TODO: 
-- DELETE+INSERT strategy
-- The incremental stage table drives the load (defines the period, batch and keys for delete+insert)
-- Unique key is used to first delete records from the target fact. This enables backfills of the desired period
-- Target records are deleted based on distinct keys from the stage table
-- Full column scan of target keys is required for deletion
-- Incremental mode creates an additional _temp table from the stage
-- Event time filters are applied only in the stage table, here are not needed
-- Additional performance improvements could be achieved with dbt predicates and microbatching
-- Example:
--  dbt run -s +fct_event --vars '{"start_date": "2025-11-01", "end_date": "2025-11-12"}'


RELOAD :  dbt run -s +consent_company_day --vars '{"start_date": "2025-09-05", "end_date": "2025-09-06"}'
-- Define daily load interval (inclusive) 
-- default: yesterday → today, overridable via --vars

dbt run -s +fct_event --vars '{"start_date": "2025-11-01", "end_date": "2025-11-12"}'


#### Data checks
Data checks ... several types

#### Unit tests
Unit test .. 


#### Tags  
Tags in this project are primarily used to support external scheduling, simplify production operations, and enable cost tracking across subject areas.  

Example: running an incremental load for the entire `consent-management` domain:  
```bash
dbt run -s tag:cm
````

#### Loads
full-refresh:
incremental: different strategies.. mention microbatches for large dataset.. also caveats..


### Modeling  
The solution is designed around two modelling approaches:

1. **Dimensional modelling**, intended for standardised, enterprise-wide adoption. This approach supports business processes via fact tables and provides context through dimension tables. The target schema is `mart`.  In the assignment one subject area was covered: `consent-management`.
   
    
    TODO: Add bus matrix

2. **Wide-table modelling**, used exclusively for specific dashboards and performance-driven analyses. This is implemented in schema `report`, and is not meant to be shared broadly across teams.


#### Naming standards
The project follows consistent naming conventions aligned with common data warehousing practices.

Schemas:
- `raw` —  untransformed source data loaded as-is from operational systems or files.  
- `staging` — temporary data prepared for modeling (ephemeral).  
- `mart` — dimensional models optimized for analytics (facts and dimensions).  
- `report` — reporting-ready tables tailored to specific dashboard needs; not intended for company-wide sharing.

Table types
- `stg_` — staging tables; used for normalization and preparation of raw data.  
- `dim_` — dimension tables.  
- `fct_` — transactional fact tables describing business process performance.  

Attribute conventions
- `_sk` — surrogate key.  
- `_fk` — foreign key.  
- `_uq` — unique key made by combining several fields. Mainly used in fact tables to simplify maintenance.
- `_id` — natural or business identifier from source data.  
- `_code` — standardized code values (eg, ISO country code).  
- `_no` — numeric business identifiers (eg., invoice_no).  
- `_flag` / `_ind` — boolean or indicator fields (Y/N).  
- `_dttm` — timestamp field.  
- `_dt` — date type field.
- `valid_from`, `valid_to` — validity columns for dimensions (non-historical in this challenge, but included for consistency).
- `_count` - numeric count of events, records, or actions
- `lag_` - time difference between milestones


Metadata columns
- `run_id` — dbt execution identifier for lineage and troubleshooting.  
- `ingest_dttm` — timestamp of data ingestion.  
- `update_dttm` — timestamp of last record update (if applicable).  
- `origin` — data origin, typically file name or source system reference.  

#### Surrogate keys and their role
When creating surrogate keys and defining relationships between facts and dimensions, several approaches exist. Two of them are outlined below:
- **Early-binding approach:**  
  in traditional dwh design, surrogate keys (sk) are generated within dimensions as independent, sometimes random unique identifiers. When loading fact tables, fact data is joined to dimensions on the business key (and time fields for scd handling) to fetch the correct sk.  
  The resulting fact record points directly to a unique, time-specific dimension row.  
  *Drawback:* facts depend on preloaded dimensions, increasing processing time, complexity and coupling.

- **Late-binding approach:**  
  In this approach, fact table foreign keys do not reference a unique dimension row when history is tracked (SCD dimension).  
  Surrogate keys in these facts are derived directly from the business key, without joining to the dimension during loading.  
  Historical context is resolved at query time, when users apply time-based filters to pick the valid dimension record.  
  *Drawback:* This approach simplifies loading and decouples facts from dimensions, but shifts responsibility to consumers.  
  If time filters are misapplied, it can cause incorrect results which represent a major risk.


In the prototype challenge, a **late-binding** strategy was applied for flexibility and simplicity. For production environments, a hybrid or **early-binding** approach is advisable to ensure data stability and reduce risk for less experienced users.

#### Dimensions
Following Kimball dimensional modeling principles, several potential dimensions were considered to be created or derived from the provided raw event dataset: `dim_device`, `dim_event_type`, `dim_consent_status`, `dim_experiment`, `dim_domain`, `dim_deployment`, `dim_user`, `dim_vendor`,..  
 
These dimensions were **not implemented** as separate entities. Instead, their attributes were kept as **degenerate dimensions** within the fact table (stored directly in `fct_event` without foreign keys).

This decisions was taken because:
- Each of these entities currently contains only one or two attributes, which do not provide descriptive context that would improve understanding of the analysed data.  
- Within the assignment scope, no additional descriptive or lookup attributes (eg., labels, hierarchies, textual descriptions) were identified as necessary to include.
- Avoiding unnecessary joins improves model simplicity and query performance.  

The decision is a **design compromise**. Typically, dimensional attributes are externalized to dedicated dimension tables to support reuse, hierarchy navigation, and descriptive enrichment.

In a future production setup, these degenerate dimensions would likely evolve into fully developed conformed dimensions, supporting:  
- richer attribute context (eg. device families and other atriburtes, user segments, ..),  
- drill-down / roll-up capabilities in reporting,  
- consistent reuse across multiple fact tables.

As part of the assignment the following dimensions were implemented: 
- `dim_company`
- `dim_country`
- `dim_date`

Each dimension also includes the default row to handle unmatched fact records. Dimensions currently do not track any history.

#### Facts
TOOD- should describe processes (check..)

granularity:
    fct_event: event
    fct_consent: company by day

#### Time standardization (UTC)
Timestamps used for linking facts and dimensions are standardized to UTC to ensure consistent across systems.
End-user reporting or presentation layers may apply local time zone conversions as needed. In this challenge, such conversions were not implemented.



## Key Insights
TODO
   - 2-3 interesting findings from the metrics you calculated about consent behavior, company performance, or other patterns in the data
------- 
   - TODO
Quite a low consent conversion rate, the top aprox 27% 

![img.png](img.png)

## Caveats & notes
TODO:
windowing function despite being powerful can be also resouce heavy. For calculating the average time-to-consent metric it is advisable that this time is direcly calculated in the SDK which would reduce the partition scan operation for matching the `consent.asked` and `consent.given` event.

For developing the solution locally additional system parameters are needed before running the solution: 
-  export RUN_MODE=local
-  export DBT_TARGET=local 

add sentence about semantic layer options..

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
