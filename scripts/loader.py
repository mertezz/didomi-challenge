import argparse
import os
import glob
import yaml
import duckdb
import json

def parse_args():
    ap = argparse.ArgumentParser(
        description="Load one or multiple CSV/Parquet files into a Postgres table using DuckDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--config", default="config.yml", help="Path to YAML config file")
    ap.add_argument("--filename", required=True, help="Input file or glob pattern (supports *.csv, *.parquet)")
    ap.add_argument("--table", required=True, help="Target table name in Postgres (e.g. raw.events)")
    ap.add_argument("--enforce-schema", help="Optional schema file (JSON) to create table structure explicitly")
    ap.add_argument(
        "--write-disposition",
        choices=["append", "truncate_append"],
        default="append",
        help="How to write data into the target table (append or truncate_append)",
    )
    return ap.parse_args()


def load_config(config_path: str):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    env_name = os.getenv("RUN_MODE", cfg.get("default", "local"))
    print(f"RUN_MODE = {env_name}")
    env = cfg["environments"][env_name]["database"]

    dsn = (
        f"postgresql://{env['user']}:{env['password']}@"
        f"{env['host']}:{env['port']}/{env['dbname']}"
    )

    schema = env.get("schema", "public")
    return dsn, schema


def setup_duckdb(dsn: str):
    con = duckdb.connect(database=":memory:")
    con.sql("install postgres; load postgres;")
    con.sql("install parquet; load parquet;")
    con.sql(f"attach '{dsn}' as pg (type postgres);")
    return con


def create_table(con, filename, target_table, schema_file=None):
    """Create table in Postgres using JSON schema or inferred schema"""
    if schema_file and os.path.exists(schema_file):
        print(f"Using explicit schema from {schema_file}")
        with open(schema_file, "r") as f:
            schema = json.load(f)

        # Build DDL dynamically
        cols = ",\n    ".join([f'"{c["name"]}" {c["type"]}' for c in schema["columns"]])
        ddl = f"create table if not exists {target_table} (\n    {cols},\n    load_ts TIMESTAMP,\n    filename TEXT\n);"
        con.sql(ddl)
    else:
        print(f"Inferring schema from file: {filename}")
        con.sql(f"""
            create table if not exists {target_table} as
            select *, now()::timestamp AS load_ts, filename
            from '{filename}'
            limit 0;
        """)


def truncate_table(con, target_table):
    print(f"Truncating {target_table}")
    con.sql(f"truncate table {target_table};")


def copy_files(con, files, target_table):
    """Insert data into target table with load_ts and filename metadata"""
    for f in files:
        print(f"Inserting {f} → {target_table}")
        con.sql(f"""
            insert into {target_table}
            select 
                t.*, 
                current_timestamp as load_ts, 
                filename
            from '{f}' t;
        """)


def main():
    args = parse_args()
    dsn, schema = load_config(args.config)
    con = setup_duckdb(dsn)
    target_table = f"pg.{args.table}"  # load to Postgres

    # Expand glob pattern
    files = glob.glob(args.filename, recursive=True)
    if not files:
        raise ValueError(f"No files found for pattern: {args.filename}")

    # Create table if it doesn't exist
    # Schema is defined based on the first file
    create_table(con, files[0], target_table, args.enforce_schema)

    # Handle write disposition
    if args.write_disposition == "truncate_append":
        truncate_table(con, target_table)

    # Insert data
    copy_files(con, files, target_table)

    print(f"Loaded {len(files)} file(s) into {target_table} using {args.write_disposition}")


if __name__ == "__main__":
    main()


# sample run:
# export RUN_MODE=local
# python loader.py --filename '../input/events/*/*' --table raw.test --write-disposition truncate_append --enforce-schema events-company.json