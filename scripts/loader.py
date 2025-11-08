import argparse
import os
import glob
import secrets
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
    ap.add_argument(
        "--force-load",
        action="store_true",
        help="Force reload of files even if already tracked in metadata",
    )
    return ap.parse_args()


def load_config(config_path):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    env_name = os.getenv("RUN_MODE", cfg.get("default", "local"))
    print(f"Run mode: {env_name}")
    env = cfg["environments"][env_name]["database"]

    dsn = (
        f"postgresql://{env['user']}:{env['password']}@"
        f"{env['host']}:{env['port']}/{env['dbname']}"
    )
    schema = env.get("schema", "public")
    return dsn, schema


def setup_duckdb(dsn):
    con = duckdb.connect(database="load_metadata.duckdb")
    con.sql("install postgres; load postgres;")
    con.sql("install parquet; load parquet;")
    con.sql(f"attach '{dsn}' as pg (type postgres);")

    # ensure metadata table exists
    con.sql("""
        create table if not exists load_metadata (
            filename text,
            table_name text,
            status text,
            load_time timestamp,
            load_no text
        );
    """)
    return con


def get_new_files(con, files, table_name, force_load=False):
    """Return new (not yet loaded) and skipped files."""
    if force_load:
        print("Force load enabled — reloading all files")
        return files, []

    if not files:
        return [], []

    # Proper escaping of file list
    file_list = ",".join([f"'{f}'" for f in files])
    query = f"""
        select unnest([{file_list}]) as filename
        except
        select filename
        from load_metadata
        where status = 'loaded' and table_name = '{table_name}';
    """

    new_files = [r[0] for r in con.sql(query).fetchall()]
    skipped = [f for f in files if f not in new_files]
    return new_files, skipped


def create_table(con, filename, target_table, schema_file=None):
    """Create target table if not exists."""
    if schema_file and os.path.exists(schema_file):
        print(f"Using explicit schema from file: {schema_file}")
        with open(schema_file, "r") as f:
            schema = json.load(f)
        cols = ",\n    ".join([f'"{c["name"]}" {c["type"]}' for c in schema["columns"]])
        ddl = f"""
            create table if not exists {target_table} (
                {cols}
            );
        """
        con.sql(ddl)
    else:
        print(f"Inferring schema from file: {filename}")
        con.sql(f"""
            create table if not exists {target_table} as
            select 
                *, 
                current_timestamp as load_time, 
                '' as load_no, 
                '{os.path.basename(filename)}' as filename
            from '{filename}'
            limit 0;
        """)


def truncate_table(con, target_table):
    print(f"Truncating {target_table}")
    con.sql(f"truncate table {target_table};")

def insert_metadata(con, filename, table_name, load_no, status="loaded"):
    con.sql(f"""
        insert into load_metadata (filename, table_name, load_time, load_no, status)
        values ('{filename}', '{table_name}', current_timestamp, '{load_no}', '{status}');
    """)


def insert_files(con, files, target_table, load_no):
    for f in files:
        print(f"Inserting {f} → {target_table}")
        try:
            con.sql(f"""
                insert into {target_table}
                select t.*, current_timestamp as load_time, '{load_no}' as load_no, '{os.path.basename(f)}' as filename
                from '{f}' t;
            """)
            insert_metadata(con, f, target_table, load_no, status="loaded")
        except Exception as e:
            print(f"Failed to load {f}: {e}")
            insert_metadata(con, f, target_table, load_no, status="failed")


def main():
    print("Starting DuckDB loader")
    print("Source: CSV/Parquet → Target: Postgres (PG)")

    args = parse_args()
    dsn, schema = load_config(args.config)
    con = setup_duckdb(dsn)
    attached_postgres_db = 'pg'
    target_table = f"{attached_postgres_db}.{args.table}"

    all_files = [os.path.abspath(f) for f in glob.glob(args.filename, recursive=True)]
    if not all_files:
        raise ValueError(f"No files found for pattern: {args.filename}")

    new_files, skipped = get_new_files(con, all_files, target_table, args.force_load)
    load_no = secrets.token_hex(6)  # short unique batch ID

    if not new_files:
        print(f"No new files to load into {args.table}")
        if skipped:
            print(f"Skipped {len(skipped)} file(s):")
            for f in skipped:
                print(f"  - {os.path.basename(f)}")
        return

    create_table(con, new_files[0], target_table, args.enforce_schema)

    if args.write_disposition == "truncate_append":
        truncate_table(con, target_table)

    insert_files(con, new_files, target_table, load_no)


if __name__ == "__main__":
    main()
