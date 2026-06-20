import duckdb
import os

db_path = os.path.join('data', 'output', 'sales.duckdb')
if not os.path.exists(db_path):
    print(f"File not found: {db_path}")
else:
    con = duckdb.connect(db_path)
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables:", tables)
    for t in tables:
        table_name = t[0]
        count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        print(f"Table {table_name} count: {count}")
        
        # Check for duplicates? Assuming all columns makeup a unique row
        cols = con.execute(f"DESCRIBE {table_name}").fetchall()
        col_names = [c[0] for c in cols]
        cols_str = ", ".join(col_names)
        dups = con.execute(f"SELECT {cols_str}, COUNT(*) FROM {table_name} GROUP BY {cols_str} HAVING COUNT(*) > 1").fetchall()
        print(f"Table {table_name} duplicates: {len(dups)}")
