import os
import psycopg2
import pandas as pd
from io import StringIO

# Configuration
CHUNK_SIZE = 100_000
CSV_FOLDER = './csv_input'  # Update to your CSV folder path
PRINT_PROGRESS_EVERY = 10

# PostgreSQL connection parameters
DB_CONFIG = {
    'dbname': 'your_db',
    'user': 'your_user',
    'password': 'your_pass',
    'host': 'localhost',
    'port': 5432
}

def get_table_schema(cursor, table_name):
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (table_name,))
    return cursor.fetchall()

def map_pg_type_to_pandas(pg_type):
    mapping = {
        'integer': 'Int64',
        'bigint': 'Int64',
        'smallint': 'Int64',
        'real': 'float32',
        'double precision': 'float64',
        'numeric': 'float64',
        'text': 'string',
        'character varying': 'string',
        'timestamp without time zone': 'datetime64[ns]',
        'timestamp with time zone': 'datetime64[ns]',
        'boolean': 'boolean',
        'date': 'datetime64[ns]',
    }
    return mapping.get(pg_type, 'string')

def create_temp_staging_table(cursor, table_name):
    cursor.execute(f"DROP TABLE IF EXISTS staging_{table_name};")
    cursor.execute(f"CREATE TEMP TABLE staging_{table_name} (LIKE {table_name} INCLUDING ALL);")

def insert_chunk_to_staging(df, table_name, cursor):
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False, header=False)
    csv_buf.seek(0)
    cursor.copy_expert(f"COPY staging_{table_name} FROM STDIN WITH CSV", csv_buf)

def copy_staging_to_target(cursor, table_name):
    cursor.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM staging_{table_name};
    """)

def process_csv_files_in_folder(folder_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN;")
        for filename in os.listdir(folder_path):
            if not filename.endswith('.csv'):
                continue

            table_name = os.path.splitext(filename)[0]
            csv_path = os.path.join(folder_path, filename)

            print(f"\n📄 Processing file: {filename} → table: {table_name}")

            schema = get_table_schema(cursor, table_name)
            columns = [col for col, _ in schema]
            dtype_dict = {col: map_pg_type_to_pandas(pg_type) for col, pg_type in schema}
            parse_dates = [col for col, pg_type in schema if 'timestamp' in pg_type or pg_type == 'date']

            create_temp_staging_table(cursor, table_name)

            chunk_iter = pd.read_csv(csv_path, chunksize=CHUNK_SIZE, dtype=dtype_dict, parse_dates=parse_dates)

            total_rows = 0
            for i, chunk in enumerate(chunk_iter):
                if chunk.shape[1] != len(columns):
                    raise ValueError(f"CSV column mismatch: expected {len(columns)} but got {chunk.shape[1]}")
                chunk.columns = columns
                insert_chunk_to_staging(chunk, table_name, cursor)
                total_rows += len(chunk)
                if i % PRINT_PROGRESS_EVERY == 0:
                    print(f"✅ {filename}: Processed {total_rows:,} rows")

            copy_staging_to_target(cursor, table_name)
            print(f"🎯 {filename}: Inserted {total_rows:,} rows into {table_name}")

        conn.commit()
        print("\n✅ All files processed and committed successfully.")
    except Exception as e:
        conn.rollback()
        print("\n❌ Error occurred. Transaction rolled back.")
        print("Error:", str(e))
    finally:
        cursor.close()
        conn.close()

# Run the processor
if __name__ == "__main__":
    process_csv_files_in_folder(CSV_FOLDER, DB_CONFIG)
          
