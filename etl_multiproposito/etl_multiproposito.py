import pandas as pd
import os
import time
import logging
import tempfile
import multiprocessing as mp
import shutil
import glob
from datetime import datetime
from dotenv import load_dotenv
import psycopg
import sqlite3

# -----------------------------
# Logging
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("etl_multiproposito.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# -----------------------------
# Utilidades
# -----------------------------


def clean_columns(columns):
    return [str(c).strip().replace(" ", "_").lower() for c in columns]


def format_dataframe(df):
    """
    Convierte inteligentemente a los tipos de datos más adecuados de pandas (nativos).
    No convierte a string, para que la BD infiera y conserve los tipos correctos
    (ej. Int64 en lugar de float, o fechas nativas en vez de texto).
    """
    return df.convert_dtypes()


def get_temp_dir():
    """Usar RAM si existe"""
    if os.path.exists("/dev/shm"):
        return "/dev/shm"
    return tempfile.gettempdir()


# -----------------------------
# Crear tabla dinámica
# -----------------------------

def map_dtype_to_postgres(dtype):
    dt = str(dtype).lower()
    if 'int' in dt:
        return 'BIGINT'
    elif 'float' in dt:
        return 'DOUBLE PRECISION'
    elif 'bool' in dt:
        return 'BOOLEAN'
    elif 'datetime' in dt:
        return 'TIMESTAMP'
    elif 'date' in dt:
        return 'DATE'
    else:
        return 'TEXT'


def map_dtype_to_sqlite(dtype):
    dt = str(dtype).lower()
    if 'int' in dt:
        return 'INTEGER'
    elif 'float' in dt:
        return 'REAL'
    elif 'bool' in dt:
        return 'INTEGER'
    elif 'datetime' in dt:
        return 'TEXT'
    elif 'date' in dt:
        return 'TEXT'
    else:
        return 'TEXT'


def create_table_postgres(conn_string, table, df):

    cols_sql_list = []
    for col, dtype in df.dtypes.items():
        type_sql = map_dtype_to_postgres(dtype)
        cols_sql_list.append(f'"{col}" {type_sql}')

    cols_sql = ",\n        ".join(cols_sql_list)

    sql = f"""
    CREATE UNLOGGED TABLE IF NOT EXISTS {table} (
        {cols_sql}
    );
    """

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    logging.info("Schema validado en PostgreSQL")


def create_table_sqlite(db_path, table, df):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cols_sql_list = []
    for col, dtype in df.dtypes.items():
        type_sql = map_dtype_to_sqlite(dtype)
        cols_sql_list.append(f'"{col}" {type_sql}')

    cols_sql = ", ".join(cols_sql_list)

    cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql})")

    conn.commit()
    conn.close()

    logging.info("Schema validado en SQLite")


# -----------------------------
# COPY PostgreSQL
# -----------------------------

def copy_csv_to_postgres(conn_string, table, csv_file):

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            with open(csv_file, "r", encoding="utf-8") as f:
                with cur.copy(f"COPY {table} FROM STDIN WITH CSV HEADER") as copy:
                    while data := f.read(8192):
                        copy.write(data)
        conn.commit()


# -----------------------------
# SQLite insert
# -----------------------------

def insert_csv_sqlite(db_path, table, csv_file):

    conn = sqlite3.connect(db_path)
    df = pd.read_csv(csv_file)

    df.to_sql(table, conn, if_exists="append", index=False)

    conn.close()


# -----------------------------
# Worker paralelo
# -----------------------------

def process_chunk(args):

    chunk, table, db_mode, conn_string, sqlite_path, temp_dir, idx = args

    csv_file = os.path.join(temp_dir, f"etl_chunk_{idx}.csv")

    chunk.to_csv(csv_file, index=False)

    if db_mode == "postgres":
        copy_csv_to_postgres(conn_string, table, csv_file)

    else:
        insert_csv_sqlite(sqlite_path, table, csv_file)

    os.remove(csv_file)

    return len(chunk)


# -----------------------------
# Lectura dinámica de archivo
# -----------------------------

def read_file_chunks(file_path, chunk_size):

    ext = file_path.lower()

    if ext.endswith(".sav"):

        df = pd.read_spss(file_path)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i+chunk_size]

    elif ext.endswith(".xlsx") or ext.endswith(".xls"):

        df = pd.read_excel(file_path)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i+chunk_size]

    elif ext.endswith(".csv"):

        df = pd.read_csv(file_path)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i+chunk_size]

    else:
        raise Exception("Formato de archivo no soportado")


# -----------------------------
# ETL individual (Por archivo)
# -----------------------------

def run_etl(file_path, table, db_mode, conn_string, sqlite_path, chunk_size=50000):

    logging.info(f"Leyendo archivo en iterador: {file_path}")

    chunks = read_file_chunks(file_path, chunk_size)
    first_chunk = next(chunks)

    # Auditory metadata
    base_name = os.path.basename(file_path)
    load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    first_chunk.columns = clean_columns(first_chunk.columns)
    first_chunk['origen_archivo'] = base_name
    first_chunk['fecha_carga'] = load_time

    first_chunk_formatted = format_dataframe(first_chunk)

    # Crear tabla según DB (IF NOT EXISTS para landing zone progressivo)
    if db_mode == "postgres":
        create_table_postgres(conn_string, table, first_chunk_formatted)
    else:
        create_table_sqlite(sqlite_path, table, first_chunk_formatted)

    temp_dir = get_temp_dir()
    cores = mp.cpu_count()
    logging.info(f"Cores disponibles: {cores}")

    pool = mp.Pool(cores)
    tasks = []
    idx = 0

    tasks.append((first_chunk_formatted, table, db_mode,
                 conn_string, sqlite_path, temp_dir, idx))
    idx += 1

    for chunk in chunks:
        # Enforce column name logic on remaining chunks
        chunk.columns = clean_columns(chunk.columns)
        chunk['origen_archivo'] = base_name
        chunk['fecha_carga'] = load_time

        chunk_formatted = format_dataframe(chunk)

        tasks.append((chunk_formatted, table, db_mode,
                     conn_string, sqlite_path, temp_dir, idx))
        idx += 1

    results = pool.map(process_chunk, tasks)
    pool.close()
    pool.join()

    total_rows = sum(results)
    logging.info(f"Filas cargadas desde {base_name}: {total_rows}")


# -----------------------------
# Múltiples Archivos Landing Zone
# -----------------------------

def process_directory(input_dir, processed_dir, error_dir, table, db_mode, conn_string, sqlite_path):

    files = glob.glob(os.path.join(input_dir, "*.*"))
    # extension filter
    valid_ext = ('.sav', '.xlsx', '.xls', '.csv')
    files = [f for f in files if f.lower().endswith(valid_ext)]

    if not files:
        logging.info(
            "No hay archivos nuevos en la carpeta data/input. Finalizando proceso.")
        return

    for fp in files:
        filename = os.path.basename(fp)
        try:
            logging.info(f"--- Procesando nuevo archivo: {filename} ---")
            run_etl(fp, table, db_mode, conn_string, sqlite_path)

            # move to success
            dest = os.path.join(processed_dir, filename)
            shutil.move(fp, dest)
            logging.info(f"--> EXITOSO: {filename} movido a data/processed/")

        except Exception as e:
            logging.error(f"--> ERROR procesando {filename}: {str(e)}")
            dest_err = os.path.join(error_dir, filename)
            shutil.move(fp, dest_err)
            logging.info(f"--> FALLIDO: {filename} movido a data/error/")


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    start = time.time()
    logging.info("===== INICIO PROCESO ETL MULTIPROPOSITO =====")

    load_dotenv()

    DB_MODE = os.getenv("DB_MODE", "sqlite").lower()
    TABLE = os.getenv("NOMBRE_TABLA", "py_etl")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_DIR = os.path.join(BASE_DIR, "data", "input")
    PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
    ERROR_DIR = os.path.join(BASE_DIR, "data", "error")

    # Ensure directories exist
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(ERROR_DIR, exist_ok=True)

    # PostgreSQL
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")

    # Only format string if variables exist (to prevent errors in purely SQLite deployments)
    if all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        conn_string = ""

    # SQLite
    SQLITE_PATH = os.getenv(
        "SQLITE_PATH", os.path.join(BASE_DIR, "pruebas.db"))

    # Process all files
    process_directory(INPUT_DIR, PROCESSED_DIR, ERROR_DIR,
                      TABLE, DB_MODE, conn_string, SQLITE_PATH)

    end = time.time()
    logging.info(f"Duración total: {round(end-start, 2)} segundos")
    logging.info("===== FIN PROCESO ETL MULTIPROPOSITO =====")
