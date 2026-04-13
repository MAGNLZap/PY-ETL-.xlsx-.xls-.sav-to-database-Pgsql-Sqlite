import pandas as pd
import os
import time
import logging
import tempfile
import multiprocessing as mp
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
        logging.FileHandler("etl_proceso.log", encoding="utf-8"),
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
    DROP TABLE IF EXISTS {table};

    CREATE UNLOGGED TABLE {table} (
        {cols_sql}
    );
    """

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    logging.info("Tabla PostgreSQL creada correctamente con tipos inferidos")


def create_table_sqlite(db_path, table, df):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cols_sql_list = []
    for col, dtype in df.dtypes.items():
        type_sql = map_dtype_to_sqlite(dtype)
        cols_sql_list.append(f'"{col}" {type_sql}')

    cols_sql = ", ".join(cols_sql_list)

    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(f"CREATE TABLE {table} ({cols_sql})")

    conn.commit()
    conn.close()

    logging.info("Tabla SQLite creada correctamente con tipos inferidos")


# -----------------------------
# COPY PostgreSQL
# -----------------------------

def copy_csv_to_postgres(conn_string, table, csv_file):

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            with open(csv_file, "r", encoding="utf-8") as f:
                # En psycopg 3, cur.copy() abre un canal y hay que enviarle los datos explícitamente
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

        # Usar dtype=object evita que Pandas intente adivinar el tipo y convierta
        # secuencias de texto numérico (ej. "010102") en enteros, respetando el 
        # tipo configurado originalmente en Excel.
        df = pd.read_excel(file_path, dtype=object)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i+chunk_size]

    else:
        raise Exception("Formato de archivo no soportado")


# -----------------------------
# ETL principal
# -----------------------------

def run_etl(file_path, table, db_mode, conn_string, sqlite_path, chunk_size=50000):

    logging.info(f"Leyendo archivo: {file_path}")

    chunks = read_file_chunks(file_path, chunk_size)

    first_chunk = next(chunks)

    first_chunk.columns = clean_columns(first_chunk.columns)

    first_chunk_formatted = format_dataframe(first_chunk)

    # Crear tabla según DB y tipos del chunk
    if db_mode == "postgres":
        create_table_postgres(conn_string, table, first_chunk_formatted)

    else:
        create_table_sqlite(sqlite_path, table, first_chunk_formatted)

    temp_dir = get_temp_dir()

    cores = mp.cpu_count()

    logging.info(f"Cores disponibles: {cores}")
    logging.info(f"Chunk size: {chunk_size}")

    pool = mp.Pool(cores)

    tasks = []

    idx = 0

    tasks.append((first_chunk_formatted, table, db_mode,
                 conn_string, sqlite_path, temp_dir, idx))
    idx += 1

    for chunk in chunks:

        chunk.columns = first_chunk.columns

        tasks.append((format_dataframe(chunk), table, db_mode,
                     conn_string, sqlite_path, temp_dir, idx))

        idx += 1

    logging.info(f"Total chunks: {len(tasks)}")

    results = pool.map(process_chunk, tasks)

    total_rows = sum(results)

    logging.info(f"Filas cargadas: {total_rows}")


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    start = time.time()

    logging.info("----- INICIO PROCESO ETL -----")

    load_dotenv()

    DB_MODE = os.getenv("DB_MODE", "sqlite").lower()

    FILE_PATH = os.getenv("ARCHIVO_EXCEL")

    TABLE = os.getenv("NOMBRE_TABLA", "py_vis")

    # PostgreSQL
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")

    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # SQLite
    SQLITE_PATH = os.getenv("SQLITE_PATH", "pruebas.db")

    run_etl(
        FILE_PATH,
        TABLE,
        DB_MODE,
        conn_string,
        SQLITE_PATH
    )

    end = time.time()

    logging.info(f"Duración total: {round(end-start, 2)} segundos")
    logging.info("----- FIN PROCESO ETL -----")
