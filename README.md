# PyETL - Herramienta de Carga Masiva Optimizada (Excel / SPSS a Base de Datos)

Este repositorio contiene un script ETL (Extract, Transform, Load) desarrollado en Python diseñado para **automatizar la carga masiva** de archivos de datos (en formatos `.xlsx` / `.xls` de Excel y `.sav` de SPSS) hacia una base de datos relacional (PostgreSQL o SQLite).

La herramienta se centra en resolver problemas comunes durante la ingestión de datos asegurando que **el 100% del dataset suba sin errores de tipado**, y fue recientemente optimizada para realizar cargas a altísima velocidad mediante procesamiento en paralelo.

## 🚀 Características Principales

- **Multiformato:** Lee directamente datos ya sea de planillas de cálculo de Microsoft Excel (`.xlsx`, `.xls`) o datasets analíticos de IBM SPSS (`.sav`). Lectura eficiente por bloques de memoria.
- **Procesamiento Paralelo (Multiprocessing):** Maximiza el rendimiento dividiendo la lectura y transformación de los datos entre todos los núcleos (CPU cores) de tu PC de forma simultánea. ¡Transfiere cientos de miles de registros en una fracción de tiempo!
- **0 Errores de Tipado y Precisión Númerica:** Mediante el uso de tipos nativos (Int64 de Pandas), el proceso asegura que los números grandes en SPSS con valores nulos _no_ se modifiquen erróneamente con decimales ocultos (ej `63791.0`), preservándolos de forma fidedigna como texto string en la base final.
- **Creación Dinámica de Tablas:** El script analiza los nombres de las columnas del archivo subido, los sanitiza (minúsculas y convierte espacios a guiones bajos `_`), y recrea la estructura de la tabla en base de datos al vuelo usando tablas Unlogged de alta velocidad en PostgreSQL.
- **Carga Súper Optimizada:** Para PostgreSQL implementa buffers dinámicos en disco/RAM inyectándolos con la sentencia `COPY STDIN WITH CSV`.
- **Logs Detallados:** Genera de manera automática el archivo `etl_proceso.log` para permitirte medir el tiempo que toma el proceso y el progreso de los bloques procesados.

---

## 📋 Requisitos Previos

Para ejecutar la herramienta en tu entorno necesitas:

- [Python 3.8](https://www.python.org/downloads/) o una versión superior instalada en tu equipo.
- (Recomendado) Servidor y Base de Datos PostgreSQL 9.x+ (En su defecto, el script incluye un modo local con SQLite que no requiere servidor).

## 🛠️ Instalación

1.  **Descargar:** Clona localmente este repositorio o descarga el archivo `.zip` en un nuevo directorio en tu equipo.
2.  **Abrir Consola:** Inicia tu terminal (PowerShell, CMD, Git Bash, etc.) y dirígete a la carpeta del proyecto.
    ```bash
    cd c:\Ruta\a\tu\proyecto\PyETL
    ```
3.  **Crear un Entorno Virtual (Recomendado):** Esto aísla las librerías de este script de otros proyectos Python en tu PC de trabajo.
    ```bash
    python -m venv venv
    ```
4.  **Activar el Entorno Virtual:**
    - _En Windows (PowerShell/CMD):_ `.\venv\Scripts\activate`
    - _En MacOS/Linux:_ `source venv/bin/activate`
5.  **Instalar las dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

---

## ⚙️ Configuración (.env)

El proceso lee sus parámetros de conexión y carga de un archivo de configuración llamado `.env`.

Dentro de los archivos del sistema, encontrarás un archivo plantilla llamado `.env.example`. **Saca una copia de este archivo y renómbralo únicamente a `.env`**. Posterior a ello, ábrelo en cualquier editor de texto (como Bloc de Notas) y personaliza las siguientes variables:

**Variables Fundamentales:**

- `DB_MODE`: Modos permitidos: `postgres` o `sqlite`. (Recomendamos postgres).
- `ARCHIVO_EXCEL`: Es la ruta del archivo que quieres cargar a tu base de datos (_Puede ser un archivo de Excel `.xlsx` o un archivo de SPSS `.sav`_).
- `NOMBRE_TABLA`: El nombre de la tabla en la cual se guardarán los datos (Recomendación: que no contenga espacios ni mayúsculas).

**Credenciales si usas DB_MODE=postgres:**

- `DB_USER`: Tu usuario en PostgreSQL.
- `DB_PASSWORD`: Tu contraseña secreta.
- `DB_HOST`: El servidor (ejemplo: `localhost` o la IP de tu base server).
- `DB_PORT`: El puerto, usualmente `5432`.
- `DB_NAME`: El nombre de la base de datos donde se va a crear la tabla.

_Si configuras tu DB_MODE como `sqlite`, ignorará las credenciales de PostgreSQL y solo verificará el parámetro `SQLITE_PATH` que creará un archivo .db para guardar localmente tus datos de prueba._

---

## ▶️ Ejecución del ETL

Una vez configurado tu archivo `.env`, lo único que debes hacer para realizar la carga y transformación es tener activado tu entorno virtual en la terminal, y lanzar el script:

```bash
python etl.py
```

### Comprendiendo los Logs

El proceso es paralelo (multiprocessing), por lo que verás la lectura por grupos en el archivo log `etl_proceso.log`. Ejemplo de una ejecución exitosa:

```text
2026-03-09 11:21:36,496 | INFO | ----- INICIO PROCESO ETL -----
2026-03-09 11:21:36,498 | INFO | Leyendo archivo: 01312026BUMDH040226V2MC_SPSS.sav
2026-03-09 11:22:07,127 | INFO | Tabla PostgreSQL creada correctamente
2026-03-09 11:22:07,128 | INFO | Cores disponibles: 8
2026-03-09 11:22:07,128 | INFO | Chunk size: 50000
2026-03-09 11:22:44,973 | INFO | Total chunks: 9
2026-03-09 11:23:30,355 | INFO | Filas cargadas: 428045
2026-03-09 11:23:31,965 | INFO | Duración total: 115.47 segundos
2026-03-09 11:23:31,966 | INFO | ----- FIN PROCESO ETL -----
```

**Manejo Tras Bambalinas:**

- Para un desempeño máximo, el pipeline creará los chunks intermedios en un archivo CSV temporero usando tu RAM (mediante el espacio `/dev/shm` de estar disponible en Linux, o caché temporero en Windows) el cual se va limpiando conforme se envían las transacciones a PostgreSQL.
