# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 4 · 1 de 3 · Bronce: las fuentes diarias de Andina tal como llegaron
# MAGIC
# MAGIC **Curso:** P.E. Data Engineer · Azure · **Sesión 08** · Caso Andina Retail a escala de laboratorio (1:1.000)
# MAGIC
# MAGIC **Qué hace:** registra en `<catálogo>.bronze` lo que hay en el lago, sin limpiar nada.
# MAGIC
# MAGIC | Fuente del caso | Cómo llegó al lago | Tabla de bronce |
# MAGIC |---|---|---|
# MAGIC | 1 · Base transaccional | ADF (`pl_pede_ingesta`) → `bronze/pedidos_ingesta/<fecha>/` | `customers`, `products`, `orders`, `order_items`, `payments` |
# MAGIC | 2 · CSV del ERP | archivo diario en `bronze/landing/erp/` | `erp_ventas`, `erp_clientes`, `erp_productos` |
# MAGIC | 3 · Clics | JSONL por hora en `bronze/landing/clics/` | `clics` (la carga el job horario, no este notebook) |
# MAGIC | 4 · Facturas y contratos | PDF en `bronze/landing/facturas/` y `contratos/` | `facturas_pdf`, `contratos_pdf` |
# MAGIC | 5 · Logs | `.log.gz` en `bronze/landing/logs/` | `logs` (la carga el job horario, no este notebook) |
# MAGIC | 6 · Puntajes | CSV semanal en `bronze/landing/puntajes/` | `puntajes` |
# MAGIC | 7 · Tipo de cambio | JSON diario en `bronze/landing/tipo_cambio/` | `tipo_cambio` |
# MAGIC | 8 · Maestro de tiendas | CSV en `bronze/landing/tiendas/` | `tiendas` |
# MAGIC
# MAGIC **Regla del bronce:** una fila por registro recibido, con el archivo de origen y el momento de carga. Los duplicados y los datos personales se resuelven en la plata; los tipos también, salvo en las 5 tablas de la base transaccional, que se leen con los tipos que Spark deduce del CSV.
# MAGIC
# MAGIC > Este notebook lo ejecuta el job `job-pede-andina-e4` (tarea `bronce`), que a su vez lo dispara Data Factory. También puedes ejecutarlo a mano con **Run all**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parámetros
# MAGIC La celda de abajo prepara lo que usa el resto del notebook:
# MAGIC
# MAGIC - Crea tres **widgets** (parámetros que el job puede enviar): catálogo, cuenta de storage y `fecha`, que es el nombre de la carpeta que dejó Data Factory.
# MAGIC - Arma las rutas del lago: la ingesta de ADF, la carpeta `landing` y la carpeta donde se guardan las tablas Delta.
# MAGIC - Crea el esquema `bronze` si todavía no existe.

# COMMAND ----------

# Funciones de Spark para columnas (F.col, F.lit…): se usan en todo el notebook
from pyspark.sql import functions as F

# Widgets: nombre, valor por defecto y etiqueta. Data Factory envía la fecha de su corrida en «fecha».
dbutils.widgets.text("catalog", "pede_dev", "Catálogo")
dbutils.widgets.text("storage_account", "", "Cuenta de storage (la tuya: stpede<alias>dldevcus001)")
dbutils.widgets.text("fecha", "2026-11-18", "Fecha de la ingesta de ADF (carpeta pedidos_ingesta)")

# Se leen los valores de los widgets a variables de Python
CATALOG = dbutils.widgets.get("catalog")
STORAGE = dbutils.widgets.get("storage_account")
# El job envía este valor. Si ejecutas el notebook a mano, escribe TU cuenta de storage en el widget de arriba.
assert STORAGE, "Falta la cuenta de storage: escribe la tuya en el widget «storage_account» (stpede<alias>dldevcus001) y vuelve a ejecutar."
FECHA = dbutils.widgets.get("fecha")

# Rutas del lago. abfss://<contenedor>@<cuenta>.dfs.core.windows.net es la forma de nombrar una carpeta de ADLS Gen2.
RAIZ = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
INGESTA = f"{RAIZ}/pedidos_ingesta/{FECHA}"   # lo que copió ADF en la corrida de esa fecha
LANDING = f"{RAIZ}/landing"                   # archivos de las demás fuentes, tal como llegaron
DELTA = f"{RAIZ}/delta"                       # datos de las tablas Delta de bronce
BRONZE = f"{CATALOG}.bronze"                  # esquema de Unity Catalog donde se registran las tablas

# IF NOT EXISTS: la primera corrida crea el esquema; las siguientes no hacen nada
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
# Se imprimen las rutas para revisar a simple vista que los parámetros llegaron bien
print("Ingesta de ADF :", INGESTA)
print("Landing        :", LANDING)
print("Tablas en      :", BRONZE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Funciones de apoyo
# MAGIC `con_auditoria` agrega de dónde vino cada fila. `guardar` escribe la tabla Delta **externa** (los datos quedan en tu lago, no en el almacenamiento del catálogo).
# MAGIC
# MAGIC La celda de abajo solo **define** esas dos funciones y el diccionario `RESUMEN`, donde cada carga anota cuántas filas dejó. Las secciones siguientes las usan.

# COMMAND ----------

# Diccionario tabla → número de filas. Lo usan la verificación final y la salida del notebook.
RESUMEN = {}

def con_auditoria(df):
    # Agrega las dos columnas de auditoría del bronce:
    # _source_file = ruta del archivo de donde salió la fila (Spark la expone en la columna oculta _metadata)
    # _ingested_at = momento de la carga
    return (df.withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))

def guardar(df, tabla):
    """Recarga completa de una fuente de landing: correr dos veces no duplica filas."""
    # mode("overwrite") reemplaza todo el contenido de la tabla; overwriteSchema permite que cambien las columnas.
    # option("path", …) la hace tabla externa: los archivos quedan en la carpeta delta/ del lago.
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("path", f"{DELTA}/{tabla}").saveAsTable(f"{BRONZE}.{tabla}"))
    # Se cuenta lo que quedó escrito (se lee de la tabla, no del DataFrame) y se anota en el resumen
    RESUMEN[tabla] = spark.table(f"{BRONZE}.{tabla}").count()
    print(f"OK {BRONZE}.{tabla:15s} {RESUMEN[tabla]:>8,d} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Fuente 1 · Base transaccional (lo que copió Data Factory)
# MAGIC Cada corrida de ADF deja una **foto completa** de las tablas en `pedidos_ingesta/<fecha>/`. El bronce guarda cada foto con su `_load_date`: si la misma fecha se procesa dos veces, se reemplaza solo esa fecha.
# MAGIC
# MAGIC La celda de abajo repite los mismos pasos para las 5 tablas:
# MAGIC
# MAGIC 1. Lee los CSV de la carpeta de la fecha.
# MAGIC 2. Normaliza los nombres de columna y agrega las columnas de auditoría (`_source_file`, `_load_date`, `_ingested_at`).
# MAGIC 3. Si la tabla ya existe, borra las filas de esa misma fecha y alinea los tipos con los que ya tiene.
# MAGIC 4. Agrega las filas nuevas (`append`) y cuenta cuántas quedaron con esa fecha.

# COMMAND ----------

# Las 5 tablas que el pipeline de ADF copia desde la base transaccional
TABLAS_SQL = ["customers", "products", "orders", "order_items", "payments"]

for tabla in TABLAS_SQL:
    # LECTURA: CSV con cabecera; inferSchema hace que Spark deduzca los tipos (número, fecha…) mirando los datos.
    # El select agrega la ruta del archivo de origen a las columnas del CSV.
    df = (spark.read.option("header", "true").option("inferSchema", "true").csv(f"{INGESTA}/{tabla}/")
          .select("*", "_metadata.file_path"))
    # toDF renombra todas las columnas de una vez: sin espacios sobrantes y en minúsculas
    df = (df.toDF(*[c.strip().lower() for c in df.columns])
            .withColumnRenamed("file_path", "_source_file")       # nombre estándar de la columna de auditoría
            .withColumn("_load_date", F.lit(FECHA))               # a qué foto (fecha de ingesta) pertenece la fila
            .withColumn("_ingested_at", F.current_timestamp()))   # momento de la carga
    destino = f"{BRONZE}.{tabla}"
    if spark.catalog.tableExists(destino):
        # Se borra la foto de esta fecha antes de volver a cargarla: así correr dos veces no duplica filas.
        # (Solo si la tabla tiene _load_date: una tabla creada en sesiones anteriores puede no tenerla.)
        if "_load_date" in spark.table(destino).columns:
            spark.sql(f"DELETE FROM {destino} WHERE _load_date = '{FECHA}'")
        # si la tabla ya existía (de S05 a S07), la carga nueva se alinea con sus tipos
        tipos = dict(spark.table(destino).dtypes)   # diccionario columna → tipo de la tabla actual
        # Cada columna que ya existe en la tabla se convierte (cast) a su tipo; las columnas nuevas pasan tal cual
        df = df.select([F.col(c).cast(tipos[c]).alias(c) if c in tipos else F.col(c) for c in df.columns])
    # ESCRITURA: append agrega filas sin tocar las fotos de otras fechas; mergeSchema acepta columnas nuevas
    (df.write.format("delta").mode("append").option("mergeSchema", "true")
       .option("path", f"{DELTA}/{tabla}").saveAsTable(destino))
    # Se cuentan solo las filas de la fecha de esta corrida (la tabla puede tener fotos de otros días)
    RESUMEN[tabla] = spark.table(destino).where(f"_load_date = '{FECHA}'").count()
    print(f"OK {destino:28s} {RESUMEN[tabla]:>6,d} filas con _load_date = {FECHA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Fuente 2 · CSV del ERP
# MAGIC El caso avisa: *«el separador y la codificación varían entre sucursales»*. Por eso cada archivo se abre mirando primero su cabecera. El bronce **no corrige** el contenido: solo lo lee bien.
# MAGIC
# MAGIC La celda de abajo hace tres cosas:
# MAGIC
# MAGIC 1. Define `leer_csv_erp`, que detecta el separador (`;` o `,`) y la codificación (UTF-8 o ISO-8859-1) de un archivo y lo lee con ellos.
# MAGIC 2. Recorre las carpetas diarias `erp/fecha=…/` y, para cada tipo de archivo (ventas, clientes, productos), une los días en una sola tabla.
# MAGIC 3. Muestra qué separador y qué codificación se detectó en cada día de ventas.

# COMMAND ----------

def leer_csv_erp(ruta):
    # Primero se lee el archivo como texto plano (una fila por línea) para inspeccionarlo sin interpretarlo
    texto = spark.read.text(ruta)
    cabecera = texto.first()["value"]   # la primera línea: los nombres de columna
    # Separador: el carácter que más se repite en la cabecera, punto y coma o coma
    sep = ";" if cabecera.count(";") > cabecera.count(",") else ","
    # Codificación: si al leer como UTF-8 aparece el carácter «�», el archivo venía en otra codificación (ISO-8859-1)
    roto = texto.where(F.col("value").contains("�")).limit(1).count() > 0   # acentos rotos al leer como UTF-8
    cod = "ISO-8859-1" if roto else "UTF-8"
    # Ahora sí se lee como CSV, con el separador y la codificación detectados, y se agrega el archivo de origen
    df = (spark.read.option("header", "true").option("sep", sep).option("encoding", cod).csv(ruta)
          .select("*", "_metadata.file_path").withColumnRenamed("file_path", "_source_file"))
    # Se deja anotado qué se detectó: sirve para auditar y para la consulta del final de la celda
    return df.withColumn("_separador", F.lit(sep)).withColumn("_codificacion", F.lit(cod))

# Lista ordenada de las carpetas diarias del ERP (solo las que se llaman fecha=…)
dias_erp = sorted(f.path for f in dbutils.fs.ls(f"{LANDING}/erp/") if "fecha=" in f.path)
for archivo in ["ventas", "clientes", "productos"]:
    partes = []   # un DataFrame por día
    for dia in dias_erp:
        # La fecha sale del nombre de la carpeta: lo que viene después de «fecha=»
        fecha_archivo = dia.rstrip("/").split("fecha=")[1]
        # Cada día se lee por separado (puede traer otro separador u otra codificación) y se le anota su fecha
        partes.append(leer_csv_erp(f"{dia.rstrip('/')}/{archivo}.csv").withColumn("fecha_archivo", F.lit(fecha_archivo)))
    # Se apilan los días. unionByName une por nombre de columna, no por posición.
    df = partes[0]
    for p in partes[1:]:
        df = df.unionByName(p)
    # Se agrega el momento de carga y se guarda como erp_ventas, erp_clientes o erp_productos
    guardar(df.withColumn("_ingested_at", F.current_timestamp()), f"erp_{archivo}")

# Comprobación visual: por cada día de ventas, qué separador y codificación se detectó y cuántas filas trajo.
# GROUP BY ALL agrupa por todas las columnas que no son agregados; ORDER BY 1 ordena por la primera columna.
display(spark.sql(f"SELECT fecha_archivo, _separador, _codificacion, COUNT(*) AS filas FROM {BRONZE}.erp_ventas GROUP BY ALL ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Fuentes 3 y 5 · Clics y logs (no se cargan aquí)
# MAGIC Llegan todo el tiempo, así que tienen su propia carga: el notebook `e4_00_clics_logs_horario`, que el job `job-pede-andina-e4-horario` ejecuta cada hora con Auto Loader. Este notebook solo comprueba que esas dos tablas existen y anota cuántas filas tienen hoy.
# MAGIC
# MAGIC Si falta alguna de las dos, la celda de abajo detiene el notebook con un mensaje que dice qué job ejecutar primero.

# COMMAND ----------

for tabla in ("clics", "logs"):
    # assert: si la condición es falsa, el notebook se detiene y muestra el mensaje.
    # Aquí la condición es que la tabla ya exista (la crea el job horario, no este notebook).
    assert spark.catalog.tableExists(f"{BRONZE}.{tabla}"), (
        f"No existe {BRONZE}.{tabla}: ejecuta primero el job job-pede-andina-e4-horario (notebook e4_00_clics_logs_horario).")
    # Solo se cuenta lo que hay y se anota en el resumen: no se lee ni se escribe ningún archivo
    RESUMEN[tabla] = spark.table(f"{BRONZE}.{tabla}").count()
    print(f"OK {BRONZE}.{tabla:15s} {RESUMEN[tabla]:>8,d} filas (carga horaria)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Fuente 4 · Facturas y contratos (PDF)
# MAGIC Un PDF no tiene filas ni columnas. El bronce guarda **el inventario y el archivo tal cual** (`binaryFile`); el dato útil se extrae en la plata.
# MAGIC
# MAGIC La celda de abajo crea dos tablas, `facturas_pdf` y `contratos_pdf`, con una fila por archivo: su ruta, su tamaño, su fecha de modificación y su contenido en bytes.

# COMMAND ----------

# Pares (carpeta de landing, tabla de bronce)
for origen, tabla in [("facturas", "facturas_pdf"), ("contratos", "contratos_pdf")]:
    # Formato binaryFile: una fila por archivo, con las columnas path, modificationTime, length y content (los bytes).
    # pathGlobFilter se queda solo con los .pdf; recursiveFileLookup en false respeta las carpetas tipo fecha=… como columnas.
    df = (spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").option("recursiveFileLookup", "false")
          .load(f"{LANDING}/{origen}/"))
    # «path» pasa a llamarse _source_file, como en las demás tablas de bronce, y se agrega el momento de carga
    guardar(df.withColumnRenamed("path", "_source_file").withColumn("_ingested_at", F.current_timestamp()), tabla)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Fuentes 6, 7 y 8 · Puntajes, tipo de cambio y maestro de tiendas
# MAGIC Tres fuentes pequeñas que se cargan igual: se lee la carpeta completa, se agregan las columnas de auditoría con `con_auditoria` y se reemplaza la tabla con `guardar`. Los puntajes y las tiendas llegan en CSV; el tipo de cambio, en JSON.

# COMMAND ----------

# CSV con cabecera y sin inferSchema: todas las columnas quedan como texto (los tipos se ponen en la plata)
guardar(con_auditoria(spark.read.option("header", "true").csv(f"{LANDING}/puntajes/")), "puntajes")
# JSON: Spark deduce las columnas a partir de las propiedades de cada registro
guardar(con_auditoria(spark.read.json(f"{LANDING}/tipo_cambio/")), "tipo_cambio")
# Maestro de tiendas: mismo tratamiento que los puntajes
guardar(con_auditoria(spark.read.option("header", "true").csv(f"{LANDING}/tiendas/")), "tiendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Verificación del bronce
# MAGIC Estos son los conteos de la muestra del curso. Si generaste tus propios datos, cambia `ESPERADO` por los de tu `esperado.json`.
# MAGIC
# MAGIC La celda de abajo compara, tabla por tabla, las filas cargadas (`RESUMEN`) con las esperadas. Imprime `OK` o `ERROR` en cada una y, si alguna no coincide, detiene el notebook: así la plata no arranca sobre un bronce incompleto.

# COMMAND ----------

# Filas esperadas por tabla con la muestra del curso
ESPERADO = {"customers": 500, "products": 200, "orders": 1500, "order_items": 3489, "payments": 1425,
            "erp_ventas": 419, "erp_clientes": 1045, "erp_productos": 440, "clics": 35043,
            "facturas_pdf": 140, "contratos_pdf": 10, "logs": 146282, "puntajes": 2000, "tipo_cambio": 20, "tiendas": 40}

fallas = []   # nombres de las tablas que no coinciden
for tabla, esperado in ESPERADO.items():
    n = RESUMEN.get(tabla)   # filas que cargó este notebook (o que contó, en clics y logs)
    estado = "OK   " if n == esperado else "ERROR"
    if n != esperado:
        fallas.append(tabla)
    # Si la tabla no se cargó, n es None: se escribe «sin cargar» en vez de un número
    print(f"{estado} {tabla:15s} {(f'{n:,d}' if n is not None else 'sin cargar'):>10s} (esperado {esperado:,d})")
# Si la lista de fallas no está vacía, el notebook se detiene aquí y el job queda en error.
# El mensaje dice qué revisar: landing completo, las 5 tablas de ADF y, para clics y logs, la llegada tardía.
assert not fallas, (f"Bronce incompleto en: {fallas}. Revisa que subiste landing/ completo y que ADF copió las 5 tablas. "
                    "Si fallan clics o logs: sube llegada-tardia/ y ejecuta otra vez el job job-pede-andina-e4-horario.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado para el job.** La última celda termina el notebook y devuelve un resumen en JSON: la etapa, la fecha procesada, cuántas tablas se cargaron y las filas de cada una. El job lo guarda como salida de la tarea `bronce`.

# COMMAND ----------

import json
# json.dumps convierte el diccionario en texto; dbutils.notebook.exit termina el notebook y entrega ese texto a quien lo ejecutó
dbutils.notebook.exit(json.dumps({"etapa": "bronce", "fecha": FECHA, "tablas": len(RESUMEN), "filas": RESUMEN}))
