# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 4 · 2 de 3 · Plata: datos limpios, tipados y sin datos personales de más
# MAGIC
# MAGIC **Curso:** P.E. Data Engineer · Azure · **Sesión 08** · Caso Andina Retail (1:1.000)
# MAGIC
# MAGIC **Qué hace:** toma el bronce y deja en `<catálogo>.silver` una tabla confiable por fuente.
# MAGIC
# MAGIC | Tabla de plata | Grano | Qué se resolvió (regla del caso) |
# MAGIC |---|---|---|
# MAGIC | `ventas_ecommerce` | línea de pedido | tipos, precio de la **línea** (no del producto), pedido sin pago se conserva |
# MAGIC | `clientes` | cliente | seudónimo `user_id` para cruzar con clics; sin nombre ni correo |
# MAGIC | `ventas_tienda` | tienda + día + turno | separador y codificación del ERP, tienda validada contra el maestro |
# MAGIC | `clientes_erp` | cliente del ERP | correos mal formados marcados, duplicados sin documento retirados, documento con hash |
# MAGIC | `productos_erp` | producto | última versión de cada producto, con origen (importado o nacional) |
# MAGIC | `eventos_web` | evento | duplicados por reintento retirados, IP fuera |
# MAGIC | `facturas` | factura | dato extraído del PDF; las ilegibles quedan marcadas |
# MAGIC | `logs` | línea de log | nivel, ruta, estado y latencia; las trazas se marcan; IP fuera |
# MAGIC | `puntajes` | cliente + semana | tipos |
# MAGIC | `tipo_cambio` | día calendario | los días sin publicación usan el último valor publicado |
# MAGIC | `tiendas` | tienda | tipos |
# MAGIC
# MAGIC Si un control de calidad falla, el notebook **se detiene**: el job queda en error y la tarea de oro no arranca. Ojo: las tablas de plata que ya se habían guardado antes del fallo quedan escritas; lo que se protege es el oro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parámetros y funciones de apoyo
# MAGIC La celda de abajo lee los parámetros (catálogo, cuenta de storage y fecha de la ingesta), crea el esquema `silver` y define tres funciones que se usan en todo el notebook:
# MAGIC
# MAGIC - `guardar(df, tabla)`: escribe una tabla de plata y anota cuántas filas quedaron.
# MAGIC - `bronce_sql(tabla)`: devuelve, de una tabla de la base transaccional, solo la foto de la fecha de esta corrida.
# MAGIC - `seudonimo(col)`: convierte el identificador del cliente en un código que no lo expone.

# COMMAND ----------

from pyspark.sql import functions as F          # funciones para columnas: F.col, F.sum, F.sha2…
from pyspark.sql.window import Window           # ventanas: cálculos «por grupo» sin perder filas (numerar, arrastrar valores)

# Widgets: parámetros que el job puede enviar. Nombre, valor por defecto y etiqueta.
dbutils.widgets.text("catalog", "pede_dev", "Catálogo")
dbutils.widgets.text("storage_account", "", "Cuenta de storage (la tuya: stpede<alias>dldevcus001)")
dbutils.widgets.text("fecha", "2026-11-18", "Fecha de la ingesta de ADF")

# Se leen los valores de los widgets a variables de Python
CATALOG = dbutils.widgets.get("catalog")
STORAGE = dbutils.widgets.get("storage_account")
# El job envía este valor. Si ejecutas el notebook a mano, escribe TU cuenta de storage en el widget de arriba.
assert STORAGE, "Falta la cuenta de storage: escribe la tuya en el widget «storage_account» (stpede<alias>dldevcus001) y vuelve a ejecutar."
FECHA = dbutils.widgets.get("fecha")
BRONZE, SILVER = f"{CATALOG}.bronze", f"{CATALOG}.silver"       # de dónde se lee y dónde se publica
DELTA = f"abfss://silver@{STORAGE}.dfs.core.windows.net/delta"   # la plata vive en su propio contenedor del lago
RESUMEN = {}                                                     # tabla → filas; lo usa la verificación final

# IF NOT EXISTS: la primera corrida crea el esquema; las siguientes no hacen nada
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

def guardar(df, tabla):
    # Recarga completa: overwrite reemplaza la tabla entera, así correr dos veces no duplica filas.
    # option("path", …) la hace tabla externa: los datos quedan en el contenedor silver del lago.
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("path", f"{DELTA}/{tabla}").saveAsTable(f"{SILVER}.{tabla}"))
    # Se cuenta lo que quedó escrito y se anota en el resumen
    RESUMEN[tabla] = spark.table(f"{SILVER}.{tabla}").count()
    print(f"OK {SILVER}.{tabla:18s} {RESUMEN[tabla]:>8,d} filas")

def bronce_sql(tabla):
    """La foto de la base transaccional que corresponde a esta corrida."""
    # El bronce guarda una foto por fecha de ingesta: aquí se filtra la de FECHA para no mezclar días
    return spark.table(f"{BRONZE}.{tabla}").where(F.col("_load_date") == FECHA)

def seudonimo(col):
    # Mismo seudónimo que usa la web en los eventos: permite cruzar sin exponer al cliente
    # Pasos: se arma el texto «andina-» + id, se le calcula el hash SHA-1, se toman sus 12 primeros caracteres
    # y se antepone «u_». El mismo cliente siempre da el mismo código. Es un seudónimo, no un anonimato: quien conozca la fórmula puede
    # recalcularlo probando ids. En producción se añade una clave secreta («sal») que solo conoce la plataforma.
    return F.concat(F.lit("u_"), F.substring(F.sha1(F.concat(F.lit("andina-"), col.cast("string"))), 1, 12))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Ventas del e-commerce (`ventas_ecommerce`) y clientes
# MAGIC **Grano:** una fila por línea de pedido. El pago entra con `left join`: un pedido pendiente o cancelado **no tiene pago y no debe desaparecer**.
# MAGIC
# MAGIC La celda de abajo sigue cuatro pasos:
# MAGIC
# MAGIC 1. Lee las cinco tablas de la base transaccional y les pone tipos (entero, fecha, decimal).
# MAGIC 2. Las une en una sola tabla de ventas y calcula el importe de cada línea.
# MAGIC 3. Pasa tres controles de calidad: que haya filas, que el grano sea único y sin claves nulas, y que el importe total cuadre con el bronce.
# MAGIC 4. Solo si pasan, guarda `ventas_ecommerce` y `clientes` (esta última con el seudónimo `user_id`).

# COMMAND ----------

# PASO 1 · Lecturas con tipos. cast convierte la columna al tipo indicado; solo se eligen las columnas que se usan.
# Pedidos: quién compró, cuándo, por qué canal y en qué estado quedó
orders = bronce_sql("orders").select(F.col("order_id").cast("int"), F.col("customer_id").cast("int"),
                                     F.col("order_date").cast("date"), "channel", "status")
# Líneas del pedido: producto, cantidad y precio. decimal(10,2) guarda los céntimos exactos (un double redondearía).
items = bronce_sql("order_items").select(F.col("order_id").cast("int"), F.col("product_id").cast("int"),
                                         F.col("quantity").cast("int"), F.col("unit_price").cast("decimal(10,2)"))
# Productos: nombre y categoría
products = bronce_sql("products").select(F.col("product_id").cast("int"), "name", "category")
# Clientes: solo ciudad y fecha de alta. El nombre y el correo no se seleccionan: no pasan a la plata.
customers = bronce_sql("customers").select(F.col("customer_id").cast("int"), "city", F.col("signup_date").cast("date"))
# Pagos: cuándo, con qué método y cuánto
payments = bronce_sql("payments").select(F.col("order_id").cast("int"), F.col("paid_at").cast("timestamp"),
                                         "method", F.col("amount").cast("decimal(10,2)"))

# PASO 2 · Uniones. Un join sin tercer argumento es «inner»: solo quedan las filas que tienen pareja en las dos tablas.
ventas = (orders.alias("o")
    .join(items.alias("i"), "order_id")           # cada pedido se repite una vez por línea: aquí se fija el grano
    .join(products.alias("p"), "product_id")      # agrega nombre y categoría del producto
    .join(customers.alias("c"), "customer_id")    # agrega la ciudad del cliente
    .join(payments.alias("pay"), "order_id", "left")   # left join: conserva todos los pedidos aunque no tengan pago
    # Se eligen las columnas finales y se renombran al español con alias
    .select("order_id", "order_date", F.col("channel").alias("canal"), F.col("status").alias("estado"),
            "customer_id", F.col("city").alias("ciudad"),
            "product_id", F.col("name").alias("producto"), F.col("category").alias("categoria"),
            F.col("quantity").alias("cantidad"), F.col("unit_price").alias("precio_unitario"),
            # importe de la línea = cantidad × precio de la línea (el precio al que se vendió, no el de lista del producto)
            (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)").alias("importe_linea"),
            # columnas del pago: quedan en NULL cuando el pedido no tiene pago
            F.col("method").alias("metodo_pago"), F.col("paid_at").alias("pagado_en"), F.col("amount").alias("importe_pago"),
            F.lit(FECHA).alias("_load_date")))     # de qué foto del bronce salió la fila

# Controles de calidad antes de publicar
# Grano: no puede haber dos filas con el mismo pedido y producto
duplicados = ventas.groupBy("order_id", "product_id").count().where("count > 1").count()
# Las claves no pueden venir vacías
nulos = ventas.where("order_id IS NULL OR product_id IS NULL OR customer_id IS NULL").count()
# Cuadre: el importe total de la plata debe ser igual al de las líneas del bronce.
# Si un join perdiera o repitiera filas, los dos totales dejarían de coincidir.
total_plata = ventas.agg(F.sum("importe_linea")).first()[0]
total_bronce = items.agg(F.sum(F.col("quantity") * F.col("unit_price"))).first()[0]
print("Duplicados en el grano:", duplicados, "| claves nulas:", nulos, "| importe plata:", total_plata, "| bronce:", total_bronce)
# Cada assert detiene el notebook con su mensaje si la condición no se cumple: con errores, el notebook no sigue y el oro no arranca
assert ventas.count() > 0, f"No hay filas de la base transaccional con _load_date = {FECHA}: ¿corrió el bronce con esa fecha?"
assert duplicados == 0 and nulos == 0, "El grano línea de pedido no es único o tiene claves nulas"
assert total_plata == total_bronce, "La plata no cuadra con el bronce"

# PASO 4 · Escrituras (solo se llega aquí si los tres controles pasaron)
guardar(ventas, "ventas_ecommerce")
# Tabla de clientes: el id, su seudónimo (para cruzar con los clics), la ciudad y la fecha de alta
guardar(customers.select("customer_id", seudonimo(F.col("customer_id")).alias("user_id"),
                         F.col("city").alias("ciudad"), "signup_date"), "clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Maestro de tiendas y ventas de tienda física (ERP)
# MAGIC El maestro es la dimensión: una venta de una tienda que **no existe en el maestro** no se puede explicar, así que se cuenta y se informa.
# MAGIC
# MAGIC La celda de abajo guarda primero `tiendas` con sus tipos. Después toma las ventas del ERP, les pone tipos, deja una sola fila por tienda, día y turno, y comprueba que todas las tiendas vendedoras estén en el maestro antes de guardar `ventas_tienda`.

# COMMAND ----------

# Maestro de tiendas: en el bronce todo es texto; aquí cada columna recibe su tipo
tiendas = spark.table(f"{BRONZE}.tiendas").select(
    F.col("tienda_id").cast("int"), "codigo", "nombre", "ciudad", "formato", "horario",
    # «activo» llega como texto "1" o "0": la comparación lo convierte en verdadero o falso
    F.col("fecha_apertura").cast("date"), F.col("m2").cast("int"), (F.col("activo") == "1").alias("activa"))
guardar(tiendas, "tiendas")

# Ventas de tienda física: tipos y una sola fila por grano
ventas_tienda = (spark.table(f"{BRONZE}.erp_ventas")
    .select(F.col("fecha").cast("date"), F.col("tienda_id").cast("int"), "turno", F.col("tickets").cast("int"),
            F.col("unidades").cast("int"), F.col("importe").cast("decimal(12,2)"), "fecha_archivo")
    # dropDuplicates: si la misma tienda, día y turno aparece más de una vez, queda una sola fila
    .dropDuplicates(["fecha", "tienda_id", "turno"]))
# left_anti: devuelve las ventas que NO encuentran su tienda en el maestro. Lo correcto es que no haya ninguna.
sin_maestro = ventas_tienda.join(tiendas, "tienda_id", "left_anti").count()
print("Ventas de tiendas que no están en el maestro:", sin_maestro, "(esperado 0)")
# Control de calidad: con una sola venta sin maestro, el notebook se detiene y la tabla no se publica
assert sin_maestro == 0, "Hay ventas de tiendas sin maestro"
guardar(ventas_tienda, "ventas_tienda")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Clientes y productos del ERP
# MAGIC Tres problemas que el caso anuncia: correos vacíos o mal formados, el mismo cliente con y sin documento, y datos personales. El documento se guarda como **hash**: sirve para distinguir dos clientes sin dejar el número a la vista. Es seudonimización, no anonimato: un DNI tiene pocos dígitos y su hash se puede recalcular probando; en producción se le añade una clave secreta («sal»).
# MAGIC
# MAGIC La celda de abajo tiene dos partes:
# MAGIC
# MAGIC - **Clientes:** marca si el correo tiene forma válida, deja una fila por cliente (prefiere la que trae documento) y guarda el documento y el correo solo como hash.
# MAGIC - **Productos:** el ERP manda el catálogo cada día; se deja la versión más reciente de cada producto.

# COMMAND ----------

cli = (spark.table(f"{BRONZE}.erp_clientes")
    # Documento: se quitan los espacios (trim) y, si queda vacío, se convierte en NULL (nullif)
    .withColumn("documento", F.nullif(F.trim("documento"), F.lit("")))
    # Correo válido: expresión regular «algo@algo.algo», sin espacios ni otra arroba.
    # rlike devuelve NULL si el correo es NULL; coalesce lo convierte en False.
    .withColumn("correo_valido", F.coalesce(F.col("correo").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.lit(False))))
# el mismo cliente (nombre + teléfono) aparece con y sin documento: queda la fila que sí lo trae
# Ventana: agrupa las filas del mismo nombre y teléfono y las ordena. isNull() vale False cuando hay documento,
# y False va antes que True: la fila con documento queda primera. A igualdad, gana el archivo más reciente
# (fecha_archivo descendente), igual que en productos.
orden = Window.partitionBy("nombre", "telefono").orderBy(F.col("documento").isNull(), F.col("fecha_archivo").desc())
# row_number numera las filas de cada grupo (1, 2, 3…); se conserva solo la número 1
cli = cli.withColumn("_n", F.row_number().over(orden)).where("_n = 1")
# Escritura sin datos personales en claro: sha2(…, 256) convierte el documento y el correo en un hash.
# El correo se pasa antes a minúsculas para que «Ana@…» y «ana@…» den el mismo hash. Nombre y teléfono no se guardan.
guardar(cli.select("cliente_erp_id", F.sha2(F.col("documento"), 256).alias("documento_hash"),
                   "correo_valido", F.sha2(F.lower("correo"), 256).alias("correo_hash"), "ciudad", "fecha_archivo"), "clientes_erp")

prod = (spark.table(f"{BRONZE}.erp_productos")
    # Deduplicación con ventana, como en clientes: por cada sku se numeran las filas de la más nueva a la más antigua (desc)…
    .withColumn("_n", F.row_number().over(Window.partitionBy("sku").orderBy(F.col("fecha_archivo").desc())))
    # …y queda solo la primera: la última versión del producto
    .where("_n = 1")
    # Tipos y nombres finales. El sku pasa a llamarse product_id para poder cruzarlo con las ventas del e-commerce.
    .select(F.col("sku").cast("int").alias("product_id"), "nombre", "categoria", F.col("precio_lista").cast("decimal(10,2)"),
            "origen", F.col("costo_usd").cast("decimal(10,2)"), (F.col("activo") == "1").alias("activo")))
guardar(prod, "productos_erp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Eventos web
# MAGIC La app reintenta y manda el mismo evento dos veces (~1,5 %): se deja **uno por `event_id`**. La IP es dato personal y la analítica no la necesita: no pasa a la plata.
# MAGIC
# MAGIC La celda de abajo lee los clics del bronce, quita los duplicados, pone tipos a las columnas, guarda `eventos_web` e imprime cuántos eventos se retiraron.

# COMMAND ----------

eventos = (spark.table(f"{BRONZE}.clics")
    # Deduplicación: de los eventos con el mismo event_id queda uno solo
    .dropDuplicates(["event_id"])
    # Tipos: «ts» pasa de texto a fecha y hora; «fecha» a fecha; los identificadores numéricos a entero.
    # La IP no está en la lista: al no seleccionarla, no pasa a la plata.
    .select("event_id", F.to_timestamp("ts").alias("ts"), F.col("fecha").cast("date"), "session_id", "user_id", "device", "event_type",
            "page", "query", F.col("product_id").cast("int"), F.col("order_id").cast("int"), "campania"))
guardar(eventos, "eventos_web")
# Filas del bronce menos filas de la plata = eventos repetidos que se retiraron
print("Eventos retirados por duplicado:", spark.table(f"{BRONZE}.clics").count() - RESUMEN["eventos_web"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Facturas: el dato que se extrae del PDF
# MAGIC Nadie consulta el PDF directo. Aquí el texto se saca con expresiones regulares porque los PDF del laboratorio guardan su texto sin comprimir. Con facturas reales harías este paso con un servicio de extracción de documentos; **la decisión de zona es la misma**: el archivo queda en bronce y el dato extraído en plata. Las que no traen texto (escaneos) quedan con `legible = false`.
# MAGIC
# MAGIC La celda de abajo convierte los bytes de cada PDF en texto, busca en él la fecha de emisión, el número de pedido, el RUC del cliente y el total, guarda la tabla `facturas` y muestra cuántas facturas resultaron legibles.

# COMMAND ----------

# Los bytes del PDF (columna content) se interpretan como texto. Es una expresión de columna: se reutiliza abajo.
texto = F.decode(F.col("content"), "ISO-8859-1")
# Patrón que se repite en cada campo: regexp_extract devuelve el grupo entre paréntesis o "" si no lo encuentra;
# nullif convierte ese "" en NULL; cast le da el tipo final.
facturas = (spark.table(f"{BRONZE}.facturas_pdf")
            # serie y número: salen del nombre del archivo (F + 3 dígitos, guion, 8 dígitos, .pdf)
    .select(F.regexp_extract("_source_file", r"(F\d{3}-\d{8})\.pdf$", 1).alias("serie_numero"),
            # fecha de la carpeta donde llegó el archivo (fecha=…)
            F.col("fecha").cast("date").alias("fecha_archivo"),
            # fecha de emisión: el texto AAAA-MM-DD que sigue a «Fecha de emision: »
            F.nullif(F.regexp_extract(texto, r"Fecha de emision: (\d{4}-\d{2}-\d{2})", 1), F.lit("")).cast("date").alias("fecha_emision"),
            # número de pedido: los dígitos que siguen a «Pedido: »; permite cruzar la factura con el pedido
            F.nullif(F.regexp_extract(texto, r"Pedido: (\d+)", 1), F.lit("")).cast("int").alias("order_id"),
            # RUC del cliente (11 dígitos): es dato personal, se guarda solo su hash
            F.sha2(F.nullif(F.regexp_extract(texto, r"Cliente RUC: (\d{11})", 1), F.lit("")), 256).alias("ruc_cliente_hash"),
            # total: el número que sigue a «TOTAL: S/ »
            F.nullif(F.regexp_extract(texto, r"TOTAL: S/ ([0-9.]+)", 1), F.lit("")).cast("decimal(12,2)").alias("total"),
            # tamaño del archivo y su ruta, para poder volver al PDF original
            F.col("length").alias("bytes"), "_source_file")
    # Una factura es legible si se pudo extraer el total. Un escaneo no tiene texto: total queda NULL y legible en false.
    .withColumn("legible", F.col("total").isNotNull()))
guardar(facturas, "facturas")
# Resumen: cuántas facturas son legibles y cuántas no, con el importe total de cada grupo
display(spark.sql(f"SELECT legible, COUNT(*) AS facturas, SUM(total) AS importe FROM {SILVER}.facturas GROUP BY legible"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Logs
# MAGIC Tres formas de línea conviven: texto, JSON y trazas de error (las líneas que empiezan con cuatro espacios y `at `, que continúan el error de la línea anterior). Se interpreta lo que se puede y lo demás se marca. La IP no pasa.
# MAGIC
# MAGIC La celda de abajo saca de cada línea el momento, el nivel, la ruta, el estado HTTP y la latencia. Si la línea es JSON, lee sus propiedades; si es texto, usa una expresión regular. Las trazas no tienen esos campos: quedan en `NULL` y marcadas con `es_traza`.

# COMMAND ----------

# Expresión regular para las líneas de texto. Cada paréntesis es un grupo numerado:
# 1 = momento · 2 = nivel (INFO, WARN o ERROR) · 3 = una palabra · 4 = el valor de req=… (3 y 4 no se usan abajo)
# 5 = método (GET o POST) · 6 = ruta · 7 = estado HTTP (3 dígitos) · 8 = latencia en milisegundos
PATRON = r"^(\S+) (INFO|WARN|ERROR) (\w+) req=(\w+) (GET|POST) (\S+) (\d{3}) (\d+)ms"
l = spark.table(f"{BRONZE}.logs")
# Una línea es JSON si empieza con «{». Es una condición de columna que se reutiliza en cada campo.
es_json = F.col("linea").startswith("{")
# Cada campo sigue el mismo esquema: when(es_json, leer la propiedad del JSON).otherwise(leer el grupo del patrón).
# Si el patrón no encaja, regexp_extract devuelve "" y nullif lo convierte en NULL.
logs = l.select(
    F.col("fecha").cast("date"),
    # momento del evento, convertido a fecha y hora
    F.to_timestamp(F.when(es_json, F.get_json_object("linea", "$.ts")).otherwise(F.nullif(F.regexp_extract("linea", PATRON, 1), F.lit("")))).alias("ts"),
    # nivel: INFO, WARN o ERROR
    F.when(es_json, F.get_json_object("linea", "$.level")).otherwise(F.nullif(F.regexp_extract("linea", PATRON, 2), F.lit(""))).alias("nivel"),
    # aplicación: sale del nombre del archivo (lo que va antes de .log.gz)
    F.regexp_extract("_source_file", r"/(\w+)\.log\.gz$", 1).alias("app"),
    # ruta que se pidió
    F.when(es_json, F.get_json_object("linea", "$.ruta")).otherwise(F.nullif(F.regexp_extract("linea", PATRON, 6), F.lit(""))).alias("ruta"),
    # estado HTTP de la respuesta, como entero
    F.when(es_json, F.get_json_object("linea", "$.status")).otherwise(F.nullif(F.regexp_extract("linea", PATRON, 7), F.lit(""))).cast("int").alias("estado_http"),
    # latencia en milisegundos, como entero
    F.when(es_json, F.get_json_object("linea", "$.ms")).otherwise(F.nullif(F.regexp_extract("linea", PATRON, 8), F.lit(""))).cast("int").alias("ms"),
    # traza de error: la línea empieza con cuatro espacios y «at »
    F.col("linea").startswith("    at ").alias("es_traza"),
    # etiqueta con la forma de la línea: json, traza o texto.
    # La línea completa no se selecciona: por eso la IP no llega a la plata.
    F.when(es_json, "json").when(F.col("linea").startswith("    at "), "traza").otherwise("texto").alias("formato"))
guardar(logs, "logs")
# Resumen: cuántas líneas hay de cada formato y nivel (GROUP BY ALL agrupa por las columnas que no son agregados)
display(spark.sql(f"SELECT formato, nivel, COUNT(*) AS lineas FROM {SILVER}.logs GROUP BY ALL ORDER BY 1, 2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Puntajes y tipo de cambio
# MAGIC El tipo de cambio **no llega todos los días** (fines de semana, el feriado del 8-oct y una falla del proveedor el 21-oct). La plata arma el calendario completo y arrastra el último valor publicado: así un pedido del domingo también tiene tipo de cambio.
# MAGIC
# MAGIC La celda de abajo guarda primero `puntajes` (solo cambia los tipos). Después, para el tipo de cambio: genera una fila por cada día del calendario, la une con los días publicados y rellena los huecos con el último valor conocido. La columna `publicado` dice si ese día hubo publicación o si el valor es arrastrado.

# COMMAND ----------

# Puntajes: solo tipos. Las probabilidades y el riesgo pasan de texto a número decimal (double).
guardar(spark.table(f"{BRONZE}.puntajes").select(
    F.col("customer_id").cast("int"), F.col("fecha_score").cast("date"),
    F.col("prob_compra_tecnologia").cast("double"), F.col("prob_compra_electro").cast("double"),
    F.col("prob_compra_hogar").cast("double"), F.col("riesgo_abandono").cast("double"), "modelo_version"), "puntajes")

# Tipo de cambio publicado: una fila por día con publicación. decimal(8,4) conserva los cuatro decimales.
publicado = spark.table(f"{BRONZE}.tipo_cambio").select(F.col("fecha").cast("date"), F.col("compra").cast("decimal(8,4)"), F.col("venta").cast("decimal(8,4)"))
# Primer día (d) y último día (h) con publicación
limites = publicado.agg(F.min("fecha").alias("d"), F.max("fecha").alias("h")).first()
# Calendario: sequence genera la lista de días desde el primero hasta el fin de mes del último (last_day)
# y explode convierte esa lista en una fila por día.
calendario = spark.sql(f"SELECT explode(sequence(DATE'{limites['d']}', last_day(DATE'{limites['h']}'), INTERVAL 1 DAY)) AS fecha")
# Ventana de arrastre: ordenada por fecha, mira desde el primer día hasta la fila actual
arrastre = Window.orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0)
tc = (calendario.join(publicado, "fecha", "left")          # left join: todos los días del calendario, con o sin publicación
      .withColumn("publicado", F.col("venta").isNotNull())   # se marca ANTES de rellenar: después ya no se distinguiría
      # last(…, ignorenulls=True): el último valor no nulo dentro de la ventana, es decir, el último publicado
      .withColumn("venta", F.last("venta", ignorenulls=True).over(arrastre))
      .withColumn("compra", F.last("compra", ignorenulls=True).over(arrastre)))
guardar(tc, "tipo_cambio")
print("Días sin publicación cubiertos con el último valor:", tc.where("NOT publicado").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Verificación de la plata
# MAGIC La celda de abajo compara las filas de cada tabla de plata (`RESUMEN`) con las esperadas para la muestra del curso y cuenta las facturas legibles. Imprime `OK` o `ERROR` en cada línea; si algo no coincide, detiene el notebook y el oro no arranca.

# COMMAND ----------

# Filas esperadas por tabla con la muestra del curso
ESPERADO = {"ventas_ecommerce": 3489, "clientes": 500, "tiendas": 40, "ventas_tienda": 419, "clientes_erp": 980, "productos_erp": 200,
            "eventos_web": 34530, "facturas": 140, "logs": 146282, "puntajes": 2000, "tipo_cambio": 31}
fallas = []   # nombres de las tablas que no coinciden
for tabla, esperado in ESPERADO.items():
    n = RESUMEN.get(tabla)   # filas que guardó este notebook
    if n != esperado:
        fallas.append(tabla)
    # Si la tabla no se guardó, n es None: se escribe «sin cargar» en vez de un número
    print(f"{'OK   ' if n == esperado else 'ERROR'} {tabla:18s} {(f'{n:,d}' if n is not None else 'sin cargar'):>10s} (esperado {esperado:,d})")
# Control adicional: de las 140 facturas, 137 deben ser legibles (las otras 3 son ilegibles)
legibles = spark.table(f"{SILVER}.facturas").where("legible").count()
print(f"{'OK   ' if legibles == 137 else 'ERROR'} facturas legibles  {legibles:>8,d} (esperado 137)")
# Si alguna tabla no coincide o las legibles no son 137, el notebook se detiene y el job queda en error
assert not fallas and legibles == 137, f"Plata con diferencias: {fallas}"

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado para el job.** La última celda termina el notebook y devuelve un resumen en JSON: la etapa, la fecha procesada y las filas de cada tabla. El job lo guarda como salida de la tarea de plata.

# COMMAND ----------

import json
# json.dumps convierte el diccionario en texto; dbutils.notebook.exit termina el notebook y entrega ese texto a quien lo ejecutó
dbutils.notebook.exit(json.dumps({"etapa": "plata", "fecha": FECHA, "filas": RESUMEN}))
