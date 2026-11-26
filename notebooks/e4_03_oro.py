# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 4 · 3 de 3 · Oro: las 8 preguntas de Andina, respondidas
# MAGIC
# MAGIC **Curso:** P.E. Data Engineer · Azure · **Sesión 08** · Caso Andina Retail (1:1.000)
# MAGIC
# MAGIC El diseño no se justifica por tecnología: se justifica por las preguntas de la sección 4 del caso. Cada tabla de `<catálogo>.gold` responde **una**.
# MAGIC
# MAGIC | # | Pregunta del caso | Tabla de oro | Fuentes |
# MAGIC |---|---|---|---|
# MAGIC | 1 | ¿Cuánto vendimos? | `ventas_diarias` | 1, 2, 8 |
# MAGIC | 2 | ¿Cuánto vale cada cliente? | `clientes_valor` | 1 |
# MAGIC | 3 | ¿Qué productos mueven el negocio? | `productos_rotacion` | 1, 3 |
# MAGIC | 4 | ¿Por qué la web no convierte? | `embudo_web` | 1, 3 |
# MAGIC | 5 | ¿Los pagos cuadran? | `conciliacion_pagos` | 1, 4 |
# MAGIC | 6 | ¿El dólar nos comió el margen? | `ventas_importados_usd` | 1, 2, 7 |
# MAGIC | 7 | ¿A quién le escribimos en la próxima campaña? | `segmentos_campania` | 1, 6 |
# MAGIC | 8 | ¿Qué dato entró, cuándo y quién puede verlo? | `auditoria_cargas` | todas |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parámetros
# MAGIC La celda de abajo lee los parámetros (catálogo, cuenta de storage y fecha), define los nombres cortos de los esquemas que usa (`S` para la plata y `G` para el oro), crea el esquema `gold` y define `publicar`: la función que ejecuta una consulta SQL y guarda su resultado como tabla de oro. Todas las preguntas se responden con ella.

# COMMAND ----------

# Funciones de Spark para columnas; aquí solo se usa F.lit, en la tabla de auditoría
from pyspark.sql import functions as F

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
# Nombres cortos de los esquemas: S = plata, G = oro. Dentro de las consultas se escriben {S} y {G}.
# Las siete primeras preguntas leen solo de la plata, que ya está limpia; la auditoría (sección 9) inventaría las tres zonas.
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
DELTA = f"abfss://gold@{STORAGE}.dfs.core.windows.net/delta"   # el oro vive en su propio contenedor del lago
RESUMEN = {}                                                   # tabla → filas; es la salida del notebook

# IF NOT EXISTS: la primera corrida crea el esquema; las siguientes no hacen nada
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {G}")

def publicar(tabla, consulta):
    # Ejecuta la consulta y guarda el resultado como tabla Delta externa en el esquema gold.
    # overwrite: cada corrida recalcula la tabla completa, así no se acumulan ni se duplican filas.
    (spark.sql(consulta).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("path", f"{DELTA}/{tabla}").saveAsTable(f"{G}.{tabla}"))
    # Se cuenta lo que quedó escrito y se anota en el resumen
    RESUMEN[tabla] = spark.table(f"{G}.{tabla}").count()
    print(f"OK {G}.{tabla:22s} {RESUMEN[tabla]:>7,d} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Pregunta 1 · ¿Cuánto vendimos? (por día, canal y tienda)
# MAGIC Une dos mundos que no se tocan en origen: el e-commerce (base transaccional, solo pedidos **pagados**) y las tiendas físicas (ERP).
# MAGIC
# MAGIC La celda de abajo publica `ventas_diarias` con una fila por día y canal para el e-commerce y una fila por día y tienda para las tiendas físicas. Al final muestra el total de operaciones e importe por canal.

# COMMAND ----------

# La consulta tiene dos mitades con las mismas columnas, pegadas con UNION ALL (una debajo de la otra):
# 1) E-commerce: solo líneas de pedidos pagados, agrupadas por día y canal. Como una tienda no aplica aquí,
#    las tres columnas de tienda van en NULL (CAST(NULL AS …) les da el tipo correcto para poder unir).
#    operaciones = pedidos distintos (COUNT(DISTINCT order_id), porque cada pedido tiene varias líneas).
# 2) Tiendas físicas: ventas del ERP unidas al maestro (JOIN … USING (tienda_id)) para traer nombre y ciudad.
#    El canal se fija en 'tienda' y operaciones = suma de tickets.
publicar("ventas_diarias", f"""
SELECT order_date AS fecha, canal, CAST(NULL AS INT) AS tienda_id, CAST(NULL AS STRING) AS tienda, CAST(NULL AS STRING) AS ciudad_tienda,
       COUNT(DISTINCT order_id) AS operaciones, SUM(cantidad) AS unidades, SUM(importe_linea) AS importe
FROM {S}.ventas_ecommerce WHERE estado = 'pagado'
GROUP BY order_date, canal
UNION ALL
SELECT v.fecha, 'tienda', v.tienda_id, t.nombre, t.ciudad, SUM(v.tickets), SUM(v.unidades), SUM(v.importe)
FROM {S}.ventas_tienda v JOIN {S}.tiendas t USING (tienda_id)
GROUP BY v.fecha, v.tienda_id, t.nombre, t.ciudad
""")
# Comprobación visual: operaciones e importe por canal, del mayor al menor
display(spark.sql(f"SELECT canal, SUM(operaciones) AS operaciones, SUM(importe) AS importe FROM {G}.ventas_diarias GROUP BY canal ORDER BY importe DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Pregunta 2 · ¿Cuánto vale cada cliente?
# MAGIC Sin nombre ni correo: para esta pregunta basta distinguir un cliente de otro.
# MAGIC
# MAGIC La celda de abajo publica `clientes_valor`: una fila por cliente con sus pedidos pagados, el importe total, el ticket promedio y las fechas de su primera y su última compra.

# COMMAND ----------

# Una fila por cliente (GROUP BY customer_id, ciudad), solo con pedidos pagados:
# pedidos = pedidos distintos · importe = suma de sus líneas · ticket_promedio = importe ÷ pedidos, con 2 decimales
# primera_compra y ultima_compra = la fecha más antigua y la más reciente (MIN y MAX de order_date)
publicar("clientes_valor", f"""
SELECT customer_id, ciudad, COUNT(DISTINCT order_id) AS pedidos, SUM(importe_linea) AS importe,
       CAST(SUM(importe_linea) / COUNT(DISTINCT order_id) AS DECIMAL(12,2)) AS ticket_promedio,
       MIN(order_date) AS primera_compra, MAX(order_date) AS ultima_compra
FROM {S}.ventas_ecommerce WHERE estado = 'pagado'
GROUP BY customer_id, ciudad
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Pregunta 3 · ¿Qué productos mueven el negocio?
# MAGIC Lo vendido (base transaccional) junto a lo mirado y lo agregado al carrito (clics). Los clics solo cubren la última semana.
# MAGIC
# MAGIC La celda de abajo publica `productos_rotacion`: una fila por producto vendido con sus unidades e importe, y al lado cuántas veces se vio su página y cuántas veces se agregó al carrito.

# COMMAND ----------

# La consulta arma tres resultados intermedios con WITH (cada uno es una subconsulta con nombre) y luego los une:
# - vendido: unidades e importe por producto, solo pedidos pagados.
# - visto: vistas por producto. El id no viene en una columna: se saca de la dirección de la página
#   ('/producto/123' → 123) con regexp_extract, solo en los eventos page_view de páginas de producto.
# - carrito: veces que cada producto se agregó al carrito (eventos add_to_cart).
# Unión final con LEFT JOIN desde «vendido»: todos los productos vendidos aparecen, aunque nadie los haya visto.
# COALESCE(…, 0) cambia por 0 el NULL de los productos sin vistas o sin carritos.
publicar("productos_rotacion", f"""
WITH vendido AS (
  SELECT product_id, producto, categoria, SUM(cantidad) AS unidades, SUM(importe_linea) AS importe
  FROM {S}.ventas_ecommerce WHERE estado = 'pagado' GROUP BY ALL),
visto AS (
  SELECT CAST(regexp_extract(page, '/producto/([0-9]+)', 1) AS INT) AS product_id, COUNT(*) AS vistas
  FROM {S}.eventos_web WHERE event_type = 'page_view' AND page LIKE '/producto/%' GROUP BY 1),
carrito AS (
  SELECT product_id, COUNT(*) AS agregados_al_carrito FROM {S}.eventos_web WHERE event_type = 'add_to_cart' GROUP BY 1)
SELECT v.*, COALESCE(vi.vistas, 0) AS vistas_semana, COALESCE(c.agregados_al_carrito, 0) AS carritos_semana
FROM vendido v LEFT JOIN visto vi USING (product_id) LEFT JOIN carrito c USING (product_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Pregunta 4 · ¿Por qué la web no convierte?
# MAGIC El embudo por sesión: visita → búsqueda → carrito → checkout.
# MAGIC
# MAGIC La celda de abajo publica `embudo_web`: por día y dispositivo, cuántas sesiones hubo, cuántas buscaron, cuántas agregaron al carrito, cuántas llegaron al checkout y el porcentaje de conversión.

# COMMAND ----------

# Dos pasos:
# 1) «sesiones»: una fila por sesión. Para cada etapa del embudo se pregunta si la sesión tuvo al menos un evento
#    de ese tipo: (event_type = 'search') da verdadero o falso por evento y MAX devuelve verdadero si alguno lo fue.
# 2) Se agrupa por día y dispositivo. CAST(… AS INT) convierte verdadero/falso en 1/0, así SUM cuenta las sesiones
#    que llegaron a cada etapa. conversion_pct = sesiones con checkout ÷ sesiones totales × 100, con 2 decimales.
publicar("embudo_web", f"""
WITH sesiones AS (
  SELECT fecha, device, session_id,
         MAX(event_type = 'search') AS busco, MAX(event_type = 'add_to_cart') AS carrito, MAX(event_type = 'checkout') AS checkout
  FROM {S}.eventos_web GROUP BY fecha, device, session_id)
SELECT fecha, device, COUNT(*) AS sesiones, SUM(CAST(busco AS INT)) AS con_busqueda, SUM(CAST(carrito AS INT)) AS con_carrito,
       SUM(CAST(checkout AS INT)) AS con_checkout, ROUND(100 * SUM(CAST(checkout AS INT)) / COUNT(*), 2) AS conversion_pct
FROM sesiones GROUP BY fecha, device
""")
# Se muestra el embudo completo, ordenado por día y dispositivo
display(spark.sql(f"SELECT * FROM {G}.embudo_web ORDER BY fecha, device"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Pregunta 5 · ¿Los pagos cuadran?
# MAGIC Pedido contra pago y contra factura. Tres situaciones que hay que poder nombrar: pedido sin pago (pendiente o cancelado), pago que no coincide y factura que no coincide. Las facturas **ilegibles** no traen número de pedido, así que no se pueden cruzar con ninguno: se cuentan aparte.
# MAGIC
# MAGIC La celda de abajo publica `conciliacion_pagos`: una fila por pedido con su importe, su pago, su factura (si se pudo cruzar) y una columna `situacion` que dice en palabras si cuadra o qué falla. Al final muestra cuántos pedidos hay en cada situación y cuántas facturas quedaron sin cruzar por ilegibles.

# COMMAND ----------

# 1) «pedido»: la plata tiene una fila por línea; aquí se vuelve a una fila por pedido.
#    importe_pedido = suma de las líneas. El pago viene repetido en cada línea del pedido: MAX lo toma una sola vez.
# 2) LEFT JOIN con las facturas legibles (las únicas que traen número de pedido): todos los pedidos se conservan,
#    tengan o no factura.
# 3) CASE asigna la situación. Las condiciones se evalúan en orden y gana la primera que se cumple:
#    estado distinto de 'pagado' → 'sin pago (estado)' (|| une textos) · pedido pagado sin fila de pago (NULL) →
#    'pagado sin pago registrado' (una comparación con NULL no da ni verdadero ni falso: hay que preguntarlo aparte)
#    · pago distinto del importe → 'pago no coincide'
#    · total de la factura distinto del pago → 'factura no coincide' · en cualquier otro caso → 'cuadra'.
#    Un pedido sin factura cruzada (la muestra solo trae facturas de una semana) no es un error: queda en 'cuadra'.
publicar("conciliacion_pagos", f"""
WITH pedido AS (
  SELECT order_id, order_date, estado, SUM(importe_linea) AS importe_pedido, MAX(importe_pago) AS importe_pago
  FROM {S}.ventas_ecommerce GROUP BY order_id, order_date, estado)
SELECT p.*, f.serie_numero, f.total AS total_factura, f.legible AS factura_legible,
       CASE WHEN p.estado <> 'pagado' THEN 'sin pago (' || p.estado || ')'
            WHEN p.importe_pago IS NULL THEN 'pagado sin pago registrado'
            WHEN p.importe_pago <> p.importe_pedido THEN 'pago no coincide'
            WHEN f.total IS NOT NULL AND f.total <> p.importe_pago THEN 'factura no coincide'
            ELSE 'cuadra' END AS situacion
FROM pedido p
LEFT JOIN (
  -- las facturas ilegibles no traen número de pedido: se conservan aparte, sin cruzar
  SELECT * FROM {S}.facturas WHERE legible) f USING (order_id)
""")
# Resumen: pedidos e importe por situación, de la más frecuente a la menos frecuente
display(spark.sql(f"SELECT situacion, COUNT(*) AS pedidos, SUM(importe_pedido) AS importe FROM {G}.conciliacion_pagos GROUP BY situacion ORDER BY pedidos DESC"))
# Facturas legibles (se cruzaron con su pedido) e ilegibles (quedaron sin cruzar)
display(spark.sql(f"SELECT legible, COUNT(*) AS facturas FROM {S}.facturas GROUP BY legible"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Pregunta 6 · ¿El dólar nos comió el margen?
# MAGIC Venta de productos **importados** valorizada con el tipo de cambio vigente de cada día. Así se separa el efecto de la venta del efecto del dólar.
# MAGIC
# MAGIC La celda de abajo publica `ventas_importados_usd`: una fila por día con la venta de importados en soles y en dólares, su costo en dólares y el margen que queda.

# COMMAND ----------

# Se cruzan tres tablas de plata:
# - ventas del e-commerce con productos del ERP (por product_id), para saber el origen y el costo en dólares;
# - con el tipo de cambio del mismo día del pedido (tc.fecha = v.order_date). La plata tiene todos los días del
#   calendario, así que un pedido de fin de semana también encuentra su tipo de cambio.
# Filtro: solo pedidos pagados de productos importados. Una fila por día (GROUP BY):
# importe_pen = venta en soles · importe_usd = venta en soles ÷ tipo de cambio de venta
# costo_usd = suma de cantidad × costo unitario · margen_usd = importe_usd − costo_usd
# tc_publicado_ese_dia avisa si el tipo de cambio es de ese día o arrastrado del último publicado.
# Antes de cruzar: todo pedido pagado debe tener tipo de cambio en su fecha. El JOIN de abajo descarta en silencio
# las filas sin pareja, así que se comprueba primero (LEFT ANTI JOIN = filas de la izquierda SIN pareja a la derecha).
sin_tc = spark.sql(f"""SELECT COUNT(*) AS n FROM {S}.ventas_ecommerce v
                       LEFT ANTI JOIN {S}.tipo_cambio tc ON tc.fecha = v.order_date WHERE v.estado = 'pagado'""").first()["n"]
assert sin_tc == 0, f"{sin_tc} líneas de pedidos pagados no tienen tipo de cambio en su fecha: revisa silver.tipo_cambio"
publicar("ventas_importados_usd", f"""
SELECT v.order_date AS fecha, tc.venta AS tipo_cambio, tc.publicado AS tc_publicado_ese_dia,
       SUM(v.importe_linea) AS importe_pen, CAST(SUM(v.importe_linea) / tc.venta AS DECIMAL(12,2)) AS importe_usd,
       CAST(SUM(v.cantidad * p.costo_usd) AS DECIMAL(12,2)) AS costo_usd,
       CAST(SUM(v.importe_linea) / tc.venta - SUM(v.cantidad * p.costo_usd) AS DECIMAL(12,2)) AS margen_usd
FROM {S}.ventas_ecommerce v
JOIN {S}.productos_erp p USING (product_id)
JOIN {S}.tipo_cambio tc ON tc.fecha = v.order_date
WHERE v.estado = 'pagado' AND p.origen = 'importado'
GROUP BY v.order_date, tc.venta, tc.publicado
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Pregunta 7 · ¿A quién le escribimos en la próxima campaña?
# MAGIC El puntaje más reciente por cliente, con un segmento accionable. Quien recibe esta tabla **no recibe el dato crudo del cliente**: solo el identificador.
# MAGIC
# MAGIC La celda de abajo publica `segmentos_campania`: una fila por cliente con la categoría que más probablemente compre, esa probabilidad, sus pedidos y el segmento (`retener`, `ofrecer` o `mantener`). Al final muestra cuántos clientes hay en cada segmento.

# COMMAND ----------

# 1) «ultimo»: de la tabla de puntajes se toma solo la semana más reciente (la de mayor fecha_score).
# 2) categoria_sugerida: greatest devuelve la mayor de las tres probabilidades y el CASE mira cuál de ellas es
#    para escribir el nombre de la categoría. prob_compra guarda ese valor máximo.
# 3) pedidos_pagados: se trae de la tabla de oro clientes_valor con LEFT JOIN; un cliente sin pedidos pagados queda en 0.
# 4) segmento, en orden de prioridad: riesgo de abandono de 0,5 o más → 'retener';
#    si no, probabilidad de compra de 0,8 o más → 'ofrecer'; el resto → 'mantener'.
publicar("segmentos_campania", f"""
WITH ultimo AS (SELECT * FROM {S}.puntajes WHERE fecha_score = (SELECT MAX(fecha_score) FROM {S}.puntajes))
SELECT u.customer_id, u.fecha_score, u.modelo_version, u.riesgo_abandono,
       CASE greatest(prob_compra_tecnologia, prob_compra_electro, prob_compra_hogar)
            WHEN prob_compra_tecnologia THEN 'Tecnología' WHEN prob_compra_electro THEN 'Electro' ELSE 'Hogar' END AS categoria_sugerida,
       greatest(prob_compra_tecnologia, prob_compra_electro, prob_compra_hogar) AS prob_compra,
       COALESCE(c.pedidos, 0) AS pedidos_pagados,
       CASE WHEN u.riesgo_abandono >= 0.5 THEN 'retener'
            WHEN greatest(prob_compra_tecnologia, prob_compra_electro, prob_compra_hogar) >= 0.8 THEN 'ofrecer'
            ELSE 'mantener' END AS segmento
FROM ultimo u LEFT JOIN {G}.clientes_valor c USING (customer_id)
""")
# Resumen: clientes por segmento, del más numeroso al menos numeroso
display(spark.sql(f"SELECT segmento, COUNT(*) AS clientes FROM {G}.segmentos_campania GROUP BY segmento ORDER BY clientes DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8b · EXTENSIÓN PROPIA (E1) · ¿Qué tiendas físicas concentran la facturación?
# MAGIC Novena tabla de oro, propia de Gabriela Ramos: el modelo del curso responde cuánto se vende por día y tienda, pero no **qué tiendas concentran la facturación**. Esta tabla ordena las tiendas por importe y calcula su peso y el acumulado (lectura tipo Pareto).
# MAGIC
# MAGIC La celda de abajo publica `rendimiento_tiendas`: una fila por tienda con tickets, unidades, importe, porcentaje de la facturación de tiendas, porcentaje acumulado y posición en el ranking. **Control de amarre:** `SUM(importe)` de esta tabla debe ser **1.003.274,99**, igual que el importe de tiendas en `ventas_diarias` (se verifica en la sección 10).
# MAGIC Va antes de la auditoría para que `auditoria_cargas` la inventaríe en cada corrida.

# COMMAND ----------

# Una fila por tienda (GROUP BY), sobre las ventas del ERP ya limpias en plata, unidas al maestro de tiendas.
# importe = suma de todas sus ventas de la semana · pct_facturacion = su importe ÷ importe de todas las tiendas × 100.
# SUM(SUM(importe)) OVER (...) es una función de ventana sobre el agregado: sin ORDER BY da el total de todas las tiendas;
# con ORDER BY importe DESC va sumando de la tienda que más factura hacia abajo (pct_acumulado, lectura tipo Pareto).
# tienda_id como segundo criterio de orden hace el acumulado determinista si dos tiendas empatan en importe.
# ranking = posición por importe (1 = la tienda que más factura).
publicar("rendimiento_tiendas", f"""
SELECT t.tienda_id, t.nombre AS tienda, t.ciudad, t.formato,
       SUM(v.tickets) AS tickets, SUM(v.unidades) AS unidades, SUM(v.importe) AS importe,
       ROUND(100 * SUM(v.importe) / SUM(SUM(v.importe)) OVER (), 2) AS pct_facturacion,
       ROUND(100 * SUM(SUM(v.importe)) OVER (ORDER BY SUM(v.importe) DESC, t.tienda_id
                                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                 / SUM(SUM(v.importe)) OVER (), 2) AS pct_acumulado,
       RANK() OVER (ORDER BY SUM(v.importe) DESC) AS ranking
FROM {S}.ventas_tienda v JOIN {S}.tiendas t USING (tienda_id)
GROUP BY t.tienda_id, t.nombre, t.ciudad, t.formato
""")
# Prueba visible de la extensión: las 10 tiendas que más facturan y cuánto acumulan
display(spark.sql(f"SELECT ranking, tienda, ciudad, importe, pct_facturacion, pct_acumulado FROM {G}.rendimiento_tiendas ORDER BY ranking LIMIT 10"))
# Y el número de amarre, a la vista: debe dar 1003274.99
display(spark.sql(f"SELECT SUM(importe) AS importe_tiendas, COUNT(*) AS tiendas_con_ventas FROM {G}.rendimiento_tiendas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Pregunta 8 · ¿Qué dato entró y cuándo?
# MAGIC Un inventario de todas las tablas de las tres zonas con su número de filas y su última escritura. **Quién puede verlo** lo responde Unity Catalog: `SHOW GRANTS ON SCHEMA <catálogo>.gold`.
# MAGIC
# MAGIC La celda de abajo recorre los esquemas `bronze`, `silver` y `gold`, pregunta a cada tabla Delta por sus filas, sus archivos, su tamaño y su última operación, y guarda todo en `auditoria_cargas`. Esta tabla no sale de una consulta SQL: se arma con un bucle de Python.

# COMMAND ----------

filas = []   # aquí se acumula una tupla por tabla inspeccionada
for zona in ("bronze", "silver", "gold"):
    # SHOW TABLES lista las tablas del esquema; collect() trae esa lista a Python para recorrerla
    for t in spark.sql(f"SHOW TABLES IN {CATALOG}.{zona}").collect():
        # Se saltan la propia tabla de auditoría (no se describe a sí misma) y las vistas temporales
        if t["tableName"] == "auditoria_cargas" or t["isTemporary"]:
            continue
        nombre = f"{CATALOG}.{zona}.{t['tableName']}"
        try:
            # DESCRIBE DETAIL: datos físicos de la tabla Delta (número de archivos, tamaño en bytes)
            d = spark.sql(f"DESCRIBE DETAIL {nombre}").first()
            # DESCRIBE HISTORY … LIMIT 1: la operación más reciente (cuándo fue y de qué tipo: WRITE, DELETE…)
            h = spark.sql(f"DESCRIBE HISTORY {nombre} LIMIT 1").first()
            # «or 0» cambia por 0 un valor vacío antes de convertirlo a entero
            filas.append((zona, t["tableName"], spark.table(nombre).count(), int(d["numFiles"] or 0), int(d["sizeInBytes"] or 0), h["timestamp"], h["operation"]))
        except Exception as e:   # tablas que no son Delta (no deberían existir en estas zonas)
            # No se detiene el notebook: se avisa y se sigue con la siguiente tabla
            print("omitida", nombre, str(e)[:80])
# La lista de tuplas se convierte en DataFrame; el segundo argumento da el nombre y el tipo de cada columna
auditoria = spark.createDataFrame(filas, "zona string, tabla string, filas long, archivos int, bytes long, ultima_escritura timestamp, ultima_operacion string")
# Se agrega la fecha de proceso y se guarda como tabla externa de oro (misma escritura que hace publicar)
(auditoria.withColumn("fecha_proceso", F.lit(FECHA)).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .option("path", f"{DELTA}/auditoria_cargas").saveAsTable(f"{G}.auditoria_cargas"))
RESUMEN["auditoria_cargas"] = len(filas)   # una fila por tabla inventariada
# Se muestra el inventario ordenado por zona y tabla
display(spark.table(f"{G}.auditoria_cargas").orderBy("zona", "tabla"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Verificación del oro: los números que debe dar la muestra del curso
# MAGIC Controles que cruzan zonas. Si alguno falla, el problema está aguas arriba.
# MAGIC
# MAGIC La celda de abajo calcula cada control con una consulta que devuelve un solo número, lo compara con el valor esperado e imprime `OK` o `ERROR`. Si alguno no coincide, el notebook se detiene.

# COMMAND ----------

def uno(sql):
    # Ejecuta una consulta que devuelve una sola fila con una sola columna y entrega ese valor
    return spark.sql(sql).first()[0]

# Cada control es una terna: (nombre, valor calculado sobre el oro, valor esperado con la muestra del curso).
# float(…) convierte los importes decimales para poder compararlos con el número esperado.
CONTROLES = [
    ("pedidos pagados del e-commerce", uno(f"SELECT SUM(operaciones) FROM {G}.ventas_diarias WHERE canal <> 'tienda'"), 1425),
    ("importe e-commerce pagado", float(uno(f"SELECT SUM(importe) FROM {G}.ventas_diarias WHERE canal <> 'tienda'")), 5339767.44),
    ("importe tiendas físicas (semana)", float(uno(f"SELECT SUM(importe) FROM {G}.ventas_diarias WHERE canal = 'tienda'")), 1003274.99),
    ("pedidos sin pago", uno(f"SELECT COUNT(*) FROM {G}.conciliacion_pagos WHERE situacion LIKE 'sin pago%'"), 75),
    ("pedidos pendientes", uno(f"SELECT COUNT(*) FROM {G}.conciliacion_pagos WHERE estado = 'pendiente'"), 6),
    ("pedidos en la conciliación", uno(f"SELECT COUNT(*) FROM {G}.conciliacion_pagos"), 1500),
    ("pagos que no coinciden", uno(f"SELECT COUNT(*) FROM {G}.conciliacion_pagos WHERE situacion IN ('pagado sin pago registrado', 'pago no coincide', 'factura no coincide')"), 0),
    ("facturas legibles cruzadas", uno(f"SELECT COUNT(*) FROM {G}.conciliacion_pagos WHERE serie_numero IS NOT NULL"), 137),
    ("sesiones con checkout (semana)", uno(f"SELECT SUM(con_checkout) FROM {G}.embudo_web"), 333),
    ("clientes con segmento", uno(f"SELECT COUNT(*) FROM {G}.segmentos_campania"), 500),
    # EXTENSIÓN E1 (Gabriela Ramos): la novena tabla debe amarrar con ventas_diarias, y el acumulado debe cerrar en 100 %
    ("importe rendimiento_tiendas (amarre)", float(uno(f"SELECT SUM(importe) FROM {G}.rendimiento_tiendas")), 1003274.99),
    ("pct_acumulado de la última tienda", float(uno(f"SELECT MAX(pct_acumulado) FROM {G}.rendimiento_tiendas")), 100.0),
]
fallas = []   # nombres de los controles que no coinciden
for nombre, valor, esperado in CONTROLES:
    # Se compara con una tolerancia de medio céntimo: al pasar los importes a float pueden aparecer diferencias mínimas
    ok = abs(float(valor) - float(esperado)) < 0.005
    if not ok:
        fallas.append(nombre)
    print(f"{'OK   ' if ok else 'ERROR'} {nombre:34s} {valor} (esperado {esperado})")
# Si la lista de fallas no está vacía, el notebook se detiene y el job queda en error
assert not fallas, f"Oro con diferencias: {fallas}"

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado para el job.** La última celda termina el notebook y devuelve un resumen en JSON: la etapa, la fecha procesada y las filas de cada tabla de oro. El job lo guarda como salida de la tarea de oro.

# COMMAND ----------

import json
# json.dumps convierte el diccionario en texto; dbutils.notebook.exit termina el notebook y entrega ese texto a quien lo ejecutó
dbutils.notebook.exit(json.dumps({"etapa": "oro", "fecha": FECHA, "tablas": RESUMEN}))
