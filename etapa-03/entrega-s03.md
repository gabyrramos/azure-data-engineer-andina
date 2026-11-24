# Entrega S03 · Diseño de lakehouse para Andina

**Integrante:** Gabriela Ramos · **Fecha:** 2026-11-24

## 1. Inventario de fuentes

| Fuente | Formato | Frecuencia | Volumen aprox. | Dueño | ¿PII? |
|---|---|---|---|---|---|
| SQL de pedidos y pagos | tablas relacionales | diaria | MB–GB | Ventas | Sí (cliente) |
| CSV de productos, clientes y ventas | CSV | diaria | MB | Comercial | Sí (clientes) |
| JSON de clics y navegación | JSON | continua | GB | Marketing digital | Sí (userId) |
| PDFs de facturas y contratos | PDF | por evento | GB | Finanzas / Legal | Sí |
| Logs de aplicación | texto/JSON | continua | GB | TI | Posible (IP, ids) |
| Puntajes de modelos | tabla/Parquet | semanal | MB | Ciencia de datos | No |
| API externa (tipo de cambio) | JSON | diaria | KB | Finanzas | No |
| Maestro de tiendas | CSV/tabla | ocasional | KB | Operaciones | No |

## 2. Ventana y formato

| Fuente | Batch o streaming | Por qué | Formato de guardado |
|---|---|---|---|
| SQL de pedidos y pagos | Batch | los reportes de ventas se revisan por día | Delta |
| Maestro y tipo de cambio | Batch | cambian poco; un dato diario basta | Delta |
| JSON de clics | Streaming | se usa para reaccionar durante la sesión | Delta (plata) |
| Logs de aplicación | Streaming | detectar fallas rápido | Parquet/Delta |
| PDFs | Batch por evento | no estructurado; se guarda tal cual | PDF original |

## 3. Zonas

| Zona | Qué guarda | Pregunta que responde | Dueño | Prueba de calidad |
|---|---|---|---|---|
| Bronce | datos crudos tal como llegan, con columnas de auditoría | ¿qué llegó y cuándo? | Ingeniería de datos | conteos contra el origen |
| Plata | datos limpios y unidos, con grano definido | ¿qué pasó en cada línea de pedido? | Ingeniería de datos | 0 nulos, 0 duplicados, total cuadra con bronce |
| Oro | tablas listas para negocio | ¿cuánto vendimos por tienda o canal? | Analítica | métricas validadas con negocio |

## 4. Partición y gobierno
- **Partición principal:** por fecha de negocio (`fecha_carga` / `order_date`), porque la mayoría de consultas filtra por periodo.
- **Identidad y acceso:** Microsoft Entra ID con RBAC y ACL; el lago se lee con identidad administrada, no con claves de cuenta.
- **PII y minimización:** `full_name`, `email` y `userId` se enmascaran o seudonimizan en plata/oro; el acceso a bronce se restringe.
- **Linaje y catálogo:** Unity Catalog (catálogo `pede_dev`).
- **Secretos:** Key Vault para la contraseña del SQL.
- **Control de costo:** autopausa en SQL y en el warehouse, ciclo de vida hot → cool y etiquetas con fecha de vencimiento.

## 5. Flujo
Fuentes → ingesta batch o streaming → **Bronce** (Delta en ADLS Gen2) → limpieza y calidad con Databricks/Spark → **Plata** → agregados con Databricks SQL → **Oro** → consumo (Power BI, modelo de ML, API). La banda de gobierno cubre todo el flujo.

> Mi diagrama original está en el repo del curso (`S03-lakehouse-ramos-gabriela.json`). Su autocomprobación fue 7/10 y le faltaban ventanas en las flechas, formato en los nodos de almacenamiento y un dato marcado como PII; esta entrega lo completa.

## 6. Riesgo y Plan B
- **Pregunta abierta:** ¿cómo se capturan cambios en el SQL (incremental) sin recargar todo cada día?
- **Plan B:** si una fuente llega tarde, la plata se procesa con la última carga válida y se marca el dato como pendiente; el proceso es idempotente y se puede reejecutar por fecha.
