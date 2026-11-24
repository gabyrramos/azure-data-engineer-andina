# Evidencia · Etapa 5 · Databricks y tablas Delta (bronce)

**Workspace:** `dbw-pede-gramos-dev-brs-001` (Premium) · **Cómputo:** SQL warehouse

## Conexión del lago con Unity Catalog
- Identidad: access connector `ac-pede-gramos-dev-brs-001` con rol **Storage Blob Data Contributor** sobre el storage. No se usan claves de cuenta.
- Credencial: `cred-pede-gramos-dev-001`.
- External locations: `loc-pede-bronze`, `loc-pede-silver`, `loc-pede-gold`.
- La verificación de *file events* falló (función opcional); se usó **Force create**. Read, List, Write y Delete pasaron.

## Verificación de lectura
`read_files` sobre `orders.csv` devolvió **10 filas**.

## Catálogo y tablas
Catálogo `pede_dev` con esquemas `bronze`, `silver` y `gold`. Cinco tablas Delta externas en `pede_dev.bronze`, con columnas de auditoría `_load_date` y `_ingested_at`.

| Tabla | Filas esperadas | Filas obtenidas |
|---|---|---|
| customers | 5 | 5 |
| order_items | 15 | 15 |
| orders | 10 | 10 |
| payments | 9 | 9 |
| products | 8 | 8 |

> Capturas: _(agregar aquí)_
