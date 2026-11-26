# Proyecto Andina Retail · Plataforma de datos en Azure

Proyecto final del P.E. Data Engineer Multicloud (módulo Azure). Plataforma de datos para el caso ficticio **Andina Retail**: 8 fuentes que llegan a un lago, se limpian y terminan en tablas de oro que responden preguntas de negocio.

**Estado vigente:** la plataforma completa a escala **1:1.000** (1.500 pedidos, 3.489 líneas, 1.425 pagos), ejecutada en mi suscripción el 2026-11-26, con los 7 números de control del caso y una extensión propia.

## Qué leer primero

| Archivo | Qué contiene |
|---|---|
| [`evidencia.md`](evidencia.md) | La corrida, los números de control, la seguridad y la prueba de la extensión |
| [`DECISIONES.md`](DECISIONES.md) | Mis decisiones de diseño con su motivo, y la extensión |
| [`adf/`](adf/) | JSON exportados de Data Factory (linked services, datasets, pipelines y triggers), sin secretos |
| [`notebooks/`](notebooks/) | Bronce, plata y oro (`e4_01`–`e4_03`), la carga horaria de clics y logs (`e4_00`) y mi extensión en `e4_03_oro.py` |
| [`databricks/`](databricks/) | Definición de los dos jobs (el diario y el horario) |

## Arquitectura

```
CADA DÍA · trigger de ADF 01:30 (Lima, detenido) → pl_pede_andina_diario
  Azure SQL ──► pl_pede_ingesta (Lookup → ForEach → Copy) ──► bronze/pedidos_ingesta/<fecha>/
  ERP, PDF, puntajes, tipo de cambio y tiendas ───────────► bronze/landing/
  job-pede-andina-e4:  bronce ──► plata ──► oro   (Unity Catalog, catálogo pede_dev)

CADA HORA · horario del job de Databricks (en pausa)
  clics y logs ──► bronze/landing/ ──► job-pede-andina-e4-horario (Auto Loader: solo archivos nuevos)
```

## Recursos (convención `<tipo>-pede-gramos-dev-brs-<instancia>`)

| Recurso | Nombre |
|---|---|
| Grupo de recursos | `rg-pede-gramos-dev-brs-001` |
| Servidor y base Azure SQL | `sql-pede-gramos-dev-brs-001` · `sqldb-andina-ventas-dev` |
| Lago ADLS Gen2 | `stpedegramosdldevbrs001` |
| Key Vault | `kv-pede-gramos-dev-001` |
| Data Factory | `adf-pede-gramos-dev-brs-001` |
| Workspace de Databricks y access connector | `dbw-pede-gramos-dev-brs-001` · `ac-pede-gramos-dev-brs-001` |
| Catálogo de Unity Catalog / secret scope | `pede_dev` / `kv-pede-gramos` |

## Números de control (todos exactos)

1.425 pedidos pagados · 5.339.767,44 de e-commerce · 1.003.274,99 de tiendas · 75 sin pago · 137 de 140 facturas cruzadas · 333 sesiones con checkout · 34.530 eventos únicos de 35.043. Detalle y consultas en [`evidencia.md`](evidencia.md).

## Historial: avance por etapas (muestra chica de 47 filas)

Antes de montar la plataforma 1:1.000 fui entregando cada sesión con la muestra del curso. Esas entregas se conservan como historial; sus números (por ejemplo 5/8/10/15/9 filas y 13.158,50) corresponden a esa muestra, que ya no existe en la base.

| Etapa | Tema | Entrega |
|---|---|---|
| 1 | Nube, naming y grupo de recursos | [ficha](etapa-01/ficha-decision-nube.md) |
| 2 | Azure SQL Database (5 tablas de Andina) | [entrega](etapa-02/entrega-lab01.md) |
| 3 | Diseño del lakehouse | [diseño](etapa-03/entrega-s03.md) |
| 4 | Data lake ADLS Gen2 | [evidencia](etapa-04/evidencia-s04.md) |
| 5 | Databricks y tablas Delta (bronce) | [evidencia](etapa-05/evidencia-s05.md) |
| 6 | Bronce → plata → gold | [plata](etapa-06/evidencia-s06.md) · [gold](etapa-06/evidencia-gold.md) |
| 7 | Data Factory y Key Vault | [evidencia](etapa-07/evidencia-adf.md) |
| 8 | Job de Databricks y presentación (versión de la muestra chica) | [evidencia](etapa-08/evidencia.md) · [PDF](etapa-08/PF-gramos.pdf) |

## Nota de seguridad

Este repositorio es público. No contiene contraseñas, claves, tokens ni identificadores de suscripción o tenant: la contraseña del SQL vive solo en Key Vault y los JSON exportados llevan referencias, no valores (el ID de suscripción está reemplazado por `<SUBSCRIPTION_ID>`).
