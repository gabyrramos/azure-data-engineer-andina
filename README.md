# Proyecto Andina Retail · Plataforma de datos en Azure

Proyecto del P.E. Data Engineer Multicloud (módulo Azure). Plataforma de datos para el caso ficticio **Andina Retail**, construida por etapas.

## Arquitectura objetivo

```
Azure SQL → Azure Data Factory → ADLS Gen2 (bronze/silver/gold) → Databricks → SQL analítico
```

## Convención de nombres

`<tipo>-pede-gramos-dev-brs-<instancia>` (alumno `gramos`, región Brazil South).

| Recurso | Nombre |
|---|---|
| Grupo de recursos | `rg-pede-gramos-dev-brs-001` |
| Servidor Azure SQL | `sql-pede-gramos-dev-brs-001` |
| Base de datos | `sqldb-andina-ventas-dev` |
| Lago ADLS Gen2 | `stpedegramosdldevbrs001` |

## Avance por etapas

| Etapa | Tema | Entrega | Estado |
|---|---|---|---|
| 1 | Nube, naming y grupo de recursos | [ficha](etapa-01/ficha-decision-nube.md) | hecha (falta estimación de costo) |
| 2 | Azure SQL Database (5 tablas de Andina) | [entrega](etapa-02/entrega-lab01.md) | hecha |
| 3 | Diseño del lakehouse | [diseño](etapa-03/entrega-s03.md) | hecha |
| 4 | Data lake ADLS Gen2 | [evidencia](etapa-04/evidencia-s04.md) | hecha (falta prueba RBAC/ACL) |
| 5 | Databricks y tablas Delta (bronce) | [evidencia](etapa-05/evidencia-s05.md) | hecha |
| 6 | Bronce → plata → gold | [plata](etapa-06/evidencia-s06.md) · [gold](etapa-06/evidencia-gold.md) | hecha (falta job) |
| 7 | Data Factory y Key Vault | [evidencia](etapa-07/evidencia-adf.md) | hecha (falta trigger) |
| 8 | Job de Databricks y presentación final | — | pendiente |

## Nota de seguridad

Este repositorio es público. No contiene contraseñas, claves ni identificadores de suscripción.
