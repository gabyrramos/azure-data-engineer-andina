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

| Etapa | Tema | Estado |
|---|---|---|
| 1 | Nube, naming y grupo de recursos | grupo creado · ficha pendiente |
| 2 | Azure SQL Database (5 tablas de Andina) | tablas cargadas y validadas · entrega pendiente |
| 3 | Diseño del lakehouse | pendiente |
| 4 | Data lake ADLS Gen2 | en curso |

## Nota de seguridad

Este repositorio es público. No contiene contraseñas, claves ni identificadores de suscripción.
