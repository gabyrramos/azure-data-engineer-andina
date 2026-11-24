# Ficha de decisión de nube · Andina Retail

**Alumno:** Gabriela Ramos · **Fecha:** 2026-11-24

## Paso 2 · Nombres y grupo de recursos
Token de alumno: `gramos` · Región: `brs` (Brazil South) · Por qué: región disponible en mi suscripción y cercana a Perú.

| Recurso | Nombre |
|---|---|
| Grupo de recursos | `rg-pede-gramos-dev-brs-001` |
| Almacenamiento (ADLS Gen2) | `stpedegramosdldevbrs001` |
| Servidor Azure SQL | `sql-pede-gramos-dev-brs-001` |
| Workspace de Databricks | `dbw-pede-gramos-dev-brs-001` |
| Access connector | `ac-pede-gramos-dev-brs-001` |

Etiquetas del grupo: curso · alumno · entorno · modulo · vence

## Paso 3 · Clasificación de servicios
| Servicio | Modelo | Tú administras |
|---|---|---|
| Virtual Machines | IaaS | sistema operativo, parches, aplicaciones y datos |
| Virtual Network | IaaS | direccionamiento, subredes y reglas |
| Azure SQL Database | PaaS | datos, esquema y accesos |
| Azure App Service | PaaS | tu código y su configuración |
| Azure Functions | PaaS (serverless) | solo el código de la función |
| Microsoft 365 | SaaS | usuarios y configuración |
| Power BI | SaaS | modelos, informes y permisos |
| Azure Databricks | PaaS | notebooks, jobs y datos |

## Paso 4 · Estimación de costo
_Pendiente: completar con la Calculadora de precios de Azure (fecha, región, supuestos y enlace)._

## Paso 5 · Equivalencias
| Función | AWS | Azure | Google Cloud |
|---|---|---|---|
| Almacenamiento de objetos | S3 | Blob Storage / ADLS Gen2 | Cloud Storage |
| Base de datos relacional administrada | RDS | Azure SQL Database | Cloud SQL |
| Cómputo serverless | Lambda | Azure Functions | Cloud Functions |

## Paso 6 · Decisión
**Modelo recomendado:** nube pública con PaaS (Azure SQL, ADLS Gen2, Databricks).
**Criterios:** costo variable con picos de campaña, el equipo conoce SQL Server y no ha operado nube, y la analítica e IA futuras necesitan un lago.
**Ventajas:** escalado en campañas (Navidad, Cyber) y menos administración de infraestructura.
**Riesgos:** datos personales sujetos a la Ley 29733 y costos que crecen si no se controlan.
**Mitigación:** etiquetas, autopausa, control de costo y acceso por identidad con datos personales enmascarados.
