# Evidencia · Etapa 7 · Ingesta con Azure Data Factory y Key Vault

**Data Factory:** `adf-pede-gramos-dev-brs-001` · **Región:** Brazil South

## Seguridad
- La contraseña del SQL no está en Data Factory: vive en el Key Vault `kv-pede-gramos-dev-001` como secreto `sqladminpede-password`, y se lee con la identidad administrada de ADF (access policy con permiso *Get*).
- El acceso al lago usa la identidad administrada de ADF con el rol **Storage Blob Data Contributor**, sin claves de cuenta.

## Objetos

| Tipo | Nombre | Detalle |
|---|---|---|
| Linked service | `ls_pede_kv` | Azure Key Vault |
| Linked service | `ls_pede_sql` | Azure SQL; la contraseña se toma de `ls_pede_kv` |
| Linked service | `ls_pede_lake` | ADLS Gen2 por identidad administrada |
| Dataset | `ds_pede_sql_tabla` | parámetro `tableName` |
| Dataset | `ds_pede_bronze_csv` | parámetros `nombre_tabla` y `fecha` |
| Pipeline | `pl_pede_ingesta` | parámetro `fecha` |

## Pipeline parametrizado
`act_lookup_tablas` → `act_foreach_tablas` (batch 5) → `act_copy_tabla`

- **Lookup:** descubre las tablas base del esquema `dbo` con `INFORMATION_SCHEMA.TABLES`, sin tablas del sistema.
- **ForEach:** itera sobre `@activity('act_lookup_tablas').output.value`.
- **Copy:** un solo par de datasets sirve para todas las tablas con `@item().TABLE_NAME`. Reintentos: 3, cada 60 segundos.
- **Destino:** `bronze/pedidos_ingesta/<fecha>/<tabla>/<tabla>.csv`

## Ejecución
El pipeline terminó en estado **Succeeded** en el Monitor de Data Factory.

## Incidencias resueltas
| Problema | Causa | Solución |
|---|---|---|
| `Missing parameter definition for fecha` | el pipeline no declaraba el parámetro | se creó `fecha` (String) en el pipeline |
| `SqlFailedToConnect` / timeout | la base Serverless estaba en autopausa | se despertó la base y se agregaron reintentos al Copy |

> Capturas: _(Monitor con la ejecución Succeeded y las 5 carpetas en el lago)_
