# Evidencia · Capa gold

## Tabla
`pede_dev.gold.ventas_por_canal` · Delta externa en `gold/delta/ventas_por_canal`.

**Pregunta que responde:** ¿cuánto se vendió por canal y estado del pedido?
**Origen:** `pede_dev.silver.pedidos_detalle` (agregada por `canal` y `estado`).

```sql
CREATE OR REPLACE TABLE pede_dev.gold.ventas_por_canal
USING DELTA
LOCATION 'abfss://gold@stpedegramosdldevbrs001.dfs.core.windows.net/delta/ventas_por_canal'
AS
SELECT canal, estado,
       COUNT(DISTINCT order_id)     AS pedidos,
       SUM(cantidad)                AS unidades,
       ROUND(SUM(importe_linea), 2) AS importe
FROM pede_dev.silver.pedidos_detalle
GROUP BY canal, estado;
```

## Resultado

| canal | estado | pedidos | unidades | importe |
|---|---|---|---|---|
| web | pagado | 6 | 10 | 10404.50 |
| tienda | pagado | 3 | 6 | 2754.00 |
| web | cancelado | 1 | 1 | 249.00 |

## Reconciliación
10404.50 + 2754.00 + 249.00 = **13407.50**, igual al total de bronce y de plata. Son 10 pedidos en total (6 + 3 + 1).

> Captura: _(agregar aquí)_
