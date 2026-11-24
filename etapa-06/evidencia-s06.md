# Evidencia · Etapa 6 · Bronce → plata

## Tabla plata
`pede_dev.silver.pedidos_detalle` · Delta externa en `silver/delta/pedidos_detalle`.

**Grano:** una fila por línea de pedido. Se une `orders` con `order_items`, `products` y `customers` (INNER) y con `payments` (LEFT), para conservar el pedido cancelado que no tiene pago.

**Resultado:** 15 filas · 10 pedidos distintos.

## Controles de calidad

| Control | Esperado | Obtenido |
|---|---|---|
| Claves nulas (`order_id`, `product_id`) | 0 | 0 |
| Duplicados por grano (`order_id`, `product_id`) | 0 | 0 |
| Total plata vs. bronce | 13407.50 / 13407.50 | 13407.50 / 13407.50 |

```sql
SELECT COUNT(*) AS nulos
FROM pede_dev.silver.pedidos_detalle
WHERE order_id IS NULL OR product_id IS NULL;

SELECT COUNT(*) AS duplicados FROM (
  SELECT order_id, product_id, COUNT(*) AS n
  FROM pede_dev.silver.pedidos_detalle
  GROUP BY order_id, product_id HAVING n > 1);

SELECT
  (SELECT ROUND(SUM(importe_linea),2) FROM pede_dev.silver.pedidos_detalle) AS plata,
  (SELECT ROUND(SUM(quantity*unit_price),2) FROM pede_dev.bronze.order_items) AS bronce;
```

> Capturas: _(agregar aquí)_

## Pendiente
Job programado en modo pausado con su historial de ejecuciones.
