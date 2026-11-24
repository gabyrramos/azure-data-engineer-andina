# Entrega · Laboratorio 01 · Gabriela Ramos

**Fecha:** 2026-11-24
**Servidor:** sql-pede-gramos-dev-brs-001.database.windows.net
**Base:** sqldb-andina-ventas-dev
**Usuario / schema:** administrador del servidor propio / `dbo`

> El servidor compartido del curso fue dado de baja, por lo que aprovisioné mi propio servidor y base en mi suscripción (Serverless con autopausa).

## 1. Conteos

| Tabla | Filas esperadas | Filas obtenidas |
|---|---|---|
| customers | 5 | 5 |
| products | 8 | 8 |
| orders | 10 | 10 |
| order_items | 15 | 15 |
| payments | 9 | 9 |

## 2. Consulta por cliente y método

| full_name | method | total |
|---|---|---|
| Ana Torres | tarjeta | 4977.80 |
| Carla Mendoza | transferencia | 378.90 |
| Carla Mendoza | yape | 477.00 |
| Diego Salazar | efectivo | 1899.00 |
| Diego Salazar | transferencia | 1299.00 |
| Luis Ramírez | yape | 378.00 |
| María Paredes | tarjeta | 3588.90 |
| María Paredes | transferencia | 159.90 |

**Total general:** 13158.50 · **Cantidad de filas:** 8

## 3. Validación de pagos
La consulta de diferencias devolvió **0 filas**. Compara `payments.amount` con la suma de `quantity * unit_price` de cada pedido y, mediante `HAVING`, solo muestra los que no coinciden. Con 0 filas, todos los pagos cuadran con sus líneas.

## 4. Por qué el pedido 1007 no aparece
- El pedido 1007 tiene estado `cancelado`, y la consulta filtra `WHERE o.status = 'pagado'`.
- Además no tiene fila en `payments`, y el `JOIN` con esa tabla descarta los pedidos sin pago.

## 5. ¿Este caso podría vivir en Cosmos DB?
**Decisión:** Sí, podría, pero SQL es mejor para esta pregunta.

**Pregunta de negocio:** ¿cuánto pagó cada cliente por método de pago?

**Justificación:** esta pregunta cruza clientes, pedidos, líneas y pagos y los agrega con sumas. Eso es lo que hace bien un modelo relacional con `JOIN` y claves foráneas. En Cosmos DB se guardaría un documento JSON por pedido (cliente, ítems y pago anidados), que sirve para leer un pedido completo rápido, pero agregar por cliente y método exigiría recorrer muchos documentos. Por eso mantengo el origen en Azure SQL.

## Tarea del módulo
**Dato de mi trabajo:** registros de uso de servicios por cliente (cada invocación genera un JSON con el cliente y el servicio usado).
**Clasificación:** semiestructurado, porque tiene campos con nombre pero sin un esquema fijo y rígido.
