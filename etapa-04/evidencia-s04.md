# Evidencia · Etapa 4 · Lago ADLS Gen2

**Cuenta:** `stpedegramosdldevbrs001` · **Región:** Brazil South · **Redundancia:** LRS

## Configuración
- Hierarchical namespace: habilitado (ADLS Gen2).
- Acceso anónimo: deshabilitado. TLS mínimo 1.2.
- Contenedores privados: `bronze`, `silver`, `gold`.

## Datos cargados en bronce
Ruta: `bronze/pedidos/fecha_carga=2026-10-10/`

- `customers.csv`
- `products.csv`
- `orders.csv`
- `order_items.csv`
- `payments.csv`

> Capturas: _(agregar aquí, sin ID de suscripción ni IP)_

## Pendiente
Prueba de permisos RBAC/ACL (403 en `bronze`, lectura en `gold`) con identidad de Entra ID.
