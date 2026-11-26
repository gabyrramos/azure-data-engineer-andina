# Decisiones de diseño · Andina Retail (gramos)

Cada decisión lleva su motivo. Las que aprendí de un error lo dicen.

1. **El lago vive en ADLS Gen2, fuera de Databricks.** Bronce, plata y oro son tablas Delta externas en `bronze/`, `silver/` y `gold/`, gobernadas por Unity Catalog (`pede_dev`). Así los datos sobreviven a la herramienta de cómputo y cada zona tiene su propia external location.

2. **Reutilicé mi entorno de S05–S07 en lugar de recrearlo** (credencial, external locations `loc-pede-*`, catálogo, Key Vault, ADF). Menos recursos y menos costo. Conservé `silver.pedidos_detalle` y `gold.ventas_por_canal` de S06 por trazabilidad; por eso la auditoría muestra 12 tablas de plata (11 del curso + `pedidos_detalle`) y 9 de oro (7 del curso + `ventas_por_canal` + mi extensión; `auditoria_cargas` no se cuenta a sí misma).

3. **Subí a escala 1:1.000 (1.500 pedidos) y reemplacé la muestra chica.** Los números de control del caso solo existen en esa base. La muestra chica (5/8/10/15/9 filas) queda en `etapa-02/` como historial, ya no se puede reproducir en vivo.

4. **Secretos: solo en Key Vault.** ADF lee la contraseña del SQL por referencia (`AzureKeyVaultSecret`, sin valor en el JSON) y Databricks la ve por un secret scope respaldado por ese mismo Key Vault (`kv-pede-gramos`). El lago se lee con identidades administradas y el rol `Storage Blob Data Contributor`, sin claves de cuenta.

5. **La identidad de ADF en Databricks tiene el mínimo privilegio.** El rol Contributor sobre el workspace **no bastó**: Databricks no registró a ADF y rechazó la llamada (error 3200, 0 corridas del job). La registré como service principal con solo `workspace-access` y `CAN_MANAGE_RUN` sobre el job diario. No es administradora y no puede tocar el job horario.

6. **El trigger diario `tg_pede_andina_0130` queda detenido.** Activo correría cada día a la 01:30 (Lima): despertaría el SQL serverless y lanzaría el job, gastando mi crédito sin que nadie consuma el resultado. Se activa en un paso (`trigger-on` del script del curso) cuando haya quien use los datos. Lo probé con corridas manuales.

7. **El horario del job de clics y logs queda en pausa.** Activo corre 24 veces al día aunque no llegue ningún archivo, y en el laboratorio los archivos llegan a mano. Lo probé a mano: la primera corrida cargó 34.967 clics en 167 archivos y 125.316 logs en 18; tras subir 4 archivos nuevos, la segunda leyó solo esos (35.043 en 168 y 146.282 en 21).

8. **SQL serverless con autopausa.** Es lo más barato para un origen que se consulta una vez al día. El costo es que la primera conexión tarda: la carga de la base tuvo que reintentar mientras despertaba. El Copy de `pl_pede_ingesta` reintenta 1 vez cada 30 s; en mi corrida la base ya estaba despierta. Si activara el trigger de la 01:30, subiría los reintentos a 3 cada 60 s, que fue lo que resolvió el `SqlFailedToConnect` de S07.

9. **Extensión propia (E1): novena tabla de oro `pede_dev.gold.rendimiento_tiendas`.**
   - **Pregunta:** ¿qué tiendas físicas concentran la facturación? El modelo del curso da las ventas por día y tienda, pero no cuánto pesa cada una.
   - **Tabla:** una fila por tienda con tickets, unidades, importe, % de la facturación, % acumulado (lectura tipo Pareto) y ranking. Sección 8b de `notebooks/e4_03_oro.py`, antes de la auditoría para que `auditoria_cargas` la inventaríe.
   - **Control de amarre:** `SUM(importe)` = **1.003.274,99**, igual que las tiendas en `ventas_diarias`, y el acumulado de la última tienda = 100 %. Los dos son `assert` del notebook: si no cuadran, el job falla.
   - **Resultado:** 38 tiendas con ventas; la primera (Andina Jesús María) pesa 3,94 % y las 10 primeras suman 33,59 %. La facturación está repartida, no concentrada en pocas tiendas.

10. **Costo.** La estimación de `etapa-01/` es de un lago de 1 TB en la calculadora. El costo real del grupo de recursos se ve en *Cost analysis* del portal (captura en `evidencia.md`).
