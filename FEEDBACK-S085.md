# Feedback · Proyecto Final Andina Retail

**Estudiante:** Gabriela Ramos (gramos)
**Usuario de GitHub:** `gabyrramos`
**Repo:** gabyrramos/azure-data-engineer-andina
**Extensión:** Ninguna
**Nota final:** **50 / 100**

| Criterio | Puntaje | Máximo |
|---|---|---|
| Repo y entrega | 5 | 10 |
| Ingesta con ADF | 10 | 15 |
| Orquestación observable | 10 | 15 |
| Bronce, plata, oro (UC) | 8 | 15 |
| Números de control | 10 | 15 |
| Seguridad y secretos | 7 | 10 |
| Extensión propia | 0 | 15 |
| Decisiones y defensa | 0 | 5 |
| **TOTAL** | **50** | **100** |

---

## Lo que hiciste bien

Gabriela, tu README está muy bien hecho: nombres reales de recursos, arquitectura por etapas y limitaciones declaradas con honestidad. Eso dice mucho de cómo trabajas.

- **Ingesta con ADF:** tus capturas son de tu entorno real: `bronze/pedidos_ingesta/2026-11-25/` con las 5 carpetas, el run en Succeeded y el trigger Stopped. La ingesta funciona y el resultado es el que pide el criterio.
- **Orquestación:** trigger `tr_diario_0130` en Stopped, con el motivo escrito en tus limitaciones.
- **Bronce, plata, oro:** documentas las 3 capas con control de calidad (0 duplicados, plata = bronce). La estructura medallón está entendida.
- **Números de control:** tu procedimiento es correcto: la conciliación cierra, con 13.407,50 en plata y el mismo número en oro. La lógica del criterio la entendiste.
- **Seguridad:** contraseña por Key Vault, access connector y Storage Blob Data Contributor documentados; sin secretos.

---

## Para llevarlo al siguiente nivel

La nota baja por dos razones concretas, y las dos tienen arreglo:

1. **El código no está en el repo.** No hay `adf/*.json` ni notebooks: todo lo de ADF está como capturas. Sin los JSON no puedo revisar el diseño (parametrización, AppendVariable, secuencia del maestro, user properties). Por eso ingesta y orquestación quedaron en 10: el resultado está demostrado; el diseño todavía no.
2. **La escala.** Trabajaste sobre la muestra andina-1k (plata con 15 filas y 13.407,50) en vez de la base 1:1.000 (3.489 líneas y 5.339.767,44). Por eso los 7 números de control no aparecen: no están en ese dataset. Con la base 1:1.000 salen solos.

Otros puntos:

- **Repo y entrega:** el costo es una estimación en Excel; la rúbrica pide la captura del portal.
- **Seguridad:** falta demostrar el secret scope.
  Para demostrarlo basta con esto y una captura de la salida (sin revelar ningún valor):

  ```python
  dbutils.secrets.listScopes()      # debe aparecer tu scope
  dbutils.secrets.list("tu-scope")  # aparecen los nombres, NUNCA los valores
  ```
- **Extensión propia y decisiones:** son 20 puntos que se pueden sumar. Para la extensión, elige una pregunta que el modelo actual no responda y agrega una novena tabla de oro.
  Así se documenta una extensión completa:

  - **Pregunta:** ¿qué tiendas físicas concentran la facturación?
  - **Tabla:** `pede_dev.gold.rendimiento_tiendas`, sección 9 de `e4_03_oro.py`
  - **Control de amarre:** `SUM(importe)` de la tabla = 1.003.274,99, igual que `ventas_diarias`
  - **Prueba:** captura del run en Succeeded + la consulta que devuelve el número.

  Con esas cuatro líneas el criterio queda cerrado y defendible.

  Para el `DECISIONES.md`, tu apartado de «Limitaciones» ya muestra que piensas las decisiones; solo falta llevarlo a un archivo con el porqué de cada una.

---

## Mensaje final

Gabriela, entendiste la lógica del medallón y de la conciliación, que es lo más importante. Lo que falta es sobre todo de forma: subir los JSON de ADF y los notebooks, correr la base 1:1.000 y sumar la extensión y el `DECISIONES.md`. Con eso tu proyecto cambia mucho.

---

## Rúbrica aplicada

| # | Criterio | Pts | Qué se evalúa |
|---|---|---|---|
| 1 | Repo y entrega | 10 | Repo con docente invitado; README con nombres reales de recursos; `adf/`, `notebooks/`, `evidencia.md`, `DECISIONES.md`; captura de costo |
| 2 | Ingesta con ADF | 15 | `Lookup → ForEach → Copy + Append`; datasets SQL/CSV parametrizados; linked service del SQL parametrizado; `bronze/pedidos_ingesta/<fecha>/` con 5 tablas |
| 3 | Orquestación observable | 15 | Maestro `SetVar → Execute → Databricks Job → SetVar`; trigger 01:30 Lima con motivo; Monitor en Succeeded con user properties; annotations |
| 4 | Bronce, plata, oro (UC) | 15 | 15 bronce, 11 plata, 8 oro en `pede_dev`; notebooks en el repo; encadenados en el job diario con run Succeeded |
| 5 | Números de control | 15 | 1.425 pagados · 5.339.767,44 · 1.003.274,99 · 75 sin pago · 137/140 facturas · 333 checkout · 34.530 de 35.043 eventos |
| 6 | Seguridad y secretos | 10 | Contraseña solo en KV; LS con referencia; secret scope por KV; MSI con roles mínimos; ningún secreto en commits |
| 7 | Extensión propia | 15 | Funciona, código en el repo y prueba concreta en `evidencia.md` |
| 8 | Decisiones y defensa | 5 | Decisiones propias documentadas en `DECISIONES.md` |

---

*Revisión de proyecto final · P.E. Data Engineer · Azure (DSRP)*
