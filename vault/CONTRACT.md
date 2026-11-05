# Contrato de oteadero

Este archivo dice qué debe hacer el sistema y fija cada decisión de diseño. Lo que aquí se fija no se vuelve a discutir en los mini sprints: se implementa. Los hechos que sostienen las decisiones, con su fuente y su versión, están en `vault/RESEARCH.md`. Los límites van solo en `LIMITATIONS.md`.

## 1. Qué es

oteadero es un motor de telemetría en tiempo real escrito desde cero, para una sola máquina. Ingiere métricas (contadores y gauges con etiquetas) y logs estructurados de varios inquilinos, los persiste en un log de entrada append-only antes de confirmarlos, agrega por ventanas de tiempo de evento con watermarks, calcula cuantiles (DDSketch) y cardinalidad (HyperLogLog), evalúa reglas de alerta en un evaluador replicado con failover, notifica cada incidente una sola vez de forma efectiva y sirve live tail y métricas en vivo con reanudación sin huecos ni duplicados. La única persistencia es el log de entrada y los checkpoints; recuperarse es reproducir desde offsets.

Todo resultado del motor se verifica contra un oráculo independiente que recalcula desde el log crudo, y un subconjunto declarado de reglas se compara además con vmalert y Prometheus.

## 2. Requisitos

| Id | El sistema debe |
| --- | --- |
| R1 | Ingerir métricas y logs de varios inquilinos por HTTP y sostener al menos el umbral de throughput de D14 en la máquina de César. |
| R2 | Aplicar cuota por inquilino y reparto justo; contar todo descarte por inquilino y causa, de modo que `aceptados + descartados = recibidos` sea exacto para cada inquilino. |
| R3 | Escribir cada lote aceptado en el log y hacer fsync antes de confirmarlo al productor. |
| R4 | Recuperar todo el estado tras `kill -9` o escritura parcial: cargar el último checkpoint válido y reproducir el log desde su offset, truncando el registro final roto. |
| R5 | Agregar por cubetas de tiempo de evento cerradas por watermark, con la política de lateness de D06, y producir resultados que dependan solo del contenido del log. |
| R6 | Calcular cuantiles con error relativo ≤ α publicado y cardinalidad dentro de la cota publicada, también tras mezclar cubetas, series y particiones. |
| R7 | Evaluar reglas sobre conteo, suma, tasa, cuantil y cardinalidad en ventana, con `for`, `keep_firing_for`, histéresis de umbral doble y agrupación por etiquetas, con la máquina de estados de D08. |
| R8 | Notificar cada incidente (disparo y resolución) una sola vez de forma efectiva en el receptor, también tras `kill -9`, reproducción y failover. |
| R9 | Replicar el evaluador en 3 procesos locales con un único notificador por época. |
| R10 | Servir live tail de logs y métricas, y un stream de cubetas cerradas, con token de reanudación, contrapresión y corte explícito. |
| R11 | Correr dentro de una simulación determinista: toda lectura de reloj, azar, disco y red pasa por una interfaz inyectable. |
| R12 | Exponer sus propias métricas: recibidos, aceptados, descartes por causa, tardíos, retraso de watermark, profundidad de colas y error observado de sketches. |

## 3. Arquitectura

```
productores ──HTTP──▶ oteadero serve ─────────────────────────────────────────────┐
                      handlers (parseo en paralelo)                               │
                      → admisión (cuota) → colas por inquilino (DRR)              │
                      → secuenciador (offsets, watermark, tardíos, ticks)         │
                      → log de entrada (group commit + fsync) → ack               │
                      → shards de agregación (por hash de serie)                  │
                      → export de cubetas, stream en vivo, live tail, /metrics    │
                      → coordinador de arrendamiento (épocas en el log)           │
                                                                                  │
log de entrada (archivos) ◀── lectura directa ── oteadero evaluate ×3 (e1, e2, e3)│
                      agregación + reglas + máquina de estados (las 3 réplicas)   │
                      solo el líder con arrendamiento vigente notifica ──HTTP──▶ receptor
```

Distribución del repositorio:

| Ruta | Contenido | Cuenta como |
| --- | --- | --- |
| `cmd/oteadero/` | Binario del motor: `serve`, `evaluate`, `version` | motor |
| `internal/` | Paquetes del motor (`clock`, `rng`, `vfs`, `wal`, `event`, `ingest`, `agg`, `ckpt`, `sketch`, `rules`, `eval`, `stream`, `selfmetrics`, `config`, `server`) | motor |
| `harness/cmd/otgen/` | Generador de carga sembrado y emisor HTTP | arnés |
| `harness/cmd/otrecv/` | Receptor de notificaciones de prueba | arnés |
| `harness/sim/`, `harness/cmd/otsim/` | Simulación determinista | arnés |
| `harness/mutants/` | Parches de mutantes | arnés |
| `oracle/` | Oráculo en Python, solo biblioteca estándar, sin código del motor | arnés |
| `scripts/` | Scripts de criterio `crit_*.py`, `run_criteria.py`, `fetch_refs.py`, `rules2prom.py`, `lib.py` | arnés |
| `rules/` | Reglas JSON de los escenarios y del subconjunto de referencia | datos |

Los tests unitarios (`*_test.go`, `test_*.py`) van junto al código y no cuentan ni como motor ni como arnés en el criterio de tamaño (D21).

## 4. Especificaciones compartidas

Esto es lo único que el motor y el oráculo comparten: formatos y semántica escritos aquí, nunca código. El oráculo se implementa leyendo esta sección, no el código del motor.

### 4.1 Eventos y protocolo de ingestión

- Inquilino: cabecera `X-Oteadero-Tenant`. Un inquilino que no está en la configuración se descarta con causa `unknown_tenant`.
- `POST /v1/metrics`: cuerpo en formato de exposición de texto de Prometheus, una muestra por línea, `nombre{etiqueta="valor",...} valor timestamp_ms`. El timestamp es obligatorio, en milisegundos Unix. Las líneas vacías y las que empiezan con `#` se ignoran. Son los mismos bytes que acepta `/api/v1/import/prometheus` de VictoriaMetrics.
- `POST /v1/logs`: cuerpo NDJSON, un objeto por línea: `{"ts": <ms>, "labels": {...}, "fields": {...}, "msg": "..."}`. `labels` define la serie; `fields` son atributos que no definen la serie (sobre ellos se calcula cardinalidad). Valores de `labels` y `fields`: strings.
- Un lote es atómico: se acepta entero o se descarta entero. Límites: 16 MiB por cuerpo y 100 000 eventos por lote; por encima, causa `too_large`. Una línea mal formada descarta el lote con causa `invalid`. Un timestamp negativo o más de 10 min adelante del reloj del servidor también es `invalid`.
- Respuesta tras el fsync: `200` con `{"accepted": n, "first_offset": o}`. Descarte: `429` (causas `quota`, `queue_full`) o `400` (`invalid`, `too_large`, `unknown_tenant`) con `{"dropped": n, "cause": "..."}`. `503` con causa `shutting_down` durante el cierre.
- Causas de descarte, cerradas: `quota`, `queue_full`, `invalid`, `too_large`, `unknown_tenant`, `shutting_down`. Un tardío no es un descarte: está aceptado en el log y excluido de las ventanas (D06).

### 4.2 Clave canónica de serie

- Etiquetas canónicas: `{a="1",b="2"}`, ordenadas por nombre en bytes, valores con `\` → `\\`, `"` → `\"`, salto de línea → `\n`. Sin etiquetas: `{}`.
- Métrica: `nombre` + etiquetas canónicas sin `__name__`, por ejemplo `http_requests{svc="api"}`.
- Log: etiquetas canónicas de `labels`; los `fields` se codifican igual, aparte.
- Identidad de serie: (inquilino, clave canónica).

### 4.3 Formato del log de entrada

- Directorio `<data>/log/`. Segmentos `<offset>.seg`, donde `<offset>` es el offset del primer evento del segmento en 20 dígitos decimales. Rotación a 256 MiB.
- Cabecera de segmento, 16 bytes: `OTLOG001` (8 bytes ASCII) + id del log (8 bytes, aleatorio al crear el log, igual en todos los segmentos).
- Registro: `u32 len` + `u32 crc` + `u8 type` + payload. `len` cuenta `type` + payload; `crc` es CRC-32 IEEE (el de zlib) sobre `type` + payload. Enteros little-endian; `uvarint` es el varint sin signo de Go (`encoding/binary`); `f64` son los bits IEEE 754.
- `type 1 BATCH`: `uvarint len + inquilino`, `u8 kind` (1 métricas, 2 logs), `i64 ingest_ms`, `uvarint n`, y n eventos.
  - Evento métrica: `uvarint len + clave canónica`, `f64 valor`, `i64 ts_ms`.
  - Evento log: `i64 ts_ms`, `uvarint len + labels canónicas`, `uvarint len + fields canónicas`, `uvarint len + msg`.
- `type 2 TICK`: `i64 ts_ms` (reloj del servidor). Ver D06.
- `type 3 LEASE`: `u64 epoch`, `uvarint len + holder`, `i64 expires_ms`. Ver D09. El oráculo lo ignora.
- Offsets: cada evento de un BATCH recibe un offset global consecutivo desde 0, en orden del log. TICK y LEASE no consumen offset.
- Un registro con `len` que excede el archivo o con `crc` incorrecto es el final del log: la recuperación trunca el segmento en el último registro válido.

### 4.4 Watermarks, cubetas y tardíos

Parámetros (configuración, con estos valores por omisión): `bucket_ms` S = 1000, `lateness_ms` L = 2000, `idle_grace_ms` G = 5000.

- Cubeta de un timestamp `ts`: `end = ceil(ts / S) · S`; la cubeta cubre `(end − S, end]`, abierta por la izquierda como las ventanas de PromQL y MetricsQL.
- Watermark por inquilino: `maxTs_t` es el máximo `ts` de los eventos del inquilino procesados hasta ahora en orden del log; `W_t = maxTs_t − L`. Antes del primer evento del inquilino, `W_t` no está definido.
- Un evento es tardío si y solo si `W_t` está definido (con los eventos anteriores a él) y `end(ts) ≤ W_t`. Los tardíos se cuentan (`late`) y no entran en ninguna cubeta.
- Una cubeta `end` del inquilino t se cierra cuando `W_t ≥ end`. Al cerrarse se emite y ya no cambia.
- `TICK ts`: para cada inquilino con `W_t` definido, `maxTs_t = max(maxTs_t, ts − G)`. El servidor escribe un TICK por segundo mientras algún inquilino tenga `maxTs_t < reloj − G`. Con `idle_grace_ms = 0` no hay ticks.
- Agregados de cubeta por serie: `count` (número de eventos), `sum` (suma de valores en orden del log, en float64; para logs 0), `rate = sum / (S/1000)` (para logs `count / (S/1000)`).
- `closed_at`: offset del último evento procesado cuando la cubeta se cerró; si la cerró un TICK, el offset del último evento anterior a ese TICK.

### 4.5 Reglas

Archivo JSON `{"rules": [...]}`. Cada regla:

| Campo | Significado |
| --- | --- |
| `name` | Nombre único |
| `tenant` | Inquilino |
| `source` | `metrics` o `logs` |
| `match` | Igualdad exacta de etiquetas; en métricas `__name__` es el nombre |
| `agg` | `count`, `sum`, `rate`, `quantile` (solo métricas, sobre valores) o `cardinality` (solo logs, sobre `field`) |
| `quantile`, `field` | Parámetro de `quantile` o `cardinality` |
| `range_ms` | Rango R, múltiplo de S |
| `interval_ms` | Intervalo de evaluación, múltiplo de S; por omisión S |
| `by` | Etiquetas de agrupación; si falta, el grupo es la serie (etiquetas sin `__name__`) |
| `op`, `threshold` | `>`, `>=`, `<` o `<=` y umbral de disparo |
| `resolve_threshold` | Opcional: histéresis; mientras dispara, la condición de permanencia usa este umbral |
| `for_ms`, `keep_firing_for_ms` | Duración mínima y permanencia, por omisión 0 |

- Instantes de evaluación: múltiplos T de `interval_ms` con `W_prev < T ≤ W_new` cada vez que avanza el watermark del inquilino, en orden creciente. La primera vez que `W_t` se define no se evalúa nada; el primer instante es el menor múltiplo mayor que ese primer `W_t`.
- Ventana en T: cubetas de las series que cumplen `match` con `end` en `(T − R, T]`. Valor del grupo: `count` = suma de conteos; `sum` = suma de sumas de cubeta, en orden creciente de `end` y, dentro de un `end`, en orden de clave canónica de serie; `rate = sum / (R/1000)` (en logs, `count / (R/1000)`); `quantile` sobre la mezcla de DDSketch; `cardinality` sobre la mezcla de HLL.
- Un grupo sin muestras en la ventana está ausente: su condición es falsa.
- Clave de grupo: etiquetas canónicas de `by` (o de la serie sin `__name__`).

### 4.6 Máquina de estados de alerta

Estados por (regla, grupo): `inactive`, `pending`, `firing`. En cada T:

- `inactive`: si la condición de disparo se cumple, pasa a `pending` con `activeAt = T`; si `for_ms = 0`, pasa directo a `firing`.
- `pending`: si la condición de disparo no se cumple, vuelve a `inactive`; si se cumple y `T − activeAt ≥ for_ms`, pasa a `firing`.
- `firing`: la condición de permanencia es la de disparo con `resolve_threshold` si existe. Si se cumple, sigue `firing` y borra `keepSince`. Si no: con `keep_firing_for_ms > 0`, fija `keepSince = T` si no estaba fijado y sigue `firing` mientras `T − keepSince < keep_firing_for_ms`; si no, pasa a `resolved` y queda `inactive`.
- Transiciones registradas: `pending`, `inactive` (desde pending), `firing`, `resolved`, cada una como `(regla, grupo, T, estado, valor)`. El valor es el de la ventana en T, o `null` si el grupo está ausente.
- Esta semántica de `for` y `keep_firing_for` coincide con la medida en promtool 3.16.0 y vmalert-tool v1.155.0 (`vault/RESEARCH.md`).

### 4.7 Incidentes y notificaciones

- Incidente: (regla, grupo, `T_f`), con `T_f` el instante de la transición a `firing`. `incident_id` = primeros 32 caracteres hex de SHA-256 de `regla \x00 grupo \x00 T_f` (T_f en decimal).
- Dos notificaciones por incidente: `firing` y, si llega, `resolved`. `Idempotency-Key: <incident_id>/<kind>`.
- `POST <receptor>/notify` con cabeceras `Idempotency-Key` y `X-Oteadero-Epoch`, y cuerpo `{"incident","kind","rule","group","t","value","epoch","evaluator"}`.
- Receptor: si la época es menor que la mayor vista, `409` y estado `stale`; si la clave ya se aceptó, `200` y estado `duplicate`; si no, `200` y estado `accepted`. Registra cada petición en JSONL: `{"recv_ms","key","epoch","evaluator","status","body"}`.
- Efectiva = `accepted`. Duplicado efectivo = un incidente (regla, grupo, T_f, kind) con más de una petición `accepted`.

### 4.8 Exports de verificación

Con `--export <dir>`, cada proceso escribe JSONL append-only en `<dir>/<proceso>-<run_id>/`, con `run_id` único por arranque:

- `serve`: `buckets.jsonl` `{"tenant","series","end","count","sum","rate","closed_at"}` y, en series con sketch, `"q": {"0.5": v, "0.9": v, "0.99": v, "0.999": v}` y `"card": {"<field>": n}`; `late.jsonl` `{"o": offset}`.
- `evaluate`: `evals.jsonl` `{"rule","group","t","value","closed_at"}` y `transitions.jsonl` `{"rule","group","t","state","value","closed_at"}`.

Tras una recuperación, los scripts toman las líneas de corridas anteriores solo si `closed_at` es menor que el offset de truncado, y las de la corrida nueva; una clave con dos valores distintos es una diferencia.

### 4.9 Streams en vivo

- `GET /v1/tail?tenant=<t>&kind=logs|metrics[&match=k=v,...]`: Server-Sent Events. Cada evento: `id: <logid>-<offset>` y `data:` con el evento en JSON, incluido `"o"`. Sin token empieza en la cabeza del log; con `Last-Event-ID` o `?after=<token>` empieza en el siguiente offset que cumple el filtro.
- `GET /v1/live?tenant=<t>`: SSE de cubetas cerradas con `id: <logid>-m<seq>`, `seq` consecutivo por inquilino y derivado del log; se guarda en el checkpoint. Reanudación dentro de un anillo de las últimas 100 000 emisiones por inquilino.
- Token de otro log: `410 {"code":"unknown_log"}`. Offset o `seq` fuera de lo retenido: `410 {"code":"expired"}`.
- Suscriptor lento: la escritura bloquea (contrapresión de TCP). Si `cabeza − offset del suscriptor > tail.max_lag_events` (1 000 000), se envía `event: error` con `data: {"code":"lagged","last":"<token>"}` y se cierra. Nunca se saltan offsets.

### 4.10 Métricas propias (`GET /metrics`, texto de Prometheus)

`oteadero_events_received_total{tenant}`, `oteadero_events_accepted_total{tenant}`, `oteadero_events_dropped_total{tenant,cause}`, `oteadero_events_late_total{tenant}`, `oteadero_watermark_lag_ms{tenant}` (reloj − W_t), `oteadero_queue_depth_events{queue}`, `oteadero_sketch_observed_rel_error_max` y, en el evaluador, `oteadero_notifications_total{status}`.

## 5. Decisiones cerradas

Cada decisión: la primera línea es la decisión; la segunda, la razón. D01–D21 son las delegadas por la idea; D22 en adelante las abrió el diseño.

**D01. Nombre.**
Decisión: el proyecto se queda con el nombre oteadero.
Razón: el 2026-11-05 no hay proyecto de observabilidad con ese nombre en GitHub, PyPI, crates.io, npm ni el proxy de Go; el único repositorio es `CesarManzoCode/oteadero`, de César.

**D02. Lenguajes.**
Decisión: motor, generador, receptor y simulación en Go 1.27 solo con biblioteca estándar; oráculo y scripts en Python 3.14 solo con biblioteca estándar.
Razón: la vacante nombra Go; VictoriaMetrics también es Go, así que la brecha de rendimiento compara el mismo runtime; sin dependencias externas la compilación no necesita red; un oráculo en otro lenguaje no puede compartir código con el motor.

**D03. Arquitectura y concurrencia.**
Decisión: los componentes del núcleo (WAL, secuenciador, agregación, reglas, evaluador, coordinador) son máquinas de estado síncronas sin E/S propia que reciben reloj, azar, disco y red por interfaces (`clock.Clock`, `rng.Source`, `vfs.FS`, transporte); en producción cada una corre en su goroutine unida por canales acotados, y en la simulación se invocan desde un solo hilo. Los handlers HTTP parsean en paralelo; un único secuenciador asigna offsets; `shards` (4 por omisión) agregan por hash FNV-1a de la identidad de serie.
Razón: un solo secuenciador hace el orden del log total y barato (unos ns por evento), el paralelismo queda en parseo y agregación, y la misma lógica corre en producción y en simulación (R11).

**D04. Protocolo de ingestión.**
Decisión: HTTP/1.1 con texto de exposición de Prometheus para métricas y NDJSON para logs (§4.1); sin gRPC, remote write ni OTLP.
Razón: los mismos bytes se envían a VictoriaMetrics en el benchmark, así que la carga es idéntica; se parsea a mano sin dependencias; gRPC exigiría módulos externos y generación de código.

**D05. Log y checkpoints.**
Decisión: log según §4.3; group commit con fsync cuando hay 4 MiB pendientes o pasan 2 ms desde el primer lote sin fsync, y ack después del fsync; checkpoint cada 10 s o cada 2 000 000 de eventos en `<data>/ckpt/<offset>.ckpt` (magic `OTCKPT01`, versión, CRC-32, estado), escrito en temporal + fsync + rename + fsync del directorio, conservando los 2 últimos; un segmento se borra solo si todo él queda antes del checkpoint más antiguo y fuera de la retención del tail (50 000 000 eventos).
Razón: el fsync por lotes amortiza el costo a millones de eventos por segundo con latencia de ack de milisegundos; el rename atómico garantiza que siempre hay un checkpoint válido.

**D06. Ventanas, watermarks y tardíos.**
Decisión: cubetas fijas de 1 s abiertas por la izquierda, ventanas de regla como unión de cubetas (ventana deslizante con paso de cubeta), watermark por inquilino `maxTs − 2 s` y ticks de inactividad en el log (§4.4); un evento fuera de la política de lateness queda en el log, se cuenta como tardío, aparece en el live tail y no entra en ninguna ventana.
Razón: el límite abierto por la izquierda coincide con el medido en VictoriaMetrics (ventana `(T−R, T]`); el watermark por inquilino evita que uno ruidoso cierre ventanas ajenas; los ticks en el log hacen que el cierre por inactividad también sea función del log.

**D07. Sketches.**
Decisión: DDSketch con almacenes positivo, negativo y cero, sin colapso de cubetas, γ construido con α interno 0.0099 y cota publicada α = 0.01, cuantil inferior de rango `⌊q(n−1)⌋` (índice desde 0); HyperLogLog con p = 14 (16 384 registros de 8 bits), hash FNV-1a 64 seguido del finalizador fmix64, corrección de rango pequeño por linear counting (E ≤ 5m/2), cota publicada 5% de error relativo por ventana. Cuantiles declarados: 0.5, 0.9, 0.99, 0.999. Los sketches se calculan solo para las métricas en `quantile_metrics` y los campos en `cardinality_fields` de cada inquilino.
Razón: DDSketch tiene cota relativa determinista y mezcla exacta (VLDB 2019); el margen 0.0099→0.01 absorbe el redondeo de `log`; el error estándar de HLL es 1.04/√m = 0.81%, y 5% son unas 6 desviaciones; sin colapso la cota vale en todo el rango.

**D08. Semántica de alertas.**
Decisión: máquina de estados de §4.6, con `for` y `keep_firing_for` idénticos a Prometheus 3.16.0 y la histéresis como umbral de permanencia propio; agrupación por `by`; un grupo ausente tiene condición falsa y reinicia `pending`.
Razón: coincidir con Prometheus hace posible la comparación externa; la histéresis no existe en Prometheus, así que solo se verifica contra el oráculo.

**D09. Failover y exclusión.**
Decisión: 3 evaluadores; el coordinador de `serve` concede arrendamientos de 2000 ms con época monótona escrita como registro LEASE en el log antes de responder; el líder renueva cada 500 ms y deja de notificar al pasar `envío de la renovación + 2000 − 250 ms` de su reloj monótono; el receptor rechaza épocas menores que la mayor vista (fencing).
Razón: la época durable en el log no retrocede tras caídas del coordinador; el margen de 250 ms cubre la deriva entre relojes locales; el fencing en el receptor cierra el caso del líder pausado que despierta.

**D10. Idempotencia y receptor.**
Decisión: claves de §4.7, derivadas solo de (regla, grupo, T_f, kind); entrega at-least-once con reintentos exponenciales de 100 ms a 2 s mientras haya arrendamiento; el outbox de cada evaluador guarda en su checkpoint las claves confirmadas; al ser promovido, un evaluador reenvía lo no confirmado por él de los últimos 10 minutos de tiempo de evento.
Razón: exactamente una vez no es alcanzable sin el receptor; con claves estables, cualquier reenvío tras caída o failover se deduplica allí.

**D11. Streams en vivo.**
Decisión: SSE con tokens `<logid>-<offset>` y `<logid>-m<seq>` (§4.9); el tail lee del log en disco al ritmo del suscriptor; corte `lagged` a 1 000 000 de eventos de retraso.
Razón: SSE trae reanudación estándar (`Last-Event-ID`); leer del log hace que la contrapresión no consuma memoria; el id del log impide reanudar contra otro log.

**D12. Subconjunto comparado con vmalert y Prometheus.**
Decisión: un solo inquilino, sin tardíos, reglas `count_over_time(sel[R]) > c`, `sum_over_time(sel[R]) / (R en s) > c` y `sum by (l) (count_over_time(sel[R])) > c`, con `for`; `timeFrom` de replay igual al primer instante de evaluación del motor (múltiplo del intervalo) y `-rule.evalDelay=0s`; VictoriaMetrics con `-search.disableCache` y `-search.latencyOffset=0s`; valores enteros. `keep_firing_for` se compara con `promtool test rules` y `vmalert-tool unittest --disableAlertgroupLabel` sobre series regulares desde t = 0, con el motor sin ticks.
Razón: MetricsQL no extrapola y estas funciones son exactas; la ventana `(T−R, T]` y los instantes `timeFrom + k·intervalo` del replay se midieron en esta máquina; los enteros evitan diferencias de redondeo por orden de suma; el replay ignora `keep_firing_for`.

**D13. promtool test rules.**
Decisión: `promtool test rules` existe y sirve como referencia de `keep_firing_for`; `vmalert-tool unittest` es la segunda.
Razón: en Prometheus 3.16.0 y vmalert-tool v1.155.0 ambos dieron el mismo resultado de `keep_firing_for` en esta máquina.

**D14. Umbrales numéricos.**
Decisión: throughput ≥ 1 000 000 de eventos aceptados (con fsync) por segundo sostenidos 60 s, con el motor en 8 CPUs lógicas (0-3,6-9) y el generador en 4 (4,5,10,11); memoria residente máxima del motor ≤ 4 GiB; p99 de latencia de notificación ≤ 8000 ms en la corrida con fallas; α = 0.01; cardinalidad ≤ 5%; barrido de 200 semillas; cuatro inquilinos con cuota de 200 000 eventos/s, ráfaga de 1 s y peso 1, uno de ellos ruidoso que envía 5 veces su cuota; cada inquilino no ruidoso obtiene al menos el 99% de lo que envía.
Razón: 1 M/s es el piso de "millones por segundo" y VictoriaMetrics declara unos 300 000/s por núcleo, así que 8 hilos de un Ryzen 5 5600G lo alcanzan sin historial; 8000 ms cubren S + L + arrendamiento + renovación + reproducción; el umbral solo puede subir tras medir, nunca bajar.

**D15. Generador de carga.**
Decisión: `otgen` con PCG de `math/rand/v2` sembrado y perfiles fijos `basic`, `late`, `saturation`, `fairness`, `alerts`, `tail`, `bench`, `reference`; retraso de evento: 90% sin retraso, 9% uniforme en (0, L), 1% uniforme en (L, 3L); valores gauge lognormales, contadores delta enteros; en `alerts`, cruces de umbral, oscilación junto al umbral y huecos sin datos; escribe un reporte JSON por inquilino con enviados, confirmados, rechazados por causa y errores de conexión.
Razón: la distribución conocida permite contar los tardíos esperados; los huecos y oscilaciones ejercitan `for`, histéresis y el mutante 4.

**D16. Simulación determinista.**
Decisión: `harness/sim` con un planificador de un solo hilo sobre tiempo virtual (cola de prioridad por instante y número de secuencia), un PCG sembrado para toda decisión, disco simulado que en una caída pierde lo no sincronizado y puede romper el último registro, red simulada con retardo, reordenamiento, pérdida y particiones, y pausas y caídas de proceso; el registro es una línea por evento entregado (`t`, nodo, tipo, SHA-256 de 8 bytes del payload) y su hash es SHA-256 del registro completo.
Razón: es el esquema de TigerBeetle y FoundationDB; con un solo hilo y un solo PRNG la misma semilla reproduce todo.

**D17. Mutantes.**
Decisión: 8 parches en `harness/mutants/`: M1 watermark sin retraso L; M2 clave de idempotencia aleatoria por envío; M3 reanudación de tail en el offset del token; M4 `pending` no se reinicia con grupo ausente; M5 tardío con `<` en vez de `≤`; M6 reproducción desde el offset del checkpoint menos uno; M7 el coordinador concede un arrendamiento a un holder nuevo sin incrementar la época; M8 descarte por `quota` sin contar.
Razón: los cuatro primeros los exige la idea; M5–M8 cubren los bordes de lateness, recuperación, exclusión por épocas y contabilidad, que son los otros invariantes que el proyecto afirma; M7 y M8 se eligieron así porque con fencing activo un líder sin arrendamiento no viola ningún invariante, y `queue_full` puede no ocurrir en la carga de saturación, mientras que `quota` siempre ocurre.

**D18. Benchmark frente a VictoriaMetrics.**
Decisión: misma carga `bench` (100 000 series, lotes de 10 000 líneas, 8 conexiones) enviada 60 s al motor y a VictoriaMetrics v1.155.0 (`/api/v1/import/prometheus`), ambos con `taskset -c 0-3,6-9` y el generador en `4,5,10,11`; throughput = eventos confirmados por segundo; se imprimen ambas cifras, el cociente, núcleos, CPU, versiones y RSS máximo (`VmHWM` de `/proc/<pid>/status`). Conclusión permitida: comparable, no equivalente: el motor confirma tras fsync y no guarda historial; VictoriaMetrics guarda historial y confirma antes de persistir. La brecha es un costo, nunca un "más rápido".
Razón: es la regla de conclusión del método; los mismos bytes y los mismos núcleos aíslan el motor.

**D19. Herramientas.**
Decisión: `go build`, `go test`, `go vet` y `gofmt`; `python3 -m unittest`; `make check` (formato, vet, tests y límite del oráculo) como comando único de chequeo y el del CI; `make criteria` corre todos los `scripts/crit_*.py` y trata 77 como omitido; `make refs` descarga los binarios de referencia fijados con SHA-256; medición con `taskset`, `/proc` y reloj monótono.
Razón: todo está instalado en la máquina de César y no requiere root.

**D20. Orden de fases.**
Decisión: K1 ingestión durable, ventanas exactas y oráculo; K2 sketches, alertas, evaluador replicado y comparación con referencias; K3 streams en vivo, simulación determinista, métricas propias, mutantes y rendimiento.
Razón: cada K se apoya en el log y el oráculo del anterior, y las interfaces inyectables existen desde K1 para que la simulación de K3 no obligue a reescribir.

**D21. Huecos de la investigación.**
Decisión: HLL con cota de Flajolet et al. 2007; t-digest no se usa; alineación de vmalert en replay medida (§D12); los binarios de Prometheus y VictoriaMetrics corren sin root en Arch (medido); Loki, Vector y Mimir no se usan como contexto; arquitectura x86-64 confirmada (Ryzen 5 5600G, 12 CPUs lógicas).
Razón: cada punto se cerró con fuente primaria o con ejecución en esta máquina (`vault/RESEARCH.md`).

**D22. Formato de reglas.**
Decisión: reglas en JSON (§4.5), y `scripts/rules2prom.py` traduce el subconjunto a YAML de Prometheus.
Razón: Go no trae parser de YAML y no se añaden dependencias; promtool y vmalert reciben YAML generado.

**D23. Separación del arnés.**
Decisión: el oráculo vive en `oracle/`, importa solo la biblioteca estándar y módulos de `oracle/`, no lee archivos de `internal/` ni `cmd/` ni invoca binarios del motor; `scripts/crit_oracle_imports.py` lo comprueba.
Razón: es el criterio de independencia de la idea y la única forma de que una igualdad con el oráculo signifique algo.

**D24. Binarios de referencia.**
Decisión: `scripts/fetch_refs.py` descarga Prometheus 3.16.0 y VictoriaMetrics v1.155.0 (`victoria-metrics-prod`, `vmalert-prod`, `vmalert-tool-prod`) a `.refbin/`, verifica los SHA-256 de `vault/RESEARCH.md`, y los scripts los buscan ahí o en `PATH`; si faltan, salen con 77.
Razón: no hay paquetes del sistema ni root; versiones fijadas hacen la comparación reproducible.

**D25. Corridas con fallas reales.**
Decisión: los criterios de recuperación y notificación corren procesos reales y usan `kill -9` sembrado; el truncado parte el último registro del log sin bajar del offset del checkpoint más reciente; la latencia se mide como `recv_ms − T` con eventos cuyo `ts` es el reloj de pared al enviarse.
Razón: sin root no hay `tc` ni `iptables`; las particiones van a la simulación (D16) y las caídas de proceso son reales.

**D26. Tamaño relativo.**
Decisión: el criterio de tamaño cuenta líneas no vacías y sin comentario de `cmd/` e `internal/` (motor) frente a `harness/`, `oracle/` y `scripts/` (arnés), sin tests unitarios.
Razón: la idea enumera el arnés como oráculo, generador, scripts y simulación; los tests unitarios acompañan a ambos lados.

## 6. Contradicciones resueltas

- method.md pone como referencia de presupuesto que la verificación propia no pase de cerca del 15% del producto; la idea exige solo que el arnés mida menos que el motor. Gana la idea en el producto: el arnés (oráculo, simulación, generador, scripts) es parte de lo que el proyecto demuestra y su criterio es D26; el 15% se aplica a la ceremonia, los checkers de `vault/private/checks/`, y se mide al cierre de cada K.
- La idea pide un "comando único" que corra todos los criterios, y method.md un comando de chequeo que corre limpio antes de cada K. Son dos comandos: `make check` (rápido, el de kiln y el CI) y `make criteria` (todos los criterios mecánicos, con 77 como omitido).
- La idea dice "umbral exacto fijado tras medir" y K0 no puede medir. D14 fija el piso contractual de 1 000 000 eventos/s; tras medir solo puede subir, y el cambio va al CHANGELOG.

## 7. Criterios mecánicos

Cada criterio de la idea es un script en `scripts/` que sale con 0 si pasa, con 1 si falla y con 77 si falta un binario externo.

| Script | Criterio | K |
| --- | --- | --- |
| `crit_build.py` | Compilación sin root ni red (`GOPROXY=off`) | K1 |
| `crit_oracle_imports.py` | El oráculo no importa ni lee el motor | K1 |
| `crit_windows.py` | `ventanas: 0 diferencias` sin fallas | K1 |
| `crit_windows_late.py` | 0 diferencias con fuera de orden y tardíos, también en la clasificación | K1 |
| `crit_recovery.py` | Tras `kill -9` y truncado, ventanas iguales al oráculo sobre el log recuperado | K1 |
| `crit_accounting.py` | `aceptados + descartados = enviados` por inquilino en saturación | K1 |
| `crit_fairness.py` | Inquilinos no ruidosos con al menos su cuota | K1 |
| `crit_quantiles.py` | Error relativo ≤ α por cubeta y cuantil; imprime el máximo | K2 |
| `crit_quantiles_merged.py` | Lo mismo sobre sketches mezclados entre cubetas, series y shards | K2 |
| `crit_cardinality.py` | Cardinalidad dentro de 5% | K2 |
| `crit_transitions.py` | `transiciones: 0 diferencias`; también tras recuperación del evaluador | K2 |
| `crit_promtool_check.py` | `promtool check rules` del subconjunto | K2 |
| `crit_vmalert_replay.py` | Igualdad con vmalert en replay | K2 |
| `crit_keep_firing_for.py` | Igualdad con `promtool test rules` y `vmalert-tool unittest` | K2 |
| `crit_notify_once.py` | 0 perdidos y 0 duplicados efectivos tras `kill -9` | K2 |
| `crit_alert_latency.py` | p99 ≤ 8000 ms; imprime p50, p99 y máximo | K2 |
| `crit_tail_resume.py` | Offsets contiguos sin repetidos tras reanudaciones; imprime cuántas | K3 |
| `crit_slow_subscriber.py` | Contrapresión o corte `lagged`, nunca salto | K3 |
| `crit_self_metrics.py` | Métricas propias iguales a oráculo y generador | K3 |
| `crit_sim_same_seed.py` | Misma semilla, mismo hash; imprime el hash | K3 |
| `crit_sim_diff_seed.py` | Semillas distintas, hashes distintos | K3 |
| `crit_sim_sweep.py` | 200 semillas con fallas pasan los invariantes; imprime cuántas | K3 |
| `crit_split_brain.py` | Nunca dos evaluadores aceptados en la misma época e incidente | K3 |
| `crit_mutants.py` | Los 8 mutantes detectados y el código sin mutar pasa | K3 |
| `crit_bench.py` | Throughput ≥ umbral; cifras de VictoriaMetrics y cociente (77 si falta) | K3 |
| `crit_bench_rss.py` | RSS máximo ≤ 4 GiB; imprime el pico | K3 |
| `crit_size.py` | Arnés menor que motor | K3 |
| `run_criteria.py` | Corre todos; 0 solo si todos pasan; 77 se reporta como omitido | K1 |

El criterio no mecánico de la idea (dificultad frente a proyectos anteriores) no tiene script: se sostiene solo con los resultados de estos.
