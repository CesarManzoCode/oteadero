# Changelog

## [Unreleased]

### Added

- Comparación con Prometheus y VictoriaMetrics (§4.5, §4.6, §7, D12, D13, D22, D24): `make refs` descarga promtool 3.16.0, `victoria-metrics-prod`, `vmalert-prod` y `vmalert-tool-prod` v1.155.0 a `.refbin/`, verifica sus SHA-256 y no repite lo ya verificado; `scripts/rules2prom.py` traduce las reglas `count`, `sum` y `rate`, con `by`, `for` y `keep_firing_for`, a YAML de Prometheus con un grupo por intervalo y rechaza las que están fuera del subconjunto; `otgen` llena los perfiles `reference` (un inquilino, valores enteros, sin retrasos) y `reference-kff` (series regulares desde t = 0); `crit_promtool_check.py` pasa `promtool check rules` sobre `rules/reference.json` y `rules/kff.json`, `crit_vmalert_replay.py` compara las transiciones del evaluador con las que vmalert reconstruye en replay sobre la misma carga en VictoriaMetrics, y `crit_keep_firing_for.py` comprueba con `promtool test rules` y `vmalert-tool unittest` las alertas `firing` del motor en cada instante; los tres salen con 77 si falta un binario.
- Criterios de notificación con fallas (§4.7, §7, D09, D10, D14, D25): `otgen --profile alerts --realtime` envía 90 s de la carga de alertas con el reloj de pared, una fase por segundo real; `crit_notify_once.py` corre `serve`, `otrecv` y los evaluadores e1, e2 y e3, mata con `kill -9` en instantes sembrados al líder y a otros evaluadores y los rearranca tras 1–3 s, y comprueba contra los incidentes del oráculo sobre el log final que no hay notificaciones perdidas, duplicados efectivos ni aceptadas desconocidas; `crit_alert_latency.py` mide `recv_ms − t` de cada notificación aceptada de esa corrida, imprime p50, p99 y máximo y exige p99 ≤ 8000 ms.
- Criterios de sketches y transiciones (§4.5, §4.8, §7, D07, D14, D15): `otgen --profile alerts` genera en modo sintético un inquilino con `latency_ms` lognormal, `errors` en deltas enteros y logs con `user`, en fases de normalidad, cruce de umbral, oscilación junto al umbral y huecos de 15 s sin datos, con los retrasos de D15; `rules/alerts.json` cubre `count`, `sum`, `rate`, `quantile` y `cardinality`, con y sin `by`, `for`, `keep_firing_for` e histéresis; `crit_quantiles.py`, `crit_quantiles_merged.py` y `crit_cardinality.py` comparan cuantiles por cubeta, cuantiles de ventanas mezcladas y cardinalidades con el valor exacto del oráculo e imprimen el error relativo máximo, y `crit_transitions.py` compara las transiciones del evaluador con las del oráculo, también tras `kill -9` del evaluador.
- Receptor de notificaciones de prueba y oráculo de reglas (§4.4–§4.8, D07, D10, D23): `otrecv --listen <addr> --log <archivo>` rechaza con 409 las épocas menores que la mayor vista, responde 200 a las claves ya aceptadas como duplicadas, escribe y sincroniza una línea JSONL por petición antes de responder y publica los conteos por estado en `GET /stats`; `python3 oracle/cli.py exact` da por cubeta el cuantil inferior de rango y el número exacto de valores distintos por campo, y con `--rules` los de cada ventana de regla; `rules` reproduce desde el log los instantes de evaluación, los valores exactos de `count`, `sum` y `rate` y la máquina de estados, y decide la condición de `quantile` y `cardinality` con el valor exportado por el motor (`--engine-evals`); `incidents` deriva de esas transiciones los incidentes y sus claves de idempotencia.
- Evaluador replicado y notificaciones (§4.3, §4.7, §4.8, §4.10, D03, D09, D10): `serve` concede arrendamientos en `POST /v1/lease` con épocas que crecen, escritas como registro LEASE y sincronizadas antes de responder, y que sobreviven a caídas y a la limpieza de segmentos; `oteadero evaluate` sigue el log en solo lectura con la misma agregación y las mismas reglas, tiene sus propios checkpoints en `<data_dir>/eval-<id>/ckpt/` y exporta `evals.jsonl` y `transitions.jsonl`; solo el líder con arrendamiento vigente notifica cada disparo y resolución con su clave de idempotencia, reintenta de 100 ms a 2 s y, al ser promovido, reenvía lo que él no confirmó de los últimos 10 min de tiempo de evento; `/metrics` publica `oteadero_notifications_total{status}`.
- Reglas de alerta y su evaluación (§4.5–§4.7, D08, D22): carga y validación del JSON de reglas; ventanas `(T−R, T]` por regla y grupo sobre las cubetas cerradas, con conteo, suma, tasa, cuantil y cardinalidad; máquina de estados con `for`, `keep_firing_for` e histéresis; `incident_id` y claves de idempotencia, y estado serializable para el checkpoint. Aún sin proceso ni notificaciones.
- Cuantiles y cardinalidad por cubeta (§4.8, §4.10, D07): las series de `quantile_metrics` llevan un DDSketch con error relativo publicado de 1% y las de logs un HyperLogLog por campo de `cardinality_fields`; `buckets.jsonl` añade `q` y `card` en esas series, los sketches entran en el checkpoint y 1 de cada 1000 cubetas con DDSketch guarda sus valores exactos para publicar el error observado en `oteadero_sketch_observed_rel_error_max`.

## [k1] - 2026-11-05

Ingestión durable, ventanas exactas y oráculo independiente.

### Added

- Criterios mecánicos de K1 (§4.8, §7, D14, D19, D25): `make build` sin root ni red, ventanas iguales al oráculo sin fallas y con tardíos, recuperación tras `kill -9` y truncado del log, contabilidad por inquilino en saturación y reparto justo; `python3 scripts/run_criteria.py` (o `make criteria`) los corre en orden, trata 77 como omitido y falla si alguno falla.
- Oráculo independiente en Python (§4.3, §4.4, §4.8, D23): `python3 oracle/cli.py windows` recalcula desde los segmentos crudos del log, sin compartir código con el motor, las cubetas cerradas, los tardíos y los aceptados y tardíos por inquilino; `scripts/crit_oracle_imports.py` falla si el oráculo importa algo fuera de la biblioteca estándar y de sus propios módulos o si menciona rutas o el binario del motor.
- Generador de carga `otgen` (§4.1, D15, D18): perfiles sembrados `basic`, `late`, `saturation`, `fairness` y `bench` que producen los mismos lotes con la misma semilla, timestamps sintéticos o de reloj (`--realtime`), envío al motor o a VictoriaMetrics (`--vm`), medición de su propia tasa (`--sink null`) y reporte JSON por inquilino con enviados, confirmados, descartes por causa y errores.
- Servidor de ingestión (§4.1, §4.10): `oteadero serve --config <archivo> [--export <dir>]` recupera, recibe lotes por `POST /v1/metrics` y `POST /v1/logs` y los confirma tras el fsync, expone `GET /metrics` y `GET /healthz` y, ante SIGTERM o SIGINT, responde 503 `shutting_down`, vacía, hace checkpoint y sale con 0; configuración JSON con valores por omisión que rechaza campos desconocidos; `oteadero version`.
- Agregación y recuperación (§4.4, §4.8, D03, D05, D06): cubetas abiertas por la izquierda repartidas en shards por hash de serie, cerradas por watermark o TICK con su `closed_at` y con el mismo resultado para cualquier número de shards; export de verificación `buckets.jsonl` y `late.jsonl`; checkpoints atómicos con CRC-32 que conservan los 2 últimos y permiten borrar segmentos antiguos; recuperación desde el checkpoint válido más reciente reproduciendo el log por el camino de la ingestión en vivo, sin reconfirmar a nadie.
- Admisión y secuenciador de ingestión (§4.1, §4.3, §4.4, D03, D05): cuota por inquilino con token bucket y ráfaga, colas acotadas servidas con Deficit Round Robin ponderado, secuenciador único que asigna offsets, clasifica tardíos con el watermark por inquilino, escribe TICKs de inactividad y confirma cada lote solo tras el fsync del group commit (4 MiB o 2 ms); todo descarte se cuenta por inquilino y causa.
- Modelo de eventos y parsers de ingestión (§4.1–§4.3): texto de exposición de Prometheus con timestamp obligatorio para métricas y NDJSON para logs, lote atómico que se rechaza entero (`invalid`, `too_large` por encima de 100 000 eventos), clave canónica de serie con escapes, hash FNV-1a de la identidad para los shards y codificación binaria del payload BATCH.
- Log de entrada durable (§4.3): segmentos con CRC-32, rotación a 256 MiB, lectura desde una posición o un offset y recuperación que trunca el registro final roto; reloj, azar y disco inyectables, con un disco en memoria que simula caídas.

### Fixed

- `CountDrop` creaba una segunda entrada de contadores para un inquilino configurado: `/metrics` repetía sus líneas y, al restaurar un checkpoint, esa entrada en cero borraba los contadores reales del inquilino.
- Los perfiles `saturation` y `fairness` de `otgen` enviaban por una sola conexión por inquilino, que espera el fsync de cada lote: los acks, no la tasa, limitaban la carga, que podía quedar por debajo de la cuota. Ahora usan 8 conexiones por inquilino, de modo que `crit_accounting.py` y `crit_fairness.py` corren con el inquilino por encima de su cuota.

### Decisions

- Los perfiles con tasa de `otgen` envían por 8 conexiones por inquilino, para que la tasa, y no la espera de los acks, fije lo que ofrece cada uno.
- `crit_accounting.py` y `crit_fairness.py` corren 10 s en lugar de los 30 s del perfil, para que el oráculo lea menos log.
- Los criterios que arrancan el motor corren con `idle_grace_ms` en 0, porque sus timestamps sintéticos quedan muy por detrás del reloj.

## [k0] - 2026-11-05

### Added

- Fundación del proyecto: contrato con las decisiones de diseño, investigación con fuentes y versiones fijadas, límites declarados, comando de chequeo (`make check`) y CI que lo corre.

### Decisions

- Motor en Go 1.27 sin dependencias externas; oráculo y scripts en Python 3.14 con biblioteca estándar.
- Ingestión HTTP con texto de exposición de Prometheus y NDJSON; log append-only con CRC-32 y group commit; cubetas de 1 s abiertas por la izquierda con watermark por inquilino de 2 s.
- DDSketch con α = 0.01 y HyperLogLog con p = 14; semántica de `for` y `keep_firing_for` igual a Prometheus 3.16.0.
- Tres evaluadores con arrendamiento de 2 s, épocas en el log y fencing en el receptor.
- Umbral de throughput de 1 000 000 eventos/s con fsync en 8 CPUs lógicas.
