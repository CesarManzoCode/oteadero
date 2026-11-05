# Changelog

## [Unreleased]

### Added

- Agregación y recuperación (§4.4, §4.8, D03, D05, D06): cubetas abiertas por la izquierda repartidas en shards por hash de serie, cerradas por watermark o TICK con su `closed_at` y con el mismo resultado para cualquier número de shards; export de verificación `buckets.jsonl` y `late.jsonl`; checkpoints atómicos con CRC-32 que conservan los 2 últimos y permiten borrar segmentos antiguos; recuperación desde el checkpoint válido más reciente reproduciendo el log por el camino de la ingestión en vivo, sin reconfirmar a nadie.
- Admisión y secuenciador de ingestión (§4.1, §4.3, §4.4, D03, D05): cuota por inquilino con token bucket y ráfaga, colas acotadas servidas con Deficit Round Robin ponderado, secuenciador único que asigna offsets, clasifica tardíos con el watermark por inquilino, escribe TICKs de inactividad y confirma cada lote solo tras el fsync del group commit (4 MiB o 2 ms); todo descarte se cuenta por inquilino y causa.
- Modelo de eventos y parsers de ingestión (§4.1–§4.3): texto de exposición de Prometheus con timestamp obligatorio para métricas y NDJSON para logs, lote atómico que se rechaza entero (`invalid`, `too_large` por encima de 100 000 eventos), clave canónica de serie con escapes, hash FNV-1a de la identidad para los shards y codificación binaria del payload BATCH.
- Log de entrada durable (§4.3): segmentos con CRC-32, rotación a 256 MiB, lectura desde una posición o un offset y recuperación que trunca el registro final roto; reloj, azar y disco inyectables, con un disco en memoria que simula caídas.
- Fundación del proyecto: contrato con las decisiones de diseño, investigación con fuentes y versiones fijadas, límites declarados, comando de chequeo (`make check`) y CI que lo corre.

### Decisions

- Motor en Go 1.27 sin dependencias externas; oráculo y scripts en Python 3.14 con biblioteca estándar.
- Ingestión HTTP con texto de exposición de Prometheus y NDJSON; log append-only con CRC-32 y group commit; cubetas de 1 s abiertas por la izquierda con watermark por inquilino de 2 s.
- DDSketch con α = 0.01 y HyperLogLog con p = 14; semántica de `for` y `keep_firing_for` igual a Prometheus 3.16.0.
- Tres evaluadores con arrendamiento de 2 s, épocas en el log y fencing en el receptor.
- Umbral de throughput de 1 000 000 eventos/s con fsync en 8 CPUs lógicas.
