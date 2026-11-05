# Changelog

## [Unreleased]

### Added

- Modelo de eventos y parsers de ingestión (§4.1–§4.3): texto de exposición de Prometheus con timestamp obligatorio para métricas y NDJSON para logs, lote atómico que se rechaza entero (`invalid`, `too_large` por encima de 100 000 eventos), clave canónica de serie con escapes, hash FNV-1a de la identidad para los shards y codificación binaria del payload BATCH.
- Log de entrada durable (§4.3): segmentos con CRC-32, rotación a 256 MiB, lectura desde una posición o un offset y recuperación que trunca el registro final roto; reloj, azar y disco inyectables, con un disco en memoria que simula caídas.
- Fundación del proyecto: contrato con las decisiones de diseño, investigación con fuentes y versiones fijadas, límites declarados, comando de chequeo (`make check`) y CI que lo corre.

### Decisions

- Motor en Go 1.27 sin dependencias externas; oráculo y scripts en Python 3.14 con biblioteca estándar.
- Ingestión HTTP con texto de exposición de Prometheus y NDJSON; log append-only con CRC-32 y group commit; cubetas de 1 s abiertas por la izquierda con watermark por inquilino de 2 s.
- DDSketch con α = 0.01 y HyperLogLog con p = 14; semántica de `for` y `keep_firing_for` igual a Prometheus 3.16.0.
- Tres evaluadores con arrendamiento de 2 s, épocas en el log y fencing en el receptor.
- Umbral de throughput de 1 000 000 eventos/s con fsync en 8 CPUs lógicas.
