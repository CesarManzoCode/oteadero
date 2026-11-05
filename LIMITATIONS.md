# Límites de oteadero

Fuente única de lo que el proyecto no demuestra. Ningún otro archivo repite estos límites.

## Alcance

- Una sola máquina. No hay clúster multi-máquina, multirregión, latencia WAN ni tráfico de producción; los evaluadores replicados son procesos locales.
- El proceso de ingestión (`serve`) no está replicado: si cae, no se ingiere ni se renuevan arrendamientos hasta que vuelve.
- No hay almacenamiento histórico ni lenguaje de consultas: la persistencia es solo el log de entrada y los checkpoints.
- Los descartes de inquilinos desconocidos se cuentan por id solo para los primeros 1024 ids distintos; los demás se suman juntos bajo `_other`.
- Los descartes no están en el log: tras una caída, los ocurridos desde el último checkpoint se pierden de los contadores, igual que los lotes en cola sin confirmar. El balde de cuota arranca lleno tras cada reinicio.
- En los exports, una suma o tasa NaN o ±Inf se escribe con los tokens `NaN`, `Infinity` y `-Infinity`, que lee el módulo `json` de Python pero no son JSON estricto.
- No hay trazas; solo métricas (contadores delta y gauges) y logs estructurados.
- La interfaz es HTTP/1.1 con texto de Prometheus y NDJSON. No se implementa gRPC, remote write de Prometheus ni OTLP, y no se demuestra "millones de usuarios".
- No se demuestran Terraform, Ansible, infraestructura inmutable, el borde TypeScript/GraphQL ni operar ClickHouse o VictoriaMetrics a escala; estos dos solo se usan como referencia.

## Verificación

- La comparación con vmalert y Prometheus cubre solo el subconjunto de reglas de `vault/CONTRACT.md` (D12): un inquilino, sin tardíos, valores enteros, `count_over_time` y `sum_over_time` con `for`, y `keep_firing_for` solo con pruebas unitarias. Cuantiles, cardinalidad e histéresis se verifican solo contra el oráculo propio.
- Las particiones de red y las pausas solo se inyectan en la simulación determinista; sin root no hay `tc` ni `iptables`. Las corridas con procesos reales inyectan `kill -9` y truncado del log.
- La simulación corre los componentes del núcleo; las capas HTTP y del sistema operativo se reemplazan por modelos.
- El truncado del log emula una escritura parcial del final; no prueba la durabilidad ante un corte de energía.
- "Una sola vez" es efectiva en el receptor, que deduplica por clave de idempotencia; la entrega es al menos una vez.
- La cota de cardinalidad es estadística (HyperLogLog), no determinista como la de DDSketch.
- La latencia de alerta se mide desde el tiempo de evento del instante de evaluación, que en esa corrida coincide con el reloj de pared al enviar.

## Rendimiento

- Las cifras de rendimiento son de una máquina (Ryzen 5 5600G, 16 GB) y no se extrapolan.
- La comparación con VictoriaMetrics es comparable, no equivalente: VictoriaMetrics guarda historial y confirma antes de persistir; oteadero confirma tras fsync y no guarda historial. La brecha es un costo, no un "más rápido".
