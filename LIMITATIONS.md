# Límites de oteadero

Fuente única de lo que el proyecto no demuestra. Ningún otro archivo repite estos límites.

## Alcance

- Una sola máquina. No hay clúster multi-máquina, multirregión, latencia WAN ni tráfico de producción; los evaluadores replicados son procesos locales.
- El proceso de ingestión (`serve`) no está replicado: si cae, no se ingiere ni se renuevan arrendamientos hasta que vuelve.
- No hay almacenamiento histórico ni lenguaje de consultas: la persistencia es solo el log de entrada y los checkpoints.
- Los descartes de inquilinos desconocidos se cuentan por id solo para los primeros 1024 ids distintos; los demás se suman juntos bajo `_other`.
- Los descartes no están en el log: tras una caída, los ocurridos desde el último checkpoint se pierden de los contadores, igual que los lotes en cola sin confirmar. El balde de cuota arranca lleno tras cada reinicio.
- Un cuerpo por encima de `max_body_bytes`, o cortado por el cliente, no se lee entero: sus descartes se cuentan por las líneas recibidas, no por las que el productor envió.
- Nada impide arrancar dos `serve` sobre el mismo `data_dir` con puertos distintos; el log solo admite un escritor y esa corrida lo corrompería.
- Un valor NaN cuenta en `count` y `sum` de su cubeta, pero no entra en sus cuantiles, y una cubeta con solo NaN exporta los cuantiles como `NaN`. ±Inf se ordenan en los extremos y se devuelven exactos.
- La cota de error de los cuantiles vale para valores normales de float64. Con subnormales (|x| < 2^−1022) la propia representación puede redondear el estimador más allá de α.
- Cambiar `quantile_metrics` o `cardinality_fields` hace fallar la recuperación si el checkpoint tiene cubetas abiertas de las series afectadas, en lugar de mezclar estados distintos.
- Cada HyperLogLog ocupa 16 KiB por campo y cubeta abierta, y un DDSketch sin colapso crece con el rango de sus valores: la memoria de las series con sketch crece con series × campos × cubetas abiertas, sin un tope propio.
- El error observado de `oteadero_sketch_observed_rel_error_max` se mide solo sobre 1 de cada 1000 cubetas con DDSketch; las cubetas auditadas guardan todos sus valores en memoria y en el checkpoint.
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
- El cierre de K1 conservó el veredicto de cada criterio, no su salida: no hay cifras publicadas de ventanas comparadas, tardíos, bytes truncados, eventos enviados o descartados, ni cocientes de reparto.
- `crit_fairness.py` decide solo con t1, t2 y t3, y con los confirmados que reporta el generador, no con el motor ni con el log. No falla si t4 no supera su cuota ni si no llega a 5 veces esa cuota, así que su código de salida no prueba que hubo un inquilino ruidoso.
- `crit_accounting.py` sale con 0 aunque la corrida no tenga ningún descarte, así que su código de salida no prueba que hubo saturación.
- Los criterios de K1 con carga corren una sola semilla fija cada uno. `crit_recovery.py` inyecta una sola caída y un solo truncado por corrida.
- `crit_accounting.py` y `crit_fairness.py` corren 10 s, no los 30 s que el generador usa por omisión en esos perfiles.
- Los criterios de K1 que arrancan el motor corren con `idle_grace_ms` en 0, porque sus timestamps sintéticos quedan muy por detrás del reloj y un TICK cerraría todas las cubetas. El cierre por TICK solo está cubierto por tests unitarios del motor y del oráculo.
- El oráculo trata como fin del log cualquier registro inválido, cabecera mala, otro id de log o hueco de offsets, y no lo reporta: no distingue un final roto de una corrupción intermedia.
- El campo `late_generated` del reporte de `otgen` cuenta el retraso nominal, no los tardíos reales: separar métricas y logs en lotes distintos puede volver tardío un log cuyo retraso nominal era menor que L.

## Rendimiento

- Las cifras de rendimiento son de una máquina (Ryzen 5 5600G, 16 GB) y no se extrapolan.
- La comparación con VictoriaMetrics es comparable, no equivalente: VictoriaMetrics guarda historial y confirma antes de persistir; oteadero confirma tras fsync y no guarda historial. La brecha es un costo, no un "más rápido".
