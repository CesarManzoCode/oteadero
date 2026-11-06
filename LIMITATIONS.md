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
- `oteadero evaluate` lee los segmentos sin esperar al fsync de `serve`: un corte de energía o un truncado del log puede dejar notificaciones o líneas de export de registros que la recuperación de `serve` corta después. Si el evaluador está vivo cuando el log se acorta detrás de él, vuelve a su checkpoint con una corrida de export nueva; las notificaciones ya enviadas no se retiran.
- Cada evaluador agrega y evalúa en un solo hilo, así que su ritmo de lectura del log no escala con los núcleos y puede quedar detrás de `serve` con mucha carga. Sigue el log por sondeo cada 10 ms.
- Un evaluador que queda detrás de la retención de segmentos, o sin checkpoint con el primer segmento ya borrado, no se recupera: el proceso termina con error.
- Cambiar el archivo de reglas hace fallar la recuperación del evaluador si tiene checkpoints; hay que borrar `<data_dir>/eval-<id>/`, y entonces sus claves confirmadas se pierden y el receptor deduplica los reenvíos.
- Los reenvíos de una promoción solo cubren 10 min de tiempo de evento por inquilino: una notificación que ningún líder entregó en ese tiempo se abandona, contada con `status="expired"`. Ese estado también cuenta, en los seguidores, las claves que el líder entregó y ellos dejan salir de la ventana.
- Un líder guarda sin tope lo que no ha podido confirmar mientras conserva el arrendamiento, aunque salga de la ventana: con el receptor caído su memoria crece con las transiciones.
- `oteadero_notifications_total` empieza en cero con cada arranque del evaluador; no está en el checkpoint.
- El `/metrics` de `evaluate` solo tiene `oteadero_watermark_lag_ms`, `oteadero_sketch_observed_rel_error_max` y `oteadero_notifications_total`; su retraso de watermark es el de lo que la réplica ya leyó del log, no el de `serve`. Recibidos, aceptados, descartes, tardíos y colas solo están en el de `serve`.
- `oteadero_queue_depth_events{queue="sequencer"}` cuenta los eventos escritos en el log que esperan el fsync; los lotes ya sincronizados que la agregación aún no procesó no tienen métrica de cola.
- En el cuerpo de una notificación, un valor NaN o ±Inf va como `null`, igual que el de un grupo ausente.
- Si se pierde un LEASE ya confirmado, como al truncar a mano el último registro del log, la época puede retroceder y dos evaluadores pueden creer que tienen la misma: las épocas valen mientras el log conserve lo sincronizado.
- Un suscriptor de `/v1/tail` cuyo cliente no lee queda bloqueado en la escritura: el corte `lagged` solo se decide entre registros, así que no le llega hasta que la escritura avanza, y mientras tanto su segmento sigue fijado frente a la limpieza del log.
- Tras un reinicio de `serve`, el anillo de `/v1/live` solo tiene lo que la reproducción desde el checkpoint vuelve a cerrar: un token anterior a ese punto da `expired`, aunque esté entre las últimas 100 000 emisiones.
- Al cerrar `serve`, los streams terminan sin evento final; el cliente reanuda con su último token.
- No hay trazas; solo métricas (contadores delta y gauges) y logs estructurados.
- La interfaz es HTTP/1.1 con texto de Prometheus y NDJSON. No se implementa gRPC, remote write de Prometheus ni OTLP, y no se demuestra "millones de usuarios".
- No se demuestran Terraform, Ansible, infraestructura inmutable, el borde TypeScript/GraphQL ni operar ClickHouse o VictoriaMetrics a escala; estos dos solo se usan como referencia.

## Verificación

- La comparación con vmalert y Prometheus cubre solo el subconjunto de reglas de `vault/CONTRACT.md` (D12): un inquilino, sin tardíos, valores enteros, `count_over_time` y `sum_over_time` con `for`, y `keep_firing_for` solo con pruebas unitarias. Cuantiles, cardinalidad e histéresis se verifican solo contra el oráculo propio.
- Las particiones de red y las pausas solo se inyectan en la simulación determinista; sin root no hay `tc` ni `iptables`. Las corridas con procesos reales inyectan `kill -9` y truncado del log.
- La simulación corre los componentes del núcleo; las capas HTTP y del sistema operativo se reemplazan por modelos.
- En el escenario de `otsim`, cada conexión del suscriptor de tail es una petición que recibe lo sincronizado hasta ese momento y termina; la espera del stream abierto entre fsyncs y la contrapresión de TCP no se simulan, y la desconexión se modela cortando la respuesta en un byte sorteado.
- En ese escenario no caen ni se pausan el receptor, los productores ni el suscriptor; los productores no reintentan los lotes sin confirmación, y los evaluadores leen solo lo sincronizado del disco de `serve`, así que la lectura de registros sin fsync no se ejercita ahí.
- El WAL escribe y sincroniza en la misma llamada, así que una caída simulada pierde los lotes sin fsync pero nunca deja un registro roto en el log; la recuperación de un final roto no se ejercita en la simulación.
- El invariante `accounting` de `otsim` solo compara lo recibido por `serve` con lo que le llegó cuando `serve` no cayó en la corrida; con caídas comprueba `recibidos = aceptados + descartados` y `aceptados = eventos en el log`, por el límite de los descartes tras una caída.
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
- El cierre de K2 tampoco conservó la salida de los criterios, solo su veredicto. No hay cifras publicadas del error relativo máximo de cuantiles o cardinalidad, ni del p50, p99 o máximo de latencia de notificación, ni de cuántos `kill -9`, incidentes o transiciones hubo. Solo consta que cada cifra cumplía su cota (0.01, 5% y 8000 ms).
- En las reglas `quantile` y `cardinality`, el oráculo decide la condición con el valor que exportó el motor, no con el exacto. Sus transiciones prueban la máquina de estados, no el lado del umbral en que cae el sketch. Ese valor solo se juzga contra la cota de error en `crit_quantiles_merged.py` y `crit_cardinality.py`.
- Los criterios de K2 con carga corren una sola semilla fija cada uno. `crit_notify_once.py` es una sola corrida de 90 s, y `crit_alert_latency.py` mide la latencia sobre esa misma corrida, así que el p99 sale de una sola ejecución con fallas.
- `crit_alert_latency.py` reutiliza la corrida que encuentre en `.work/notify_once/` y solo lanza una nueva si no hay ninguna: no comprueba que esa corrida sea del código actual.
- `crit_keep_firing_for.py` construye las alertas esperadas con las que el motor tenía en `firing` en cada instante. Prueba que promtool y vmalert-tool aceptan ese resultado, no los estados `pending`.
- El log del cierre de K2 no registra las versiones ni los SHA-256 de los binarios de referencia que corrieron. Las versiones fijadas son las que verifica `scripts/fetch_refs.py`.

## Rendimiento

- Las cifras de rendimiento son de una máquina (Ryzen 5 5600G, 16 GB) y no se extrapolan.
- La comparación con VictoriaMetrics es comparable, no equivalente: VictoriaMetrics guarda historial y confirma antes de persistir; oteadero confirma tras fsync y no guarda historial. La brecha es un costo, no un "más rápido".
