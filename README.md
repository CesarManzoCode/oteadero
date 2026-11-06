# oteadero

Motor de telemetría en tiempo real escrito desde cero para una sola máquina. Ingiere métricas y logs estructurados de varios inquilinos con contrapresión y reparto justo, los persiste en un log append-only antes de confirmarlos, agrega por ventanas de tiempo de evento con watermarks, calcula percentiles (DDSketch) y cardinalidad (HyperLogLog) con cotas publicadas, evalúa reglas de alerta en un evaluador replicado con failover, notifica cada incidente una sola vez de forma efectiva y sirve live tail con reanudación sin huecos ni duplicados.

La corrección no se afirma: se mide. Cada resultado del motor se compara con un oráculo independiente que recalcula desde el log crudo, un subconjunto declarado de reglas se compara con vmalert y Prometheus, y el motor corre también dentro de una simulación determinista con fallas inyectadas.

Lo que el proyecto no demuestra está en [LIMITATIONS.md](LIMITATIONS.md). El diseño y sus decisiones están en [vault/CONTRACT.md](vault/CONTRACT.md) y las fuentes en [vault/RESEARCH.md](vault/RESEARCH.md).

## Uso

Requisitos: Go 1.27, Python 3.14, make. Sin root y sin dependencias externas.

```sh
make build      # binarios en bin/
make check      # formato, vet, tests unitarios y límite del oráculo
make refs       # descarga Prometheus 3.16.0 y VictoriaMetrics v1.155.0 fijados por SHA-256
make criteria   # todos los criterios mecánicos; 77 = omitido por falta de binario
```

## Benchmark

`python3 scripts/crit_bench.py` compila los binarios y mide el throughput del motor según D14 y D18; `python3 scripts/crit_bench_rss.py` lee el pico de memoria residente de esa misma corrida.

- Generador: `otgen --profile bench --duration-s 60` (100 000 series, lotes de 10 000 líneas, 8 conexiones) con `taskset -c 4,5,10,11`. Antes, el script mide la tasa del generador solo con `--sink null` en los mismos núcleos.
- Motor: `oteadero serve` con `taskset -c 0-3,6-9` (8 CPUs lógicas), `idle_grace_ms` 0 y cuota de 100 000 000 eventos/s. Throughput = eventos confirmados (tras fsync) / duración de la fase sostenida del reporte de `otgen`; pasa con al menos 1 000 000 eventos/s. La memoria es `VmHWM` de `/proc/<pid>/status` al terminar la carga; pasa con 4096 MiB o menos.
- VictoriaMetrics: v1.155.0 (`victoria-metrics-prod`, de `make refs` o del `PATH`) en los mismos núcleos, con la misma carga enviada por `otgen --vm` a `/api/v1/import/prometheus`. Sin ese binario el script imprime solo el resultado del motor y sale con 77.
- El script imprime la CPU (`/proc/cpuinfo`), la versión de `oteadero version`, la de `go version` y la del kernel. La conclusión permitida es la de D18: comparable, no equivalente.

Requisitos: Go 1.27, Python 3.14, `taskset` y una máquina con al menos 12 CPUs lógicas. Las cifras se publican en Resultados al cerrar K3, con la salida medida.

## Estado

| Fase | Contenido | Estado |
| --- | --- | --- |
| K0 | Fundación: contrato, investigación, comando de chequeo | terminada |
| K1 | Ingestión durable, ventanas exactas y oráculo | Cerrada |
| K2 | Sketches, alertas, evaluador replicado y comparación con vmalert y Prometheus | Cerrada |
| K3 | Streams en vivo, simulación determinista, mutantes y rendimiento | pendiente |

## Resultados

Resultados medidos al cerrar cada fase, con el comando que los produce. Lo que no cubren está en [LIMITATIONS.md](LIMITATIONS.md).

### K1

Veredictos de la verificación de cierre sobre el commit `7a996f2`.

| Comando | Criterio | Resultado |
| --- | --- | --- |
| `make check` | Formato, vet, tests unitarios y límite del oráculo | Salida 0: 12 paquetes Go en `ok`, 38 tests de `oracle/` y 36 de `scripts/` |
| `make build` y `python3 scripts/crit_build.py` | Compilación sin root ni red | Pasa |
| `python3 scripts/crit_oracle_imports.py` | El oráculo no importa, lee ni invoca el motor | Pasa |
| `python3 scripts/crit_windows.py` | Ventanas del motor iguales a las del oráculo, sin fallas | Pasa |
| `python3 scripts/crit_windows_late.py` | Ventanas y clasificación iguales, con fuera de orden y tardíos | Pasa |
| `python3 scripts/crit_recovery.py` | Ventanas iguales al oráculo tras `kill -9` y truncado del log | Pasa |
| `python3 scripts/crit_accounting.py` | Aceptados + descartados = enviados por inquilino, en saturación | Pasa |
| `python3 scripts/crit_fairness.py` | Inquilinos no ruidosos con al menos el 99% de lo que envían | Pasa |
| `python3 scripts/run_criteria.py --dir <d>`, con scripts de prueba en `<d>` | Sale con 1 si un criterio falla, con 0 con pasados y omitidos, y reporta los omitidos | Pasa |

### K2

Veredictos de la verificación de cierre sobre el commit `57e10b5`. Una salida 77 por falta de un binario de referencia cuenta como fallo, no como omitido.

| Comando | Criterio | Resultado |
| --- | --- | --- |
| `make check` | Formato, vet, tests unitarios y límite del oráculo | Salida 0: 16 paquetes Go en `ok`, 75 tests de `oracle/` y 104 de `scripts/` |
| `python3 scripts/crit_quantiles.py` | Error relativo de cada cuantil declarado por cubeta frente al exacto | Pasa: máximo ≤ 0.01 |
| `python3 scripts/crit_quantiles_merged.py` | Lo mismo sobre sketches mezclados entre cubetas, series y shards | Pasa: máximo ≤ 0.01 |
| `python3 scripts/crit_cardinality.py` | Cardinalidad estimada frente al conteo exacto en todas las ventanas | Pasa: máximo ≤ 5% |
| `python3 scripts/crit_transitions.py` | Transiciones del evaluador iguales a las del oráculo, sin fallas y tras `kill -9` del evaluador | Pasa: 0 diferencias en las dos comparaciones |
| `python3 scripts/crit_promtool_check.py` | `promtool check rules` sobre las reglas del subconjunto | Pasa |
| `python3 scripts/crit_vmalert_replay.py` | Transiciones del subconjunto iguales a las de vmalert en replay | Pasa: 0 diferencias |
| `python3 scripts/crit_keep_firing_for.py` | `keep_firing_for` frente a `promtool test rules` y `vmalert-tool unittest` | Pasa: los dos aceptan |
| `python3 scripts/crit_notify_once.py` | Con `kill -9` sembrados de los evaluadores, incidentes perdidos y duplicados efectivos en el receptor | Pasa: 0 perdidos y 0 duplicados efectivos |
| `python3 scripts/crit_alert_latency.py` | p99 de latencia de notificación en la corrida con fallas | Pasa: p99 ≤ 8000 ms |

Los criterios de K1 siguen pasando sobre el mismo commit.
