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

## Estado

| Fase | Contenido | Estado |
| --- | --- | --- |
| K0 | Fundación: contrato, investigación, comando de chequeo | terminada |
| K1 | Ingestión durable, ventanas exactas y oráculo | Cerrada |
| K2 | Sketches, alertas, evaluador replicado y comparación con vmalert y Prometheus | pendiente |
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
