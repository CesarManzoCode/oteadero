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
| K0 | Fundación: contrato, investigación, comando de chequeo | cerrada |
| K1 | Ingestión durable, ventanas exactas y oráculo | pendiente |
| K2 | Sketches, alertas, evaluador replicado y comparación con vmalert y Prometheus | pendiente |
| K3 | Streams en vivo, simulación determinista, mutantes y rendimiento | pendiente |

## Resultados

Todavía no hay resultados medidos. Se publicarán aquí al cerrar cada fase, con el comando que los produce.
