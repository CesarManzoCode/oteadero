# Investigación de oteadero

Fuentes primarias con su revisión fijada y lo medido en la máquina de César. Cada línea marca si es un hecho observado (`[medido]` en esta máquina, `[fuente]` en documentación primaria) o una hipótesis (`[hipótesis]`). Las decisiones que se apoyan en esto están en `vault/CONTRACT.md`.

Fecha de verificación: 2026-11-05.

## Máquina

- [medido] AMD Ryzen 5 5600G, 6 núcleos y 12 CPUs lógicas, x86-64; CPUs hermanas: 0/6, 1/7, 2/8, 3/9, 4/10, 5/11 (`lscpu -e`). 14 GiB visibles de 16 GB, Arch Linux, kernel 7.2.14.
- [medido] `go version go1.27.3 linux/amd64`, `Python 3.14.9`, `GNU Make 4.4.1`, `git 2.56.0`, `taskset` de util-linux 2.42.4. `cargo 1.98.1` está instalado y no se usa.
- [medido] No hay `promtool`, `vmalert` ni `victoria-metrics` en `PATH`, y `pacman -Q` no tiene instalados `prometheus`, `victoriametrics` ni `vmutils`.
- [medido] Con un módulo Go sin paquetes, `go vet ./...` y `go test ./...` salen con 1 ("matched no packages") y `go build ./...` con 0; `python3 -m unittest discover` sin tests no sirve como chequeo vacío. Por eso `make check` corre cada paso solo si hay código.
- [medido] La biblioteca estándar de Python 3.14 no trae CRC-32C (`binascii` no lo tiene); `zlib.crc32` es CRC-32 IEEE, igual que `hash/crc32.ChecksumIEEE` de Go. De ahí el CRC del log.
- [medido] `go doc` confirma `math/rand/v2.NewPCG(seed1, seed2 uint64)`, `hash/fnv.New64a`, `hash/crc32` y `runtime/debug.SetMemoryLimit` en Go 1.27.3.

## Binarios de referencia

Descargados de las releases oficiales de GitHub, verificados contra los checksums publicados y ejecutados sin root desde una carpeta de usuario.

| Archivo | SHA-256 | Versión que reporta |
| --- | --- | --- |
| `prometheus-3.16.0.linux-amd64.tar.gz` | `68727009472a2c02e1ff7f062fd5b3e637285bffc8fc2d340c76fb449d6e0f34` | `promtool, version 3.16.0 (revision: e2e28adeee2c5883d000fc82aca6008cfbf7e96d)` |
| `victoria-metrics-linux-amd64-v1.155.0.tar.gz` | `7180e65913d55ab99c0be8f4a3ad17a0fafe5598a21ff6327fecba3190d7b176` | `victoria-metrics-20261023-161400-tags-v1.155.0-0-g3fec2f7241` |
| `vmutils-linux-amd64-v1.155.0.tar.gz` | `b6181fc16cf19835706ea5d67215eea56c1fe87350fdfd82a5ff803ea2c1a907` | `vmalert-20261023-161309-tags-v1.155.0-0-g3fec2f7241`, `vmalert-tool-20261023-161309-tags-v1.155.0-0-g3fec2f7241` |

URLs: `https://github.com/prometheus/prometheus/releases/download/v3.16.0/prometheus-3.16.0.linux-amd64.tar.gz`, `https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v1.155.0/victoria-metrics-linux-amd64-v1.155.0.tar.gz`, `https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v1.155.0/vmutils-linux-amd64-v1.155.0.tar.gz`. Dentro de los tar: `prometheus-3.16.0.linux-amd64/promtool`, `victoria-metrics-prod`, `vmalert-prod`, `vmalert-tool-prod`.

- [medido] Prometheus 3.16.0 se publicó el 2026-11-05 y VictoriaMetrics v1.155.0 el 2026-10-26 (API de releases de GitHub); son las últimas estables.
- [medido] Banderas confirmadas con `--help` en esas versiones:
  - `promtool check rules [<rule-files>...]`; `promtool test rules <test-rule-file>...`.
  - `vmalert-tool unittest --files=<archivo>` y `--disableAlertgroupLabel`; sin esta bandera exige `groupname` en cada caso.
  - `vmalert-prod`: `-rule`, `-datasource.url`, `-remoteWrite.url`, `-replay.timeFrom`, `-replay.timeTo` (RFC3339), `-replay.disableProgressBar`, `-replay.rulesDelay` (1s por omisión), `-replay.maxDatapointsPerQuery` (1000), `-rule.evalDelay` (30s por omisión), `-notifier.blackhole`, `-evaluationInterval` (1m).
  - `victoria-metrics-prod`: `-storageDataPath`, `-httpListenAddr`, `-retentionPeriod` (mínimo 24h), `-search.disableCache`, `-search.latencyOffset` (30s por omisión), `-inmemoryDataFlushInterval` (5s por omisión; los datos en memoria no sobreviven `kill -9` antes de ese plazo).
- [medido] `POST /api/v1/import/prometheus` con líneas `nombre{etiquetas} valor timestamp_ms` responde 204 y los datos quedan en sus timestamps; `POST /internal/force_flush` los hace visibles enseguida.
- [medido] En VictoriaMetrics la ventana de `count_over_time(x[60s])` en T es `(T−60s, T]`: con muestras en cada segundo exacto, `tfirst_over_time` en T = t0+240 s devolvió t0+181 s y `tlast_over_time` t0+240 s.
- [medido] vmalert en replay (`-rule.evalDelay=0s`, intervalo de grupo 10s, `for: 20s`) evalúa en `timeFrom + k·intervalo`: con `timeFrom` alineado dio `pending` en t0+150 s y t0+160 s y `firing` desde t0+170 s; con `timeFrom` desplazado 3 s, todos los instantes se desplazaron 3 s. Escribe las series `ALERTS{alertstate}` y `ALERTS_FOR_STATE` vía `-remoteWrite.url`.
- [medido] `keep_firing_for`: regla `sum_over_time(m[30s]) > 10`, `for: 20s`, `keep_firing_for: 30s`, intervalo 10s, valores `0 0 5 5 5 5 5 0 ...`. La condición se cumple en 40, 50 y 60 s; dispara de 60 a 90 s y no en 100 s. Es decir, sigue disparando mientras `T − primer T falso < keep_firing_for`. `promtool test rules` y `vmalert-tool unittest` dieron exactamente lo mismo. `promtool check rules` salió con 0 y "SUCCESS: 1 rules found".

## Fuentes primarias

| Tema | Fuente | Revisión |
| --- | --- | --- |
| Reglas de alerta: `for`, `keep_firing_for`, `pending` | https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/ | docs de Prometheus 3.16, consultadas 2026-11-05 |
| Formato de `promtool test rules` (`rule_files`, `evaluation_interval`, `input_series`, `alert_rule_test`, `exp_alerts`; `_` falta, `stale`) | https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/ | idem |
| vmalert: replay (`keep_firing_for has no effect during replay`, grupos en secuencia, `nocache=1`), `eval_offset`, `eval_alignment` | https://docs.victoriametrics.com/victoriametrics/vmalert/ | docs de v1.155.0, consultadas 2026-11-05 |
| vmalert-tool unittest | https://docs.victoriametrics.com/victoriametrics/vmalert-tool/ | idem |
| Importación en formato de exposición (timestamps en ms), capacidad (un nodo con 1.5+ M muestras/s en producción) | https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/ | idem |
| MetricsQL: `rate` e `increase` no extrapolan y usan la muestra anterior a la ventana | https://docs.victoriametrics.com/victoriametrics/metricsql/ | idem |
| DDSketch: α-accurate `|x̃q − xq| ≤ α·xq`, γ = (1+α)/(1−α), cubeta `⌈logγ x⌉`, estimador `2γ^i/(γ+1)`, cuantil inferior de rango `⌊1 + q(n−1)⌋`, mezcla exacta | https://www.vldb.org/pvldb/vol12/p2195-masson.pdf | PVLDB 12(12), 2019 |
| HyperLogLog: error estándar ≈ 1.04/√m, αm = 0.7213/(1 + 1.079/m) para m ≥ 128, linear counting si E ≤ 5m/2 | https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf | Flajolet, Fusy, Gandouet, Meunier, AofA 2007 |
| Watermarks y allowed lateness (definición, tardíos descartados por omisión) | https://ci.apache.org/projects/flink/flink-docs-release-1.12/learn-flink/streaming_analytics.html | Flink 1.12 |
| Simulación determinista (semilla + commit reproducen la falla) | https://docs.tigerbeetle.com/about/vopr | consultada por la investigación adjunta |
| Vacante | https://railway.com/careers/platform-engineer-instrumentation | abierta el 2026-11-05 |
| Capacidad de VictoriaMetrics, ~300 000 muestras/s por núcleo | https://forge.k3s.fr/github-mirrors/VictoriaMetrics/commit/c55a64ba08387a99a39c4003c12406cf5b36ff32 | commit c55a64ba |
| Acciones del CI | https://github.com/actions/checkout (v7.0.1), https://github.com/actions/setup-go (v7.0.0, `go-version-file`), https://github.com/actions/setup-python (v7.1.0, `python-version`) | últimas releases el 2026-11-05; Go 1.27.3 y Python 3.14.10 están en sus manifiestos |

## Nombre

- [medido] Búsqueda del 2026-11-05: ningún resultado web de software de observabilidad llamado oteadero; PyPI, crates.io, npm y el proxy de Go responden 404; en GitHub el único repositorio es `CesarManzoCode/oteadero`, creado ese día, público y vacío.

## Hipótesis que solo se cierran midiendo

- [hipótesis] 1 000 000 de eventos/s con fsync en 8 CPUs lógicas: VictoriaMetrics declara unos 300 000/s por núcleo con historial; lo confirma o refuta `crit_bench.py` en K3.
- [hipótesis] p99 de notificación ≤ 8000 ms con `kill -9` del evaluador: lo mide `crit_alert_latency.py` en K2.
- [hipótesis] El generador no es el cuello de botella en 4 CPUs lógicas: `crit_bench.py` imprime su tasa contra un sumidero nulo antes de medir.

## Huecos que quedan abiertos

- Si Railway usa VictoriaMetrics internamente y cómo es su motor de alertas: no cambia el diseño.
- No se midió la alineación de vmalert en modo en vivo, solo en replay; el proyecto solo usa replay.
