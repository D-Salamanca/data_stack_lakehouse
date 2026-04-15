# Project Status — Data Lakehouse StackOverflow

**Fecha:** 2026-04-15
**Estado:** ✅ Pipeline end-to-end funcional con datos reales

---

## Resumen ejecutivo

Arquitectura Medallion (Bronze → Silver → Gold) sobre MinIO + Iceberg + Nessie,
orquestada con Airflow. El último DAG run (`verify_1776283348`) terminó en
verde con las 3 tareas: bronze (1m26s) → silver (1m52s) → gold (45s).

Las tablas Silver y Gold están materializadas en Nessie y son consultables
desde Trino (Silver) y Dremio (Gold). Datos reales del dataset público de
StackOverflow publicado por ClickHouse en S3.

---

## Servicios desplegados

Stack base (`docker compose up -d`):

| Servicio | Puerto | Estado |
|---|---|---|
| Airflow webserver | 8080 | healthy |
| Airflow scheduler | — | healthy (mem_limit 3.5 GB) |
| PostgreSQL | 5432 | healthy |
| Spark master | 7077 / 8081 | healthy |
| Spark worker | 8082 | up (4 GB / 2 cores) |
| MinIO | 9000 / 9001 | healthy |
| Nessie | 19120 | healthy |
| Trino | 8090 | healthy (heap 512 MB) |

Profiles opcionales:

| Servicio | Comando | Estado |
|---|---|---|
| Dremio | `docker compose --profile bi up -d dremio` | verificado funcional |
| Jupyter | `docker compose --profile dev up -d jupyter` | disponible |

---

## Datos cargados

### Bronze (MinIO `s3://bronze/`, formato Parquet, override)

| Path | Filas (cap MAX_ROWS=50k) |
|---|---|
| `posts/2019/posts_2019.parquet` | 50.000 |
| `posts/2020/posts_2020.parquet` | 50.000 |
| `posts/2021/posts_2021.parquet` | 50.000 |
| `users/2019/users_2019.parquet` | ~17.000 |
| `users/2020/users_2020.parquet` | ~17.000 |
| `users/2021/users_2021.parquet` | ~17.000 (cargado por DAG vía DLT) |
| `votes/2019/votes_2019.parquet` | 50.000 |
| `votes/2020/votes_2020.parquet` | 50.000 |
| `votes/2021/votes_2021.parquet` | 50.000 |
| `badges/all/badges.parquet` | 60.000 |

**Origen real:** `https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/`
(streaming HTTP range-reads vía `pyarrow.iter_batches`, no se descarga el dataset completo).

### Silver (Iceberg + Nessie, MERGE upsert, con `fecha_cargue`)

| Tabla | Filas | Años cubiertos | Duplicados |
|---|---|---|---|
| `iceberg.silver.users_hist` | 50.000 | 2019, 2020, 2021 | 0 |
| `iceberg.silver.post_hist` | 60.000 | 2019, 2020, 2021 | 0 |
| `iceberg.silver.votes_hist` | 60.000 | 2019, 2020, 2021 | 0 |
| `iceberg.silver.badges_hist` | 60.000 | — | 0 |

### Gold (Iceberg + Nessie, MERGE upsert, con `fecha_cargue`)

| Tabla | Filas | Notas |
|---|---|---|
| `iceberg.gold.cant_post_x_user_hist` | 47.128 | KPIs por user_id × año |

---

## DAG `stackoverflow_pipeline`

```
bronze_ingest (DLT)  →  silver_transform (Spark+Iceberg)  →  gold_agg (Spark+Iceberg)
```

- Un solo Play. Trigger desde la UI o `airflow dags trigger stackoverflow_pipeline`.
- Silver corre en cluster Spark (worker con 4 GB).
- Gold corre en `local[1]` desde el scheduler.
- Ambos usan `spark.sql.iceberg.vectorization.enabled=false` para evitar el
  SIGSEGV (rc=134) del Arrow vectorized reader sobre Java 17.

---

## Optimizaciones aplicadas vs versión original

| Cambio | Ahorro |
|---|---|
| Airflow CeleryExecutor → LocalExecutor (sin redis, worker, triggerer, flower) | ~2 GB |
| Dremio movido a `--profile bi` | ~5 GB cuando no se usa |
| Jupyter movido a `--profile dev` | ~1 GB cuando no se usa |
| Trino heap 1 GB → 512 MB | ~500 MB |
| Airflow webserver con `mem_limit: 1g` | techo (no ahorro neto) |
| Airflow scheduler con `mem_limit: 3.5g` | techo (suficiente para el driver Spark) |

**Total:** ~6.5 GB menos en el stack base sin tocar el pipeline.

---

## Verificación rápida (smoke tests)

```bash
# Stack arriba
docker compose ps

# Tablas registradas en Nessie
curl -s http://localhost:19120/api/v2/trees/main/entries | python -m json.tool

# Conteos en Silver/Gold vía Trino
docker exec proyecto2-trino trino --server http://localhost:8080 --execute \
  "SELECT COUNT(*) FROM iceberg.silver.users_hist"

# Disparar DAG completo
docker exec proyecto2-airflow-webserver \
  airflow dags trigger stackoverflow_pipeline --run-id "manual_$(date +%s)"
```

---

## Credenciales

| Servicio | Usuario | Password |
|---|---|---|
| Airflow UI | airflow | airflow |
| MinIO | minioadmin | minioadmin123 |
| Dremio | admin | Admin1234! |
| Trino | trino | (sin contraseña) |
| Jupyter | — | token: `fhbd` |

---

## Issues conocidos y workarounds documentados

1. **Iceberg vectorization SIGSEGV** — silver y gold deshabilitan
   `spark.sql.iceberg.vectorization.enabled` por bug de Arrow + Java 17 en
   Iceberg 1.6.1 al hacer MERGE.
2. **DLT schema state** — `bronze_ingest.py` hace `rmtree /tmp/dlt_pipelines/`
   antes de cada run para evitar coerciones binary↔bigint heredadas entre runs.
3. **`play.clickhouse.com/stackoverflow` ya no existe** — el dataset se lee
   directamente de los Parquets públicos de S3.

---

## Diagrama

`docs/architecture.svg` (fuente: `docs/architecture.mmd`).
