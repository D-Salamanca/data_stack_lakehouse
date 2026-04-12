# Proyecto 2 – FHBD | Arquitectura Lakehouse con StackOverflow

Pipeline de datos completo implementando la arquitectura **Medallion (Bronze → Silver → Gold)**
usando MinIO, Apache Iceberg, Apache Spark, Nessie, Airflow, DLT, Trino y Dremio.

---

## Arquitectura

```
ClickHouse público             MinIO (S3)                    Nessie (catalog)
stackoverflow dataset  ──DLT──►  bronze/                 ──► iceberg.silver.*
                               ├── posts/2019,2020,2021       ├── post_hist
                               ├── users/2019,2020,2021       ├── users_hist
                               ├── votes/2019,2020,2021       ├── votes_hist
                               └── badges/all/                └── badges_hist
                                                          ──► iceberg.gold.*
                                                              ├── cant_post_x_user_hist ★
                                                              ├── vote_stats_per_post
                                                              ├── top_tags
                                                              ├── user_engagement
                                                              └── badges_summary

               Trino ──► consultar Silver    Dremio ──► consultar Gold
```

### DAG Airflow — un solo Play ▷

```
bronze_ingest (DLT) ──► silver_transform (Spark+Iceberg) ──► gold_agg (Spark+Iceberg)
```

★ Solo estos pasos van por el pipeline. El resto se carga manualmente antes de ejecutar.

---

## Estructura del repositorio

```
proyecto2-lakehouse/
├── dags/
│   └── stackoverflow_pipeline.py     # DAG con 3 tareas
├── scripts/
│   ├── bronze_ingest.py              # Task 1 — DLT users_2021 ★
│   ├── bronze_manual_load.py         # Carga manual Bronze previa
│   ├── silver_transform.py           # Task 2 — users_hist ★
│   ├── silver_post_hist_manual.py    # Script manual post_hist
│   ├── silver_votes_badges_manual.py # Script manual votes_hist + badges_hist
│   └── gold_agg.py                   # Task 3 — cant_post_x_user_hist ★
├── notebooks/
│   ├── bronze_ingest.ipynb           # Respaldo manual Task 1
│   ├── silver_transform.ipynb        # Respaldo manual Task 2
│   └── gold_agg.ipynb                # Respaldo manual Task 3 + tablas extra
├── trino/
│   └── catalog/
│       └── iceberg.properties        # Configuración catálogo Trino→Nessie
├── queries_trino.sql                 # Consultas Silver que replican Gold
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.spark
├── spark-defaults.conf
├── requirements.txt
├── .env
└── README.md
```

---

## Servicios y puertos

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| MinIO S3 API | http://localhost:9000 | — |
| Nessie API | http://localhost:19120 | — |
| Trino UI | http://localhost:8090 | trino (sin contraseña) |
| Dremio UI | http://localhost:9047 | admin / (se configura al primer inicio) |
| Jupyter Notebook | http://localhost:8888 | token: fhbd |

---

## Despliegue paso a paso

### 1. Clonar el repositorio y preparar carpetas

```bash
git clone <url-del-repo>
cd proyecto2-lakehouse
mkdir -p dags scripts notebooks logs plugins data config trino/catalog
```

### 2. Configurar UID de Airflow (Linux/Mac)

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Construir imágenes y levantar servicios

```bash
docker compose build --no-cache
docker compose up -d
```

> ⏳ Esperar ~3 minutos a que `airflow-init` termine antes de abrir la UI.

### 4. Verificar que todos los servicios están corriendo

```bash
docker compose ps
```

Todos deben mostrar estado `running` o `healthy`.

---

## ⚠️ Configuración manual obligatoria

### Conexión Spark en Airflow

> Se pierde con `docker compose down -v`. Debe rehacerse cada vez.

Ir a **Admin → Connections → (+)**:

| Campo | Valor |
|---|---|
| **Conn Id** | `spark_default` |
| **Conn Type** | `Spark` |
| **Host** | `spark://spark-master` |
| **Port** | `7077` |

### Configurar Dremio (primer inicio)

1. Abrir http://localhost:9047
2. Crear usuario administrador (ej: `admin` / `Admin1234!`)
3. Agregar fuente **Nessie**:
   - **Name:** `nessie`
   - **Nessie Endpoint URL:** `http://proyecto2-nessie:19120/api/v2`
   - **Authentication:** None
4. Agregar fuente **S3 (MinIO)**:
   - **Name:** `minio`
   - **Access Key:** `minioadmin`
   - **Secret Key:** `minioadmin123`
   - **Endpoint:** `http://proyecto2-minio:9000`
   - **Path Style Access:** ✅

---

## Ejecución del pipeline

### Paso 1 — Carga manual Bronze (ejecutar una sola vez)

```bash
# Desde el contenedor de Airflow o directamente si tienes las dependencias locales
docker exec -it proyecto2-airflow-worker bash
cd /opt/airflow
python scripts/bronze_manual_load.py
```

Esto carga en MinIO:
```
bronze/posts/2019/posts_2019.parquet
bronze/posts/2020/posts_2020.parquet
bronze/posts/2021/posts_2021.parquet
bronze/users/2019/users_2019.parquet
bronze/users/2020/users_2020.parquet
bronze/votes/2019/votes_2019.parquet
bronze/votes/2020/votes_2020.parquet
bronze/votes/2021/votes_2021.parquet
bronze/badges/all/badges.parquet
```

### Paso 2 — Construir Silver post_hist (script manual)

```bash
docker exec -it proyecto2-spark-master bash
spark-submit /opt/spark/scripts/silver_post_hist_manual.py
```

### Paso 3 — Construir Silver votes_hist + badges_hist (script manual)

```bash
docker exec -it proyecto2-spark-master bash
spark-submit /opt/spark/scripts/silver_votes_badges_manual.py
```

### Paso 4 — Ejecutar el DAG en Airflow ▷

1. Abrir http://localhost:8080
2. Buscar el DAG `stackoverflow_pipeline`
3. Activar el toggle (pausa → activo)
4. Clic en **Trigger DAG ▷**
5. Monitorear en **Graph View**

El DAG ejecuta en secuencia:
```
bronze_ingest → silver_transform → gold_agg
```

---

## Consultar los datos

### Trino — tablas Silver

Abrir http://localhost:8090 con usuario `trino` (sin contraseña).

```sql
-- Ver tablas disponibles
SHOW TABLES FROM iceberg.silver;

-- Contar registros
SELECT COUNT(*) FROM iceberg.silver.users_hist;
SELECT COUNT(*) FROM iceberg.silver.post_hist;

-- Replicar tabla Gold desde Silver
SELECT
    p.OwnerUserId       AS user_id,
    u.DisplayName       AS display_name,
    p.anio,
    COUNT(p.Id)         AS cant_posts
FROM iceberg.silver.post_hist p
LEFT JOIN iceberg.silver.users_hist u ON p.OwnerUserId = u.Id
WHERE p.OwnerUserId IS NOT NULL
GROUP BY p.OwnerUserId, u.DisplayName, p.anio
ORDER BY cant_posts DESC
LIMIT 20;
```

> Ver archivo `queries_trino.sql` para las 5 réplicas completas de tablas Gold.

### Dremio — tablas Gold

Abrir http://localhost:9047 → navegar a **nessie → gold**:

```sql
-- Ver tablas Gold disponibles
SELECT * FROM nessie.gold.cant_post_x_user_hist LIMIT 20;
SELECT * FROM nessie.gold.vote_stats_per_post ORDER BY total_votes DESC LIMIT 20;
SELECT * FROM nessie.gold.top_tags ORDER BY cant_preguntas DESC LIMIT 20;
SELECT * FROM nessie.gold.user_engagement ORDER BY total_posts DESC LIMIT 20;
SELECT * FROM nessie.gold.badges_summary ORDER BY total_badges DESC LIMIT 20;
```

---

## Ejecución manual (respaldo si Airflow falla)

Si algún task del DAG falla, ejecutar el notebook correspondiente en Jupyter
(http://localhost:8888, token: `fhbd`):

| Task fallido | Notebook de respaldo |
|---|---|
| `bronze_ingest` | `notebooks/bronze_ingest.ipynb` |
| `silver_transform` | `notebooks/silver_transform.ipynb` |
| `gold_agg` | `notebooks/gold_agg.ipynb` |

---

## Reiniciar desde cero

```bash
# Borra todos los contenedores, volúmenes y datos
docker compose down -v

# Levantar de nuevo
docker compose up -d
```

> ⚠️ Después de `down -v` hay que repetir los pasos de configuración manual
> (conexión Spark en Airflow, configuración de Dremio y toda la carga Bronze).

---

## Stack tecnológico

| Tecnología | Versión | Rol |
|---|---|---|
| Apache Airflow | 2.10.3 | Orquestación del pipeline |
| dlt (Data Load Tool) | 0.5.4 | Ingesta Bronze (Task 1) |
| Apache Spark | 3.5.2 | Procesamiento Silver y Gold |
| Apache Iceberg | 1.6.1 | Formato de tabla open table format |
| Project Nessie | latest | Catálogo Iceberg con versionado Git-like |
| MinIO | latest | Object storage S3-compatible (Data Lake) |
| Trino | 438 | Motor SQL para consultar Silver |
| Dremio OSS | latest | Motor SQL para consultar Gold |
| PostgreSQL | 13 | Metadata DB de Airflow |
| Redis | 7.2 | Broker Celery para Airflow |
| Jupyter | base-notebook | Notebooks de respaldo |
