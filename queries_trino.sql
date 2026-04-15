-- ============================================================
-- queries_trino.sql
-- Proyecto 2 FHBD | Consultas Trino sobre tablas Silver
--
-- Propósito: replicar las tablas Gold consultando Silver desde Trino
-- Motor    : Trino → http://localhost:8090
-- Catalog  : iceberg (apunta a Nessie vía REST catalog)
-- Schema   : silver
--
-- Uso:
--   1. Abrir Trino UI: http://localhost:8090
--   2. Conectar con usuario 'trino' (sin contraseña)
--   3. Ejecutar cada bloque por separado
-- ============================================================


-- ============================================================
-- VERIFICACIÓN INICIAL — tablas Silver disponibles
-- ============================================================

SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.silver;

-- Conteos rápidos
SELECT 'post_hist'   AS tabla, COUNT(*) AS filas FROM iceberg.silver.post_hist
UNION ALL
SELECT 'users_hist'  AS tabla, COUNT(*) AS filas FROM iceberg.silver.users_hist
UNION ALL
SELECT 'votes_hist'  AS tabla, COUNT(*) AS filas FROM iceberg.silver.votes_hist
UNION ALL
SELECT 'badges_hist' AS tabla, COUNT(*) AS filas FROM iceberg.silver.badges_hist;


-- ============================================================
-- RÉPLICA 1: cant_post_x_user_hist
-- Posts por usuario, año y tipo de post
-- Fuente: post_hist + users_hist
-- ============================================================

SELECT
    p.OwnerUserId                               AS user_id,
    COALESCE(u.display_name, 'Unknown')         AS display_name,
    p.anio,
    p.PostTypeId                                AS post_type_id,
    CASE
        WHEN p.PostTypeId = 1 THEN 'Question'
        WHEN p.PostTypeId = 2 THEN 'Answer'
        ELSE 'Other'
    END                                         AS post_type,
    COUNT(p.Id)                                 AS cant_posts,
    COALESCE(SUM(p.Score), 0)                   AS total_score,
    ROUND(AVG(p.Score), 2)                      AS avg_score,
    COALESCE(SUM(p.ViewCount), 0)               AS total_views,
    CURRENT_TIMESTAMP                           AS fecha_cargue
FROM iceberg.silver.post_hist p
LEFT JOIN iceberg.silver.users_hist u
    ON p.OwnerUserId = u.id
WHERE p.OwnerUserId IS NOT NULL
GROUP BY
    p.OwnerUserId,
    u.display_name,
    p.anio,
    p.PostTypeId
ORDER BY cant_posts DESC
LIMIT 100;


-- ============================================================
-- RÉPLICA 2: vote_stats_per_post
-- Estadísticas de votos por post
-- Fuente: post_hist + votes_hist
-- ============================================================

SELECT
    p.Id                                                    AS post_id,
    COALESCE(p.Title, '')                                   AS title,
    p.PostTypeId                                            AS post_type_id,
    p.OwnerUserId                                           AS owner_user_id,
    p.anio,
    COUNT(v.Id)                                             AS total_votes,
    SUM(CASE WHEN v.VoteTypeId = 2 THEN 1 ELSE 0 END)      AS upvotes,
    SUM(CASE WHEN v.VoteTypeId = 3 THEN 1 ELSE 0 END)      AS downvotes,
    SUM(CASE
            WHEN v.VoteTypeId = 2 THEN 1
            WHEN v.VoteTypeId = 3 THEN -1
            ELSE 0
        END)                                                AS net_votes,
    COALESCE(p.Score, 0)                                    AS score,
    COALESCE(p.ViewCount, 0)                                AS view_count,
    CURRENT_TIMESTAMP                                       AS fecha_cargue
FROM iceberg.silver.post_hist p
LEFT JOIN iceberg.silver.votes_hist v
    ON p.Id = v.PostId
GROUP BY
    p.Id, p.Title, p.PostTypeId,
    p.OwnerUserId, p.anio, p.Score, p.ViewCount
ORDER BY total_votes DESC
LIMIT 100;


-- ============================================================
-- RÉPLICA 3: top_tags
-- Ranking de etiquetas más usadas en preguntas
-- Fuente: post_hist
-- ============================================================

SELECT
    tag,
    anio,
    COUNT(*)                        AS cant_preguntas,
    COALESCE(SUM(Score), 0)         AS total_score,
    ROUND(AVG(Score), 2)            AS avg_score,
    COALESCE(SUM(ViewCount), 0)     AS total_views,
    COALESCE(SUM(AnswerCount), 0)   AS total_answers,
    CURRENT_TIMESTAMP               AS fecha_cargue
FROM iceberg.silver.post_hist
CROSS JOIN UNNEST(
    split(regexp_replace(Tags, '<|>', ' '), ' ')
) AS t(tag)
WHERE PostTypeId = 1
  AND Tags IS NOT NULL
  AND Tags <> ''
  AND trim(tag) <> ''
GROUP BY trim(tag), anio
ORDER BY cant_preguntas DESC
LIMIT 50;


-- ============================================================
-- RÉPLICA 4: user_engagement
-- Interacciones totales por usuario
-- Fuente: users_hist + post_hist + votes_hist + badges_hist
-- ============================================================

SELECT
    u.id                                                        AS user_id,
    COALESCE(u.display_name, 'Unknown')                         AS display_name,
    COALESCE(u.reputation, 0)                                   AS reputation,
    COALESCE(u.location, '')                                    AS location,
    COUNT(DISTINCT p.Id)                                        AS total_posts,
    SUM(CASE WHEN p.PostTypeId = 1 THEN 1 ELSE 0 END)          AS total_questions,
    SUM(CASE WHEN p.PostTypeId = 2 THEN 1 ELSE 0 END)          AS total_answers,
    COALESCE(SUM(p.Score), 0)                                   AS total_score,
    COALESCE(SUM(p.ViewCount), 0)                               AS total_views,
    COALESCE(SUM(p.CommentCount), 0)                            AS total_comments,
    COUNT(DISTINCT v.Id)                                        AS total_votes_received,
    COUNT(DISTINCT b.Id)                                        AS total_badges,
    CURRENT_TIMESTAMP                                           AS fecha_cargue
FROM iceberg.silver.users_hist u
LEFT JOIN iceberg.silver.post_hist p
    ON u.id = p.OwnerUserId
LEFT JOIN iceberg.silver.votes_hist v
    ON p.Id = v.PostId
LEFT JOIN iceberg.silver.badges_hist b
    ON u.id = b.UserId
GROUP BY u.id, u.display_name, u.reputation, u.location
ORDER BY total_posts DESC
LIMIT 100;


-- ============================================================
-- RÉPLICA 5: badges_summary
-- Resumen de insignias por usuario
-- Fuente: badges_hist + users_hist
-- ============================================================

SELECT
    b.UserId                                                    AS user_id,
    COALESCE(u.display_name, 'Unknown')                         AS display_name,
    COALESCE(u.reputation, 0)                                   AS reputation,
    COUNT(b.Id)                                                 AS total_badges,
    SUM(CASE WHEN b.Class = 1 THEN 1 ELSE 0 END)               AS gold_badges,
    SUM(CASE WHEN b.Class = 2 THEN 1 ELSE 0 END)               AS silver_badges,
    SUM(CASE WHEN b.Class = 3 THEN 1 ELSE 0 END)               AS bronze_badges,
    SUM(CASE WHEN b.TagBased = TRUE THEN 1 ELSE 0 END)         AS tag_based_badges,
    COUNT(DISTINCT b.Name)                                      AS unique_badge_types,
    CURRENT_TIMESTAMP                                           AS fecha_cargue
FROM iceberg.silver.badges_hist b
LEFT JOIN iceberg.silver.users_hist u
    ON b.UserId = u.id
GROUP BY b.UserId, u.display_name, u.reputation
ORDER BY total_badges DESC
LIMIT 100;


-- ============================================================
-- CONSULTAS DE VALIDACIÓN — comparar Silver vs Gold
-- ============================================================

-- Validar que cant_post_x_user_hist de Trino coincide con Gold de Dremio
SELECT
    COUNT(*)        AS total_registros,
    SUM(cant_posts) AS total_posts,
    MAX(anio)       AS ultimo_anio,
    MIN(anio)       AS primer_anio
FROM (
    SELECT
        p.OwnerUserId   AS user_id,
        p.anio,
        p.PostTypeId    AS post_type_id,
        COUNT(p.Id)     AS cant_posts
    FROM iceberg.silver.post_hist p
    WHERE p.OwnerUserId IS NOT NULL
    GROUP BY p.OwnerUserId, p.anio, p.PostTypeId
) sub;


-- Top 10 usuarios más activos desde Silver
SELECT
    u.display_name,
    COUNT(DISTINCT p.Id)    AS total_posts,
    SUM(p.Score)            AS total_score,
    COUNT(DISTINCT b.Id)    AS total_badges
FROM iceberg.silver.users_hist u
LEFT JOIN iceberg.silver.post_hist p ON u.id = p.OwnerUserId
LEFT JOIN iceberg.silver.badges_hist b ON u.id = b.UserId
GROUP BY u.display_name
ORDER BY total_posts DESC
LIMIT 10;


-- Distribución de posts por año y tipo
SELECT
    anio,
    CASE
        WHEN PostTypeId = 1 THEN 'Question'
        WHEN PostTypeId = 2 THEN 'Answer'
        ELSE 'Other'
    END     AS tipo,
    COUNT(*) AS cantidad
FROM iceberg.silver.post_hist
GROUP BY anio, PostTypeId
ORDER BY anio, PostTypeId;
