--insert into pb_mlm_vod
-- Shows
WITH
 
mlm AS (
  SELECT
    media_title,
    LOWER(element_at(split(replace(replace(media_title,'&#039;',''),' ',''),'/'),2)) AS mapped_title,
    TRY_CAST(
        REGEXP_EXTRACT(
          element_at(
            split(REPLACE(REPLACE(media_title,'&#039;',''),' ',''),'/'),1),
            '([0-9]+)'
        ) AS INTEGER
      ) AS mapped_id,
    DATE(server_time) AS server_date,
    idvisitor,
    SUM(watched_time) AS watched_time
  FROM analyticsdatabase.pb_matomo_log_media
  WHERE media_title IS NOT NULL 
    AND watched_time < 10800
    AND lower(media_title) NOT LIKE lower('%track %')
    AND TRIM(media_title) <> ''
 
    and server_time >= (current_timestamp - interval '24' hour)
 
  GROUP BY 1,2,3,4,5
),
 
mlm_ep AS (
  SELECT
    media_title,
    mapped_title,
    mapped_id ,
    server_date,
    idvisitor,
    watched_time,
    CAST(REGEXP_EXTRACT(mapped_title, '(?i)s([0-9]+)e([0-9]+)', 1) AS INTEGER) AS mlm_season,
    CAST(REGEXP_EXTRACT(mapped_title, '(?i)s([0-9]+)e([0-9]+)', 2) AS INTEGER) AS mlm_episode
  FROM mlm
  WHERE mapped_title IS NOT NULL
),
 
eps AS (
  SELECT
    show_id,
    episode_id,
    show_title,
    media_title AS episode_title,
    season_number,
    episode_number,
    REPLACE(LOWER(REPLACE(show_title,'''','')),' ','') AS show_slug
  FROM analyticsdatabase.pb_content_snapshot_v2
  WHERE categories_title IN ('Shows','Documentaries' ) 
    AND episode_id IS NOT NULL
),
 
joined AS (
  SELECT
    e.show_id,
    e.episode_id,
    e.show_title,
    e.episode_title,
    e.season_number,
    e.episode_number,
    m.mapped_title,
    m.media_title,
    m.server_date,
    m.idvisitor,
    m.watched_time
  FROM eps e
  LEFT JOIN mlm_ep m
    ON (
      m.mapped_title = e.show_slug
      OR starts_with(m.mapped_title, e.show_slug || '-')
      OR starts_with(m.mapped_title, e.show_slug || ' ')
    ) 
    AND  (m.mapped_id = e.show_id  OR m.mapped_id = e.episode_id)
    and (
      CASE 
        WHEN m.mlm_season IS NOT NULL 
          THEN (m.mlm_season = e.season_number) 
               AND (m.mlm_episode = e.episode_number)
        ELSE 1=1 
      END
    )
),
 
final AS (
  SELECT
    show_id,
    show_title,
    media_title,
    mapped_title,
    episode_id,
    episode_title,
    season_number,
    episode_number,
    server_date,
    idvisitor,
    SUM(watched_time) AS watched_time
  FROM joined
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
 
ats AS (
  SELECT
    show_id, 
    episode_id, 

    idvisitor,
 
    SUM(watched_time)/60.0 AS watched_minut , 
    server_date
  FROM final
  -- WHERE lower(media_title) LIKE lower('%Begum Akhtar%') 
  GROUP BY 1,2,3,5
)
 
SELECT sum(watched_minut) FROM ats
where idvisitor is not null