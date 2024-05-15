INSERT INTO 
 actors_history_scd
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1920
  ),
  this_year_scd AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1921
  ),
  combined AS (
    SELECT
      COALESCE(ls.actor, ts.actor) AS actor,
      COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
      COALESCE(ls.start_date, ts.current_year) AS start_date,
      COALESCE(ls.end_date, ts.current_year) AS end_date,
      CASE
        WHEN ls.is_active <> ts.is_active THEN 1
        WHEN ls.is_active = ts.is_active THEN 0
      END AS is_active_change,
      ls.is_active AS is_active_last_year,
      ts.is_active AS is_active_this_year,
      CASE
        WHEN ls.quality_class <> ts.quality_class THEN 1
        WHEN ls.quality_class = ts.quality_class THEN 0
      END AS quality_class_change,
      ls.quality_class AS quality_class_last_year,
      ts.quality_class AS quality_class_this_year,
      1921 AS current_year
    FROM
      last_year_scd AS ls
      FULL OUTER JOIN this_year_scd AS ts ON ls.actor_id = ts.actor_id
      AND ls.end_date + 1 = ts.current_year
  ),
  both_changes AS (
    SELECT
      *,
      CASE
        WHEN is_active_change = 0
        AND quality_class_change = 0 THEN 0
        WHEN is_active_change IS NULL
        OR quality_class_change IS NULL THEN NULL
        ELSE 1
      END AS both_change
    FROM
      combined
  ),
  changes AS (
    SELECT
      actor,
      actor_id,
      CASE
        WHEN both_change = 0 THEN ARRAY[
          CAST(
            ROW(
              CAST(quality_class_last_year AS VARCHAR),
              CAST(is_active_last_year AS VARCHAR),
              CAST(start_date AS VARCHAR),
              CAST(end_date + 1 AS VARCHAR)
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN both_change = 1 THEN ARRAY[
          CAST(
            ROW(
              CAST(quality_class_last_year AS VARCHAR),
              CAST(is_active_last_year AS VARCHAR),
              CAST(start_date AS VARCHAR),
              CAST(end_date AS VARCHAR)
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW(
              CAST(quality_class_this_year AS VARCHAR),
              CAST(is_active_this_year AS VARCHAR),
              CAST(current_year AS VARCHAR),
              CAST(current_year AS VARCHAR)
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        WHEN both_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              CAST(
                COALESCE(quality_class_last_year, quality_class_this_year) AS VARCHAR
              ),
              CAST(
                COALESCE(is_active_last_year, is_active_this_year) AS VARCHAR
              ),
              CAST(start_date AS VARCHAR),
              CAST(end_date AS VARCHAR)
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
      END AS change_array,
      current_year
    FROM
      both_changes
  )
SELECT
  c.actor,
  c.actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  c.current_year
FROM
  changes AS c
  CROSS JOIN UNNEST (change_array) AS arr
