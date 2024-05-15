INSERT INTO
  actors_history_scd
WITH
  lagged AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      CASE
        WHEN LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY current_year) IS NULL THEN 'not ' || quality_class
        ELSE LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY current_year)
      END AS previous_quality_class,
      CASE
        WHEN is_active THEN TRUE
        ELSE FALSE
      END AS is_active,
      CASE
        WHEN LAG(is_active) OVER (PARTITION BY actor_id ORDER BY current_year) THEN TRUE
        ELSE FALSE
      END AS is_active_last_year,
      current_year
    FROM
      actors
    WHERE
      current_year <= 1920
  ),
  changes AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      previous_quality_class,
      is_active,
      is_active_last_year,
      CASE
        WHEN quality_class <> previous_quality_class THEN TRUE
        ELSE FALSE
      END AS quality_class_change,
      CASE
        WHEN is_active <> is_active_last_year THEN TRUE
        ELSE FALSE
      END AS is_active_change,
      current_year
    FROM
      lagged
  ),
  did_change AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      is_active,
      quality_class_change,
      is_active_change,
      CASE
        WHEN (
          quality_class_change
          OR is_active_change
        ) THEN 1
        ELSE 0
      END AS did_change,
      current_year
    FROM
      changes
  ),
  streaks AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      is_active,
      SUM(did_change) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS streak_identifier,
      current_year
    FROM
      did_change
  )
SELECT
  actor,
  actor_id,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  1920 AS current_year
FROM
  streaks
GROUP BY
  actor,
  actor_id,
  streak_identifier
