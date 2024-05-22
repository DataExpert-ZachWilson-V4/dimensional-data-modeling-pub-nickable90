/*
This query is performing a historical analysis of actors' data from the nikhilsahni.actors table and 
inserting the results into the nikhilsahni.actors_history_scd table. It uses several steps to track changes 
in the actors' quality_class and is_active status over time and to identify periods (streaks) where these 
attributes remained the same.
*/

INSERT INTO
  nikhilsahni.actors_history_scd
/*
The lagged CTE retrieves data from nikhilsahni.actors and calculates the previous_quality_class and 
is_active_last_year for each actor using the LAG window function. It also ensures that is_active is always 
a boolean (TRUE or FALSE)
*/
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
      nikhilsahni.actors
    WHERE
      current_year <= 1920
  ),
  /*
  The changes CTE determines if there was a change in quality_class or is_active status compared to 
  the previous year.
  It creates boolean flags (quality_class_change and is_active_change) to indicate whether a change occurred.
  */
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
  /*
  The did_change CTE creates a did_change flag that indicates whether there was any c
  hange (either in quality_class or is_active) for each year.
  */
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
/*
The streaks CTE calculates a streak_identifier for each period where the quality_class and is_active status 
remain the same. It uses a cumulative sum of the did_change flag to assign a unique identifier to each streak 
of unchanged values.
*/
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
/*
The final SELECT statement groups the data by actor and streak_identifier to summarize each streak.
It inserts a row for each streak into nikhilsahni.actors_history_scd with the actor, quality_class, 
is_active, start date (MIN(current_year)), end date (MAX(current_year)), and current_year.
*/
SELECT
  actor,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  1920 AS current_year
FROM
  streaks
GROUP BY
  actor,
  streak_identifier
