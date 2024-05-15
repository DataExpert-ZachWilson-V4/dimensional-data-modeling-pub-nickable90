
INSERT INTO actors
/*
  Since each actor can have more than one films in a given year, with this query 
  we are creating just one record for that actor for a given year and putting all the 
  changing dimensions in an Array by using ARRAY_AGG() based on GROUP BY. 
  We are also getting the Average rating of the movies of that year.
  */
WITH
  actor_recent_rating AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(ROW(YEAR, film, votes, rating, film_id)) AS films,
      AVG(rating) AS avg_rating,
      YEAR
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 1914
    GROUP BY
      actor,
      actor_id,
      YEAR
  ),
  /*
  Once we have the Average rating, we are then categorizing them using the case statements 
  while keeping all other columns we created in the previous query
  */
  this_year AS (
    SELECT
      actor,
      actor_id,
      films,
      CASE
        WHEN avg_rating <= 6 THEN 'bad'
        WHEN avg_rating > 6
        AND avg_rating <= 7 THEN 'average'
        WHEN avg_rating > 7
        AND avg_rating <= 8 THEN 'good'
        ELSE 'star'
      END AS quality_class,
      YEAR
    FROM
      actor_recent_rating
  ),
  last_year AS (
    SELECT
      *
    FROM
      nikhilsahni.actors
    WHERE
      current_year = 1913
  )
SELECT
  COALESCE(ty.actor, ly.actor) AS actor,
  COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL
    AND ly.current_year IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL
    AND ly.current_year IS NOT NULL THEN ty.films || ly.films
  END AS films,
  CASE
    WHEN ty.quality_class IS NULL THEN ly.quality_class
    ELSE ty.quality_class
  END AS quality_class,
  CASE
    WHEN ty.year IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year AS ly
  FULL OUTER JOIN this_year AS ty ON ly.actor_id = ty.actor_id
