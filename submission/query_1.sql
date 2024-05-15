/*
This is to create a cumulative design table, where we maintain the history of changing dimensions in a single column 
as an array of arrays(or rows) in a single record. So that the latest partition or record can be used to pull the entire history.
"films" in this table is an array that maintains rows of all the changing dimensions from different years.
*/
CREATE OR REPLACE TABLE actors (
  -- actor: Actor's full name.
  actor VARCHAR,
  -- actor_id: Unique identifier of an actor.
  actor_id VARCHAR,
  -- films: An array of rows of multiple films associated with the actor. Each row represents details of one film.
  films ARRAY(
    ROW(
  -- year: Release year of the film.
      year INTEGER,
  -- film: Name of the film.
      film VARCHAR,
  -- votes: Number of votes received by the film.
      votes INTEGER,
  -- rating: Rating of the film.
      rating DOUBLE,
  -- film_id: Unique identifier of the film.
      film_id VARCHAR
    )
  ),
  -- quality_class: Category based on the Average rating of all the films in the most recent year.
  quality_class VARCHAR,
  -- is_active: Whether the actor is active and making the films in the given year
  is_active BOOLEAN,
  -- current_year: Represent the year this row is relevant to the actor
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    PARTITIONING = ARRAY['current_year']
  )
