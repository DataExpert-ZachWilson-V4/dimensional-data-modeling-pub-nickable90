/*
This table tracks both quality_class and is_active dimensions with the same start_date and end_date
*/
CREATE OR REPLACE TABLE nikhilsahni.actors_history_scd (
  -- actor: Full name of the actor.
  actor VARCHAR,
  -- actor_id: Unique identifier of actor.
  actor_id VARCHAR,
  -- quality_class: Category based on the Average rating for a given start_date and end_date.
  quality_class VARCHAR,
  -- is_active: Whether the actor is active and making the films for a given start_date and end_date.
  is_active BOOLEAN,
  -- start_date: Start date to track the two dimensions.
  start_date INTEGER,
  -- end_date: End date to track the two dimensions.
  end_date INTEGER,
  -- current_year: Represents the most recent year.
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    PARTITIONING = ARRAY['current_year']
  )
