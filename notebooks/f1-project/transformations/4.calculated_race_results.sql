-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT
  races.race_year,
  constructors.name as team_name,
  drivers.name as driver_name,
  results.position,
  results.points,
  11 - results.position as calculated_points --(logic added) because the points differed from year to year, so the total points for 1990 would differ the total points from 2018,
FROM f1_processed.results
  join f1_processed.drivers on results.driver_id = drivers.driver_id
  join f1_processed.constructors on results.constructor_id = constructors.constructor_id
  join f1_processed.races on results.race_id = races.race_id
where results.position <= 10 -- we are interested only the top 10 

-- COMMAND ----------

select * from  f1_presentation.calculated_race_results