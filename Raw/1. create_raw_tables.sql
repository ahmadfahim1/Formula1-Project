-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## create circuits table

-- COMMAND ----------

create table if not exists f1_raw.circuits(circuitId INT,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string
)
using csv
options(path "/mnt/formula7dl7/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create races table

-- COMMAND ----------

create table if not exists f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name string,
  date DATE,
  time STRING,
  url STRING
)
using csv
options(path "/mnt/formula7dl7/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables for json files

-- COMMAND ----------

create table if not exists f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
using json
options(path "/mnt/formula7dl7/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### drivers table

-- COMMAND ----------

create table if not exists f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,

  code STRING,
  name STRUCT < forename: string, surname: string >,
  dob Date,
  nationality STRING,
  url STRING
)
using json
options(path "/mnt/formula7dl7/raw/drivers.json", header true)

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create results table

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points float,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed float,
  statusId INT

)
using json
options(path "/mnt/formula7dl7/raw/results.json" , header true)

-- COMMAND ----------

select * from  f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create pit stop table
-- MAGIC ##### multi line json

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId INT,
  duration String,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
using json
options(path "/mnt/formula7dl7/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### creating tables from multiple source files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
using csv
options(path "/mnt/formula7dl7/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####json file

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
using json
options (path "/mnt/formula7dl7/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

