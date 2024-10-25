-- Databricks notebook source
Drop database if exists f1_processed cascade;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula7dl7/processed";

-- COMMAND ----------

drop database if exists f1_presentation cascade;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/formula7dl7/presentation';

-- COMMAND ----------

