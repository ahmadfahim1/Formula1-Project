-- Databricks notebook source
create database if not exists f1_processed
LOCATION "/mnt/formula7dl7/processed"

-- COMMAND ----------

desc database f1_processed

-- COMMAND ----------

Select * from f1_processed.circuits;

-- COMMAND ----------

