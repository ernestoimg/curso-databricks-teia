// Databricks notebook source
// MAGIC %md
// MAGIC ## Create DataBase

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS TaxiServiceWareHouse 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Import libraries 

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
