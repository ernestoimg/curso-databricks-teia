// Databricks notebook source
dbutils.widgets.text("ProcessMonth","201812","Mes a procesar (yyyymm) :")

val processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

dbutils.notebook.run("ProcessFHVTaxiTrip",300,Map("ProcessMonth"->processMonth))

// COMMAND ----------

dbutils.notebook.run("ProcessGreenTaxiTrip",300,Map("ProcessMonth"->processMonth))

// COMMAND ----------

dbutils.notebook.run("ProcessYellowTaxiTrip",300,Map("ProcessMonth"->processMonth))

// COMMAND ----------

dbutils.notebook.run("ProcessTaxiTripConsolidated",300,Map("ProcessMonth"->processMonth))
