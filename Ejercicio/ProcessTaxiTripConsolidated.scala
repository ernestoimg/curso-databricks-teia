// Databricks notebook source
// MAGIC %md
// MAGIC ### Get the data in tabla	FhvTaxiTripData 

// COMMAND ----------

val fhvTaxiTripData = spark.sql("select * from TaxiServiceWareHouse.FhvTaxiTripData ") 

// COMMAND ----------

var fhvTaxiTripDF = fhvTaxiTripData.drop("BaseLicenseNumber")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get the data in tabla GreenTaxiTripData 

// COMMAND ----------

val greenTaxiTripData  = spark.sql("select * from TaxiServiceWareHouse.GreenTaxiTripData") 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get the data in tabla YellowTaxiTripData

// COMMAND ----------

val yellowTaxiTripData = spark.sql("select * from TaxiServiceWareHouse.YellowTaxiTripData")

// COMMAND ----------

// MAGIC %md
// MAGIC ## union dataframes

// COMMAND ----------

var dataFrameConsolidated = fhvTaxiTripDF
.join(greenTaxiTripData, Seq("PickupLocationId","DropLocationId","TripYear","TripMonth","TripDay"),"left")
.join(yellowTaxiTripData, Seq("PickupLocationId","DropLocationId","TripYear","TripMonth","TripDay"),"left")
.drop("fare_amount")
.drop("droptime")
.drop("extra")
.drop("improvement_surcharge")
.drop("mta_tax")
.drop("passenger_count")
.drop("payment_type")
.drop("pickuptime")
.drop("ratecodeid")
.drop("store_and_fwd_flag")
.drop("tip_amount")
.drop("total_amount")
.drop("tolls_amount")
.drop("trip_distance")
.drop("triptimeinminutes")
.drop("triptype")
.drop("vendorid")



// COMMAND ----------

// MAGIC %md
// MAGIC ## Load Data

// COMMAND ----------

dataFrameConsolidated
  .write
  .mode("overwrite")
  .option("path", "/mnt/datalake/ernesto.martinez/TaxiTripDataConsolidated.parquet")
  .saveAsTable("TaxiServiceWareHouse.TaxiTripDataConsolidated")
