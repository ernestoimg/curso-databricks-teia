// Databricks notebook source
// MAGIC %md
// MAGIC ## Se declaran los parametros del notebook
// MAGIC

// COMMAND ----------

dbutils.widgets.text("ProcessMonth","201812","Mes a procesar (yyyymm) :")

val processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Imports

// COMMAND ----------

// MAGIC %run
// MAGIC ./Common_Functions

// COMMAND ----------

// MAGIC %md
// MAGIC ## Get the data from file

// COMMAND ----------

val greenTaxiTripData = spark
  .read
  .option("header",true)
  .option("delimiter","\\t")
  .option("inferSchema",true)
  .csv(s"/mnt/datalake/GreenTaxiTripData_${processMonth}.csv")

// COMMAND ----------

val defaultValueMap = Map(
  "Payment_Type"->5,
  "RateCodeID"->1
)

var greenTaxiTripDF = greenTaxiTripData
      .filter(col("passenger_count")> 0)
      .where(col("trip_distance")> 0.0)
      .na.drop(Seq("PULocationID","DOLocationID"))
      .na.fill(defaultValueMap)
      .dropDuplicates()
      .withColumnRenamed("lpep_pickup_datetime","PickupTime")
      .withColumnRenamed("PUlocationID","PickupLocationId")
      .withColumnRenamed("lpep_dropoff_datetime","DropTime")
      .withColumnRenamed("PUlocationID","PickupLocationId")
      .withColumnRenamed("DOlocationID","DropLocationId")
      .withColumn("TripYear",year($"PickupTime"))
      .withColumn("TripMonth",month($"PickupTime"))
      .withColumn("TripDay",dayofmonth($"PickupTime"))
      .withColumn("TripTimeInMinutes",round(unix_timestamp($"DropTime")- unix_timestamp($"PickupTime"))/60)
      .withColumn("TripType",
        when($"RatecodeID" ===6 ,"SharedTrip").otherwise("SoloTrip")
      )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load Data

// COMMAND ----------

greenTaxiTripDF
  .write
  .mode("overwrite")
  .option("path", "/mnt/datalake/ernesto.martinez/GreenTaxiTripData.parquet")
  .saveAsTable("TaxiServiceWareHouse.GreenTaxiTripData")
