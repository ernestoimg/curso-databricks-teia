// Databricks notebook source
// MAGIC %md
// MAGIC ## Se declaran los parametros del notebook

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
// MAGIC ## Create a schema to file 

// COMMAND ----------

var fhvTaxiTripSchema = StructType(
  List(
    StructField("Pickup_DateTime",TimestampType,true),
  StructField("DropOff_datetime",TimestampType,true),
  StructField("PUlocationID",IntegerType,true),
  StructField("DOlocationID",IntegerType,true),
  StructField("SR_Flag",IntegerType,true),
  StructField("Dispatching_base_number",StringType,true),
  StructField("Dispatching_base_num",StringType,true)
  )
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Get the data in file 

// COMMAND ----------

val fhvTaxiTripData = spark
  .read
  .option("header",true)
  .schema(fhvTaxiTripSchema)
  .csv(s"/mnt/datalake/FHVTaxiTripData_${processMonth}_*.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Delete and drop rows 

// COMMAND ----------

var fhvTaxiTripDataFormatted = fhvTaxiTripData
  .na
  .drop(Seq("PULocationID","DOLocationID"))
  .dropDuplicates()
  .withColumnRenamed("Pickup_DateTime","PickupTime")
  .withColumnRenamed("DropOff_DateTime","DropTime")
  .withColumnRenamed("PUlocationID","PickupLocationId")
  .withColumnRenamed("DOlocationID","DropLocationId")
  .withColumnRenamed("Dispatching_base_number","BaseLicenseNumber")
  .withColumn("TripYear",year($"PickupTime"))
  .withColumn("TripMonth",month($"PickupTime"))
  .withColumn("TripDay",dayofmonth($"PickupTime"))
  .withColumn("TripTimeInMinutes",round(unix_timestamp($"DropTime")- unix_timestamp($"PickupTime"))/60)
  .withColumn("TripType",
        when($"SR_Flag" ===1 ,"SharedTrip").otherwise("SoloTrip")
  )


// COMMAND ----------

// MAGIC %md
// MAGIC ## Load data

// COMMAND ----------

fhvTaxiTripDataFormatted
  .write
  .mode("overwrite")
  .option("path", "/mnt/datalake/ernesto.martinez/FhvTaxiTripData.parquet")
  .saveAsTable("TaxiServiceWareHouse.FhvTaxiTripData")
