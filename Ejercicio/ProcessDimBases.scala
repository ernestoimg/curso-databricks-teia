// Databricks notebook source
// MAGIC %md
// MAGIC # FhvBases 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Import Libaries 

// COMMAND ----------

// MAGIC %run 
// MAGIC ./Common_Functions

// COMMAND ----------

// MAGIC %md
// MAGIC ## Execute Common functions notebook 

// COMMAND ----------

dbutils.notebook.run("Common_Functions",300,Map())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create Schema 

// COMMAND ----------

val fhvBaseSchema = StructType(
  List(
    StructField("License Number",StringType,true),
    StructField("Entity Name",StringType,true),
    StructField("Telephone Number",LongType,true),
    StructField("SHL Endorsed",StringType,true),
    StructField("Address",StructType(
                  List(
                    StructField("Building",StringType,true),
                    StructField("Street",StringType,true),
                    StructField("City",StringType,true),
                    StructField("State",StringType,true),
                    StructField("Postcode",LongType,true),
                  )
                ),true),
    StructField("GeoLocation",StructType(
                  List(
                    StructField("Latitude",DoubleType,true),
                    StructField("Longitude",DoubleType,true),
                    StructField("Location",StringType,true),
                  )
                ),true),
    StructField("Type of Base",StringType,true),
    StructField("Date",StringType,true),   
    StructField("Time",StringType,true),   
  )
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Extract data from FhvBAse.json

// COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake"))

// COMMAND ----------

var fhvBaseData = spark
.read
.option("multiline",true)
.schema(fhvBaseSchema)
.option("inferSchema",true)
.json("/mnt/datalake/FhvBases.json")

// COMMAND ----------

display(fhvBaseData)

// COMMAND ----------

var fhvBaseDataFormated = fhvBaseData
  .select(
    $"License Number".alias("BaseLicenseNumber"),
    $"Entity Name".alias("EntityName"),
    $"Telephone Number".alias("TelephoneNumber"),
    $"SHL Endorsed".alias("ShlEndorsed"),
    $"Type of Base".alias("BaseType"),
    $"Address.Building".alias("AddressBuilding"),
    $"Address.Street".alias("AddressStreet"),
    $"Address.City".alias("AddressCity"),
    $"Address.State".alias("AddressState"),
    $"Address.Postcode".alias("AddressPostCode"),
    $"GeoLocation.Latitude".alias("GeoLocationLatitude"),
    $"GeoLocation.Longitude".alias("GeoLocationLongitude"),
    $"GeoLocation.Location".alias("GeoLocationLocation")
  )

// COMMAND ----------

display(fhvBaseDataFormated)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load the data in blob storage [DimFHVBases]

// COMMAND ----------

fhvBaseDataFormated
  .write
  .mode("overwrite")
  .option("path", "/mnt/datalake/ernesto.martinez/DimFHVBases.parquet")
  .saveAsTable("TaxiServiceWareHouse.DimFHVBases")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Explotación FHVTaxiTrip  

// COMMAND ----------


