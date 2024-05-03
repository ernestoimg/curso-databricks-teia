// Databricks notebook source
// MAGIC %md
// MAGIC ## Imports

// COMMAND ----------

dbutils.notebook.run("Functions",300, Map())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Widgets

// COMMAND ----------

dbutils.widgets.help

// COMMAND ----------

dbutils.widgets.text("ProcessMonth","201812","Mes a procesar (yyyymm) :")

val processMonth = dbutils.widgets.get("ProcessMonth")

// COMMAND ----------

val yellowTaxiParamDf = spark
  .read
  .option("header",true)
  .option("inferSchema",true)
  .csv(s"/mnt/datalake/YellowTaxiTripData_$processMonth.csv")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Extracción de datos desde mount/

// COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake"))

// COMMAND ----------

dbutils.fs.head("dbfs:/mnt/datalake/YellowTaxiTripData_201812.csv")

// COMMAND ----------

// MAGIC %fs
// MAGIC head 'dbfs:/mnt/datalake/YellowTaxiTripData_201812.csv'
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Leer un archivo y guardarlo en DF 

// COMMAND ----------

val yellowTaxiDF = spark
  .read
  .option("header",true)
  .csv("/mnt/datalake/YellowTaxiTripData_201812.csv")

  display(yellowTaxiDF)

// COMMAND ----------

val greenTaxiDf = spark
  .read
  .option("header",true)
  .option("delimiter","\\t")
  .csv("/mnt/datalake/GreenTaxiTripData_201812.csv")

  display(greenTaxiDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Leer datos del archivo FHVTaxiTripData_201812_01

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls "/mnt/datalake/"

// COMMAND ----------

// MAGIC %fs 
// MAGIC head "dbfs:/mnt/datalake/FHVTaxiTripData_201812_01.csv"

// COMMAND ----------

val fhvTaxiTripDF = spark
  .read
  .option("header",true)
  .option("inferSchema",true)
  .csv("/mnt/datalake/FHVTaxiTripData_201812_01.csv")

  display(fhvTaxiTripDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create Schema

// COMMAND ----------

val fhvTaxiTripSchema = StructType(
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

val fhvTaxiTripSchemaDF = spark
  .read
  .option("header",true)
  .schema(fhvTaxiTripSchema)
  .csv("/mnt/datalake/FHVTaxiTripData_201812_*.csv")

// COMMAND ----------

display(fhvTaxiTripSchemaDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Generate Schema to JSON file 

// COMMAND ----------

val fhvBaseDf = spark
.read
.option("multiline",true)
.option("inferSchema",true)
.json("/mnt/datalake/FhvBases.json")

// COMMAND ----------

val fileBasesSchema = StructType(
  List(
    StructField("Date",StringType,true),
    StructField("Address",
      StructType(
        List(
          StructField("Building",StringType, true)
        )
      ),true
    )
  )
)

// COMMAND ----------

val fhvBaseWithSchemaDf = spark
.read
.option("multiline",true)
.option("inferShema",true)
//.schema(fileBasesSchema)
.json("/mnt/datalake/FhvBases.json")

// COMMAND ----------

display(fhvBaseWithSchemaDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transofarmación de datos

// COMMAND ----------

display(yellowTaxiDF.describe("passenger_count","trip_distance"))

// COMMAND ----------

var yellowTaxiDateDf = yellowTaxiDF
.filter(col("passenger_count")> 0)

yellowTaxiDateDf.count

// COMMAND ----------

yellowTaxiDateDf = yellowTaxiDateDf.where(col("trip_distance")> 0.0)

yellowTaxiDateDf.count

// COMMAND ----------

yellowTaxiDateDf.na.drop(Seq("PULocationID","DOLocationID"))

// COMMAND ----------

val defaultValueMap = Map(
  "Payment_Type"->5,
  "RateCodeID"->1
)

yellowTaxiDateDf.na.fill(defaultValueMap)

// COMMAND ----------

yellowTaxiDateDf.dropDuplicates()

// COMMAND ----------

yellowTaxiDateDf = yellowTaxiDateDf.where(col("tpep_pickup_datetime")>="2018-12-01").where(col("tpep_dropoff_datetime")<"2019-01-01")

// COMMAND ----------

// Validate the max date in field [tpep_dropoff_datetime]
display(yellowTaxiDateDf.select(min(col("tpep_pickup_datetime"))))

// COMMAND ----------

// Validate the max date in field [tpep_dropoff_datetime]
display(yellowTaxiDateDf.select(max(col("tpep_dropoff_datetime"))))

// COMMAND ----------

// ejeucte the trsnformation steps in a moment 

val defaultValueMap = Map(
  "Payment_Type"->5,
  "RateCodeID"->1
)

var yellowTaxiDateUniqueMovDf = yellowTaxiDF
.filter(col("passenger_count")> 0)
.where(col("trip_distance")> 0.0)
.na.drop(Seq("PULocationID","DOLocationID"))
.na.fill(defaultValueMap)
.dropDuplicates()
.where(col("tpep_pickup_datetime")>="2018-12-01").where(col("tpep_dropoff_datetime")<"2019-01-01")

// COMMAND ----------

// Validate the max date in field [tpep_dropoff_datetime]
display(yellowTaxiDateUniqueMovDf.select(min(col("tpep_pickup_datetime"))))



// COMMAND ----------

// Validate the max date in field [tpep_dropoff_datetime]
display(yellowTaxiDateUniqueMovDf.select(max(col("tpep_dropoff_datetime"))))

// COMMAND ----------

// MAGIC %md
// MAGIC ##Manipulacion de columnas

// COMMAND ----------

val fhvTaxiTripData = spark
  .read
  .option("header",true)
  .schema(fhvTaxiTripSchema)
  .csv("dbfs:/mnt/datalake/FHVTaxiTripData_201812_*.csv")

// COMMAND ----------

var fhvTaxiTripDataDF = fhvTaxiTripData
.select(
  $"Pickup_Datetime".alias("PickupTime")
  ,$"DropOff_datetime"
  ,$"PUlocationID"
  ,$"DOlocationID"
  ,$"SR_Flag"
  ,$"Dispatching_base_number"
)

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

fhvTaxiTripDataDF = fhvTaxiTripDataDF
.withColumnRenamed("DropOff_datetime","DropTime")
.withColumnRenamed("PUlocationID","PickuoLocationId")
.withColumnRenamed("DOlocationID","DropLocationId")
.withColumnRenamed("Dispatching_base_number","BaseLicenseNumber")

// COMMAND ----------

fhvTaxiTripDataDF = fhvTaxiTripDataDF
.withColumn("TripYear",year($"PickupTime"))
.withColumn("TripMonth",month($"PickupTime"))
.select(
  $"*",
  dayofmonth($"PickupTime").alias("TripDay")
)

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

fhvTaxiTripDataDF = fhvTaxiTripDataDF
.withColumn("TripTimeInMinutes",round(unix_timestamp($"DropTime")- unix_timestamp($"PickupTime"))/60)

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

fhvTaxiTripDataDF = fhvTaxiTripDataDF
.withColumn("TripType",
        when($"SR_Flag" ===1 ,"SharedTrip").otherwise("SoloTrip")
)

fhvTaxiTripDataDF.printSchema

// COMMAND ----------

display(fhvTaxiTripDataDF)

// COMMAND ----------

val fhvBaseDF = spark
.read
.option("multiline",true)
.option("inferShema",true)
.json("/mnt/datalake/FhvBases.json")

// COMMAND ----------

var fhvBaseFlatDF = fhvBaseDF
.select(
  $"License Number".alias("BaseLicenseNumber"),
  $"Type of Base".alias("BaseType"),
  $"Address.Building".alias("AddressBuilding"),
  $"Address.City".alias("AddressCity")
)

display(fhvBaseFlatDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Se crea union entre frames

// COMMAND ----------

var fhvTaxiTripDataWithBaseOf = fhvTaxiTripDataDF
.join(fhvBaseFlatDF, Seq("BaseLicenseNumber"),"inner")


display(fhvTaxiTripDataWithBaseOf)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Operaciones de agregacion

// COMMAND ----------

 fhvTaxiTripDataWithBaseOf = fhvTaxiTripDataWithBaseOf
.groupBy("AddressCity","BaseType")
.agg(sum("TripTimeInMinutes"))
.withColumnRenamed("sum(TripTimeInMinutes)","TotalTripTime")
.orderBy("AddressCity","BaseType")

// COMMAND ----------

display(
fhvTaxiTripDataWithBaseOf
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Uso de lenguaje SQL 

// COMMAND ----------

fhvTaxiTripDataWithBaseOf.createOrReplaceTempView("LocalTaxiTripData")

// COMMAND ----------

var sqlDf = spark.sql("select * from LocalTaxiTripData")

display(sqlDf)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from LocalTaxiTripData

// COMMAND ----------

fhvTaxiTripDataWithBaseOf.createOrReplaceGlobalTempView("TaxiTripData")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Revision de registros corruptos

// COMMAND ----------

// Permissive => permite los datos corruptos
// DropMalFormated => descarta datos corruptos
// FailFast => Arroja un error al intentar procesar 
// 

var rateCode = spark
.read
.option("mode","DropMalFormated")
.option("badRecordsPath","mnt/datalake/JsonBadRecords") // Los datos que presentan error serán almacenados en el path
.json("/mnt/datalake/RateCodes.json")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Carga de Datos

// COMMAND ----------

// Muestra número de particiones
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

// cambia el número de particiones [No recomendado]
spark.conf.set("spark.sql.shuffle.partitions", 10)

// COMMAND ----------

// append => adjunta info
// ErrorIfExists => manda error si existe
// Ignore => ignora la escritura si existe error
// Overwrite => sobrescribe la info existente 

yellowTaxiDateUniqueMovDf
  .write
  .option("header",true)
  .mode("overwrite")
  .csv("/mnt/datalake/ernesto.martinez/YellowTaxiData.csv")

// COMMAND ----------

spark
  .read
  .option("header",true)
  .csv("/mnt/datalake/ernesto.martinez/YellowTaxiData.csv")
  .count

// COMMAND ----------

yellowTaxiDateUniqueMovDf
  .rdd
  .getNumPartitions

// COMMAND ----------

// coalesce => Disminuye el número de particiones
// repartition => Aumenta número de particiones

yellowTaxiDateUniqueMovDf = yellowTaxiDateUniqueMovDf.coalesce(1)

yellowTaxiDateUniqueMovDf
  .rdd
  .getNumPartitions

// COMMAND ----------

yellowTaxiDateUniqueMovDf = yellowTaxiDateUniqueMovDf.repartition(10)

yellowTaxiDateUniqueMovDf
  .rdd
  .getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC ## Guardar información en parquet

// COMMAND ----------

yellowTaxiDateUniqueMovDf
  .write
  .mode("overwrite")
  .format("delta")
  .parquet("/mnt/datalake/ernesto.martinez/YellowTaxiData.parquet")

// COMMAND ----------

var parquetDataFrame = spark.read.parquet("/mnt/datalake/ernesto.martinez/YellowTaxiData.parquet")

display(parquetDataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Guardar datos en tablas 

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC Create DATABASE IF NOT EXISTS TaxiServiceWareHouse

// COMMAND ----------


// Tabla administrada
yellowTaxiDateUniqueMovDf
  .write
  .mode("overwrite")
  .saveAsTable("TaxiServiceWareHouse.YellowDataManaged")

// COMMAND ----------

// Tabla no administrada 
yellowTaxiDateUniqueMovDf
  .write
  .mode("overwrite")
  .option("path", "/mnt/datalake/ernesto.martinez/YellowTaxiTablaData.parquet")
  .saveAsTable("TaxiServiceWareHouse.YellowDataTabla")

// COMMAND ----------

// MAGIC %md
// MAGIC ### identificar tabla [administrada o no administrada]

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC DESCRIBE EXTENDED TaxiServiceWareHouse.YellowDataTabla

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC DESCRIBE EXTENDED TaxiServiceWareHouse.YellowDataManaged

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crear una tabla con comandos sql 

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC Drop table TaxiServiceWareHouse.YellowDataTabla

// COMMAND ----------

display(spark.sql("select * from TaxiServiceWareHouse.YellowDataTabla"))

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS TaxiServiceWareHouse.YellowDataTabla
// MAGIC using DELTA
// MAGIC OPTIONS 
// MAGIC (
// MAGIC   path  "/mnt/datalake/ernesto.martinez/YellowTaxiTablaData.parquet"
// MAGIC )

// COMMAND ----------

display(spark.sql("select * from TaxiServiceWareHouse.YellowDataTabla"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Leer tablas con comandos scala

// COMMAND ----------

display(
  spark
  .read
  .table("TaxiServiceWareHouse.YellowDataTabla")

)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Z-Order 

// COMMAND ----------


