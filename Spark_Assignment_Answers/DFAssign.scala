package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode


object DFAssign extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
 val schema=StructType(List(
      StructField("Country",StringType),
      StructField("WeekNum",IntegerType),
      StructField("InvoiceNo",IntegerType),
      StructField("TotalQun",IntegerType),
      StructField("InvoiceVal",DoubleType)
      ))
  
  val WindowDF=spark.read
  .format("csv")
  .schema(schema)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/windowdata.csv")
  .load
  
  WindowDF.show(false)
  
  WindowDF.write
  .partitionBy("Country","WeekNum")
  .mode(SaveMode.Overwrite)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/windowdataparquet")
  .save
  
  WindowDF.write
  .format("avro")
  .partitionBy("Country")
  .mode(SaveMode.Overwrite)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/windowdataAVRo")
  .save
  
   WindowDF.write
   .format("csv")
  .partitionBy("Country")
  .mode(SaveMode.Overwrite)
  .option("maxRecordsPerFile", 2000)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/windowdatacsv")
  .save
  
}
