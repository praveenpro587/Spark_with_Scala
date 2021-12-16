package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowAggEx extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val schema="country String,weeknum Int,numinvoices Int,totalquantity Int,invoicevalue Double"
  
  val WindowDF=spark.read
  .format("csv")
  .schema(schema)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/windowdata.csv")
  .load
  
  val mywindow=Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  
  WindowDF.withColumn("Running_Total",sum("invoicevalue").over(mywindow)).show()
  
   //WindowDF.withColumn("Running_Total",ntile(2).over(mywindow)).show()
  
}
