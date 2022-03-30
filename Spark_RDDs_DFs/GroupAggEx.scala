package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GroupAggEx extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val OrdersDf=spark.read
  .format("csv")
  .option("header", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/order_data.csv")
  .load
  
  //Object Expression
  OrdersDf.groupBy("Country", "InvoiceNo")
  .agg(
      sum("Quantity").as("Total_Quantity"),
      sum(expr("Quantity * UnitPrice")).as("Invoice_value")
      ).show(false)
      
      
  //String Expression
  OrdersDf.groupBy("Country", "InvoiceNo")
  .agg(expr("sum(Quantity) as Total_Quantity"),
      expr("sum(Quantity * UnitPrice) as Invoice_value")
      ).show(false)
      
  //Spark SQL     
 OrdersDf.createOrReplaceTempView("InvoiceData")
 spark.sql("""select Country,InvoiceNo,sum(Quantity) as Total_Quantity,
              sum(Quantity * UnitPrice) as Invoice_value from InvoiceData
              group by Country,InvoiceNo""").show(false)
  
  
}
