package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SimpleAggEx extends App{
  
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
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/order_data.csv")
  .load
  
  //Object Expression
  OrdersDf.select(
      count("*").as("Total_Rows"),
      sum("Quantity").as("Total_Quantity"),
      avg("UnitPrice").as("AVG_Unit_Price"),
      countDistinct("InvoiceNo").as("Distinct_INV")
      ).show()
      
  //String Expression    
  OrdersDf.selectExpr(
      "count(*) as Total_Rows",
      "sum(Quantity) as Total_Quantity",
      "avg(UnitPrice) as AVG_Unit_Price",
      "count(Distinct(InvoiceNo)) as Distinct_INV"
      ).show(false)
 //Spark SQL     
 OrdersDf.createOrReplaceTempView("InvoiceData")
 spark.sql("select count(*) as Total_Rows,"+
     "sum(Quantity) as Total_Quantity,"+
     "avg(UnitPrice) as AVG_Unit_Price,"+
     "count(Distinct(InvoiceNo)) as Distinct_INV from InvoiceData").show()
  
}
