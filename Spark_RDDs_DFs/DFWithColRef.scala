package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DFWithColRef extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val OrderDF=spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/orders.csv")
  .load
  
  //Using Column String 
  //OrderDF.select("order_id", "order_status").show(false)
  
  //We can't use both column string and column object in same statement like below
  //OrderDF.select("order_id",col("order_status"))
  
  import spark.implicits._
  
  //Using column object-->either with col,column,scala specific $,'
  OrderDF.select(col("order_id"), column("order_status"),$"order_date",'order_customer_id).show()
  
  //Column Expression with column object
  OrderDF.select(col("order_id"),expr("concat(order_status,'_STATUS')")).show(false)
  
  //Simplest way to use column strings with expression
  OrderDF.selectExpr("order_id","concat(order_status,'_STAUS')").show(false)
  
  
  
  
}
