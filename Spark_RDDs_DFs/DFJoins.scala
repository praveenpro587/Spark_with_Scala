package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object DFJoins extends App{
  
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
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/orders.csv")
  .load
  
   val CustomersDF=spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/customers.csv")
  .load
  
  val joinCon=OrdersDf.col("order_customer_id")===CustomersDF.col("customer_id")
  val JoinDF=OrdersDf.join(CustomersDF,joinCon,"outer")
  .sort("order_id")
  .withColumn("order_id", expr("coalesce(order_id,-1)"))
  JoinDF.show(false)
  
}
