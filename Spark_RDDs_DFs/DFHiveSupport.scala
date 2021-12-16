package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object DFHiveSupport extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
   val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport() //it will use for creating hive metastore
  .getOrCreate()
  
  val orderDf=spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/orders.csv")
  .load
  
  spark.sql("create database if not exists Retail")
  
  //orderDf.show(false)
  
  orderDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .bucketBy(4,"order_customer_id")
  .sortBy("order_customer_id")
  .saveAsTable("Retail.orders")
  
  spark.catalog.listTables("Retail").show()
  
  spark.stop()
  
}
