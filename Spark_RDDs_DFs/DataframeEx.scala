package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object DataframeEx extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val SparkConf=new SparkConf()
  SparkConf.set("spark.app.name", "MyAPP")
  SparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(SparkConf)
  .getOrCreate()
  
  
  //Programatic way of defining Schema
  val Orderschema=StructType(List(
      StructField("order_id",IntegerType,false),
      StructField("order_date",TimestampType),
      StructField("Customer_order_id",LongType),
      StructField("Status",StringType)
      ))
  
  //DDL way of creating schema which supports after spark 2.4 version
  //val DDLSchema="Order_id Int, Order_date Timestamp, Cust_id Long, Status String"
  //Dataset[Row] is nothing but Dataframe
  val ordersDf:Dataset[Row]=spark.read
  .option("header",true)
  .schema(Orderschema)
  .csv("C:/Users/prave/OneDrive/Desktop/orders.csv")
  
  /*val groupedDF=ordersDf.repartition(4)
  .where("order_customer_id>1000")
  .select("order_customer_id")
  .groupBy("order_customer_id")
  .count()*/
  
  ordersDf.show(false)
 
  
  ordersDf.printSchema()
  spark.stop()
}
