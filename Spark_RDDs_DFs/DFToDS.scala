package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp


case class Orders(order_id:Int, order_date:Timestamp,order_customer_id:Int,order_status:String)

object DFToDS extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val SparkConf=new SparkConf()
  SparkConf.set("spark.app.name", "MyAPP")
  SparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(SparkConf)
  .getOrCreate()
  
  //Dataset[Row] is nothing but Dataframe
  val ordersDf:Dataset[Row]=spark.read
  .option("header",true)
  .option("inferSchema",true)
  .csv("C:/Users/prave/OneDrive/Desktop/orders.csv")
  
  import spark.implicits._
  val OrdersDS=ordersDf.as[Orders].filter(x=>x.order_id>10)
  //Here we give wrong column name it will show error directly(compile time safety)
  //filter(x=>x.order_id>10)
  OrdersDS.show()
  
  
  spark.stop()
}
