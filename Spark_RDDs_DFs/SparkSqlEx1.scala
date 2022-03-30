package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode

object SparkSqlEx1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val ordersDf=spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/orders.csv")
  .load
  
  ordersDf.createOrReplaceTempView("orders")
  
  val result=spark.sql("select order_status,count(*) as status_count from orders group by order_status order by status_count")
  
  //result.show(false)
  
  //How many orders each customer placed
  
  val cust_ord_cnt=spark.sql("select order_customer_id,count(*) as cust_cnt from orders where order_status='CLOSED'"+
      "group by order_customer_id order by cust_cnt desc")
  
  cust_ord_cnt.show(false)
  
  /*result.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/orders")
  .save*/
}
