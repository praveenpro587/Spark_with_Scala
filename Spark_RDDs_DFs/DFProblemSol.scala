package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DFProblemSol extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val list=List((1,"2013-07-25",11599,"CLOSED"),(2,"2014-07-25",256,"PENDING PAYMENT"),
      (3,"2013-07-25",11599,"COMPLETE"),(4,"2019-07-25",8827,"CLOSED"))
      
  //val schema="order_id Int,Order_date String,Customer_id Int,Status String"
  
  import spark.implicits._
  
  val ordersDf=spark.createDataFrame(list)
  .toDF("order_id","order_date","customer_id","Status")
  .withColumn("order_date", unix_timestamp(col("order_date") //converting into unix_timestamp
  .cast(DateType))) //casting into datetype
  .withColumn("New_id", monotonically_increasing_id) //unique id generation
  .dropDuplicates("order_date","customer_id") //drop duplicates based on columns
  .drop("order_id") //drop column
  .select("New_id","order_date","customer_id","Status") //selecting columns in order we want
  .orderBy("New_id")
  
  ordersDf.show(false)
}
