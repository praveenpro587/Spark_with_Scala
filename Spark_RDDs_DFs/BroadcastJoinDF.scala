package demo
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BroadcastJoinDF extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //If we use expliict Schema it will improve performance W.R.T time as it don't need extra time for inferrring schema
  val Orderschema=StructType(List(
      StructField("order_id",IntegerType,false),
      StructField("order_date",TimestampType,true),
      StructField("Customer_order_id",LongType,true),
      StructField("Status",StringType,true)
      ))
      
      
      
  val OrdersDf=spark.read
  .format("csv")
  .option("header", true)
  .schema(Orderschema)
  .option("path", "C:/Users/prave/OneDrive/Desktop/orders.csv")
  .load
  
   val CustomersDF=spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/customers.csv")
  .load
  
  spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
  
  val joinCon=OrdersDf.col("Customer_order_id")===CustomersDF.col("customer_id")
  
  val JoinDf=OrdersDf.join(broadcast(CustomersDF),joinCon,"inner")
  
  JoinDf.show(false)
  
  
}
