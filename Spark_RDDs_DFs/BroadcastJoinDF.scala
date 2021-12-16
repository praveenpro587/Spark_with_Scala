package demo
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object BroadcastJoinDF extends App{
  
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
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/orders.csv")
  .load
  
   val CustomersDF=spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/customers.csv")
  .load
  
  spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
  
  val joinCon=OrdersDf.col("order_customer_id")===CustomersDF.col("customer_id")
  
  val JoinDf=OrdersDf.join(broadcast(CustomersDF),joinCon,"inner")
  
  JoinDf.show()
  
  
}
