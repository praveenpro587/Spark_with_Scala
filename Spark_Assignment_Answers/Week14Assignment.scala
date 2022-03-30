package demo


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object Week14Assignment extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark=SparkSession.builder()
  .appName("OrdersDataPerformanceOPT")
  .master("local[*]")
  .getOrCreate()
  
  val OrdersSchema=StructType(List(
      StructField("order_id", IntegerType, true),
      StructField("order_date", StringType),
      StructField("order_customer_id", IntegerType),
      StructField("order_status", StringType)
      ))
      
 val CustomersSchema=StructType(List(
     StructField("order_item_id", IntegerType, true),
     StructField("order_item_order_id", IntegerType),
     StructField("order_item_product_id", IntegerType),
     StructField("order_item_quantity", IntegerType),
     StructField("order_item_subtotal", FloatType),
     StructField("order_item_product_price", FloatType)
     ))
 val OrdersDF=spark.read
 .format("csv")
 .option("header", true)
 .schema(OrdersSchema)
 .option("path", "C:/Users/prave/OneDrive/Desktop/Orders.txt")
 .load()
 
  val CustomersDF=spark.read
 .format("csv")
 .schema(CustomersSchema)
 .option("path", "C:/Users/prave/OneDrive/Desktop/Order_items.txt")
 .load()
 
val JoinCon=OrdersDF.col("order_id")===CustomersDF.col("order_item_order_id")
val JoinType="inner"

//AUTO BroadcastJoin OFF
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")


val JoinedDF=CustomersDF.join(broadcast(OrdersDF),JoinCon,JoinType)
.persist(StorageLevel.MEMORY_AND_DISK_SER)

//Now solve the problem using DataFrames approach 
  

val DFSol=JoinedDF.groupBy(to_date(col("order_date")).alias("Formatted_Date"),col("order_status"))
.agg(round(sum("order_item_subtotal"),2).alias("Total"),
    countDistinct("order_id").alias("Total_orders"))
    .orderBy(col("Formatted_Date").desc,
        col("order_status"),
        col("Total").desc,
        col("Total_orders"))
        
 DFSol.show(false)
  //Now solve the problem using SparkSQL approach 
  
 JoinedDF.createOrReplaceTempView("OrdersTable")
 
 val SparkSQLDF=spark.sql("""select cast(to_date(order_date) as String) as Formatted_date,order_status,
           cast(sum(order_item_subtotal) as DECIMAL(10,2)) as Total,count(distinct(order_id)) as Total_orders
           from OrdersTable group by Formatted_date,order_status order by Formatted_date desc,order_status,
           Total desc,Total_orders""")
           
 SparkSQLDF.show(false)
  
  
}
