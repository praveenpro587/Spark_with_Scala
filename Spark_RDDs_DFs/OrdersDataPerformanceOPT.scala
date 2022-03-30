package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object OrdersDataPerformanceOPT extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark=SparkSession.builder()
  .appName("OrdersDataPerformanceOPT")
  .master("local[*]")
  .getOrCreate()
  
  val df=spark.read
  .format("csv")
  .option("inferSchema", true)
  .option("header", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/orders.csv")
  .load
  
  df.createOrReplaceTempView("Orders")
 //Query-1 takes almost 4 minutes for execution as it uses SORT Agg internally
  spark.sql("""select order_customer_id,date_format(order_date,'MMMM') order_month,
            count(1) CNT,first(date_format(order_date,'M')) MonthNum 
            from Orders group by order_customer_id,order_month order by cast(MonthNum as int)""").show()
  
  
  //Below Query-2 will give more optimization that above Query-1 because of casting logic of MonthNum
  //Query-2 takes 1.2 minutes for execution as it uses HASH Agg internally
  spark.sql("""select order_customer_id,date_format(order_date,'MMMM') order_month,
            count(1) CNT,first(cast(date_format(order_date,'M') as int)) MonthNum 
            from Orders group by order_customer_id,order_month order by MonthNum""").show()
  
}
