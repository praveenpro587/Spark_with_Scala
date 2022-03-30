package ProblemSolving

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import demo.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import java.time.LocalDate //for dateformat thing
import java.time.format.DateTimeFormatter

object ordersUdf extends App{
  
  
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
  
  //df.printSchema()
  
    //val df1=df.withColumn("order_date",(col("order_date").cast("date")))
  
  val df1=df.withColumn("order_date", date_format(col("order_date"),"yyyy-MM-dd"))
  
  //df1.printSchema()
    
    /*def status_check(dataframe:DataFrame,order_date:Column,order_status:Column)={
    
      if((dataframe("order_customer_id")==11599) && (dataframe("order_status")=="CLOSED")){
        "Completed"
      }
      else order_status
  }
  
  
  
  
  val finaldf=df1.withColumn("New_status", lit(status_check(df1,df1("order_date"),df1("order_status"))))
  
  finaldf.show(false)*/
  
  def status_check(dte:String,status:String)={
    if(dte=="2013-07-25" && status=="PENDING_PAYMENT"){
      "Completed"
    }
    else status
  }
  
  val update_pay_method=udf(status_check(_:String,_:String))
  
  val value=df1.withColumn("new_status",update_pay_method(col("order_date"),col("order_status")))
  
  value.show(1000,false)
  
}
