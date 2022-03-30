package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StandarLoadingDF extends App{
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val SparkConf=new SparkConf()
  SparkConf.set("spark.app.name", "StandardDF")
  SparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(SparkConf)
  .getOrCreate()
  
  /*val DF=spark.read
  .format("csv")
  .option("header", "true")
  .option("inferschema", "true")
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/orders.csv")
  .load*/
  
  /*val DF=spark.read
  .format("json")
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/players.json")
  .option("mode", "FAILFAST")
  .load*/
  
  val DF=spark.read
  .option("path", "C:/Users/prave/OneDrive/Desktop/users.parquet")
  .load
  
  DF.show(false)
  
  spark.stop()
  
}
