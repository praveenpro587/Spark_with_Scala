package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

case class windowdata(Country:String,WeekNum:Int,InvoiceNo:Int,TotalQun:Int,InvoiceVal:String)

object DFAssgn2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  import spark.implicits._
  
  val WindowDF=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/windowdata.csv")
  .map(x=>x.split(","))
  .map(x=>windowdata(x(0),x(1).trim.toInt,x(2).trim.toInt,x(3).trim.toInt,x(4)))
  .toDF()
  .repartition(8)
  
  WindowDF.show(false)
  
  WindowDF.write
  .format("json")
  .mode(SaveMode.Overwrite)
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/windowdataJson")
  .save
  
}
