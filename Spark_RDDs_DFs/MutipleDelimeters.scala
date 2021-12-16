package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object MutipleDelimeters extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  case class Person(Name:String,Age:Int)
  
  val filerdd=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/SampleFile.txt")
  val header=filerdd.first()
  
  val newRdd=filerdd.filter(x=>x!=header)
  
  import spark.implicits._
  
  val DF=newRdd.map(x=>x.mkString.split("\\~\\|"))
  .map(x=>Person(x(0),x(1).toInt)).toDF()
  
  DF.show()
  
  val DF1=DF.select(split(col("Name"),",").getItem(0).as("FirstName"),
      split(col("Name"),",").getItem(1).as("LastName"),col("Age"))
      
      DF1.show()
  
}
