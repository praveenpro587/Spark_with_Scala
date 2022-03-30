package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object StringExprUDF extends App{
  
  def ageCheck(age:Int)={
    if(age>18) "Y" else "N"
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val schema="name String,Age Int,City String"
  
  val DF=spark.read
  .format("csv")
  .schema(schema)
  .option("path", "C:/Users/prave/OneDrive/Desktop/dataset1")
  .load
  
  //Sql/String EXPR UDF
  spark.udf.register("parseAgeFunc",ageCheck(_:Int):String)
  DF.withColumn("Adult", expr("parseAgeFunc(age)")).show()
  
  
  
  
}
