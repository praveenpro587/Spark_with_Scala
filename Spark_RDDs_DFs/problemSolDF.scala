package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

case class Person(Name:String, Age:Int, City:String)

object problemSolDF extends App{
  
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
  
  //We can create Column names as below also
  //DatasetDf.toDF("name","age","city")
 
  import spark.implicits._
  //Converting DF to DS
  val DS=DF.as[Person]
  
  
  //Creating UDF 
  val parseAge=udf(ageCheck(_:Int):String)
  
  val df1=DF.withColumn("Adult",parseAge(col("Age")))
  
  df1.show(false)
  
  //Syntax for filter or in place of filter we can use where both are same
  df1.filter(df1("Adult")==="Y").show(false)
  //df1.filter($"Adult"==="Y")
  //df1.filter(col("Adult")==="Y")
  //df1.filter('Adult==="Y")
  
  
  
}
