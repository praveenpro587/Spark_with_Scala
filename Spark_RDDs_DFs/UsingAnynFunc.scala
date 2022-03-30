package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UsingAnynFunc extends App{
  
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
  
  //Anonymous Function
  spark.udf.register("parseAgeFunc",(x:Int)=>{
    if(x>18) "Y" else "N"
  })
  
  DF.withColumn("Adult", expr("parseAgeFunc(age)")).show()
  
  spark.catalog.listFunctions().filter(x=>x.name=="parseAgeFunc").show()
  
  
  //If functon register to the catalog so directly we can use it in SQL query
  DF.createOrReplaceTempView("PeopleTable")
  spark.sql("select name,age,city,parseAgeFunc(age) as Adult from PeopleTable").show()
  
}
