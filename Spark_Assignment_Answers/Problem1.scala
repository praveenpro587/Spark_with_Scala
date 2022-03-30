package ProblemSolving

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object Problem1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark: SparkSession = SparkSession.builder().appName("DataValidation").master("local").getOrCreate()
   val rdd1=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Azure DataBricks/Problem1.txt")
   
   val rdd2=rdd1.map(x=>(x.split(",")(0),List(x.split(",")(1),x.split(",")(2))))
   
   val df1=spark.createDataFrame(rdd2).toDF("id","Value")
   
   val df2=df1.select(df1.col("id"),explode(df1.col("Value"))).withColumnRenamed("col", "Value")
   
   df2.show()
}
