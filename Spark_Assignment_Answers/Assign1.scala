package ProblemSolving

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object Assign1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark: SparkSession = SparkSession.builder().appName("DataValidation").master("local").getOrCreate()
   
   val rdd1=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Azure DataBricks/src_dest.txt")
   val rdd2=rdd1.map(x=>(x.split(",")(0).toInt,x.split(",")(1)))
   
   val rdd3=rdd2.reduceByKey((x,y)=>(x.concat(",")+y)).sortBy(x=>x._1)
   
   val rdd4=rdd3.map(x=>(x._1,x._2.split(",")(0),x._2.split(",")(1)))
   
   rdd4.foreach(println)

   //rdd3.foreach(println)  
   
   //val rowrdd=rdd3.map(x=>Row(x._1,x._2))
   
   val schema="id int,value String"
   
   //rowrdd.foreach(println)
   
    val df1=spark.createDataFrame(rdd4).toDF("id","SRC","DEST")
    
    df1.show(false)
}
