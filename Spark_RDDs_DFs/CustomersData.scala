package demo

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object CustomersData extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","Customers")
  
  val fileread=sc.textFile("C:/Users/prave/OneDrive/Desktop/customerorders.csv")
  val maprdd=fileread.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  val finalrdd=maprdd.reduceByKey(_+_)
  finalrdd.foreach(println)
  val sortrdd=finalrdd.sortBy(x=>x._2,false)
  val res=sortrdd.take(10)
  res.foreach(println)
}
