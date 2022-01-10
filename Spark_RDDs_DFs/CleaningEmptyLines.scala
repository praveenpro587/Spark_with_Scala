package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
object CleaningEmptyLines extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","RemEmpLines")
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week2/samplefile.txt")
  val cleanrdd=filerdd.filter(x=>(!x.isEmpty))
  cleanrdd.collect.foreach(println)
  
}
