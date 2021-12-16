package demo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCount {
  
  def main(args:Array[String]):Unit={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark:SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("RddCreation")
      .getOrCreate()
      
      val rdd=spark.sparkContext.textFile("D:/Spark/textfiles/alice.txt")
      
      val lines=rdd.flatMap(f=>f.split(" "))
      
      val words=lines.map(f=>(f,1))
      
      val wordcount=words.reduceByKey(_+_)
      
      wordcount.foreach(println)
  }
}
