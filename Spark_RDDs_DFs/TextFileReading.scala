package demo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TextFileReading {
  
  def main(args:Array[String]):Unit={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark:SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("RddCreation")
      .getOrCreate()
      
      //val lines=spark.sparkContext.textFile("D:/Spark/textfiles/*")
      
      //lines.foreach(println)
      
      val rddWhole=spark.sparkContext.wholeTextFiles("D:/Spark/textfiles/*")
      
      rddWhole.foreach(f=>{
        //println(f._1 +"=>"+f._2)
      })
      
      val rdd2=spark.sparkContext.textFile("D:/Spark/textfiles/sample.txt,D:/Spark/textfiles/sample2.txt")
      
      rdd2.foreach(f=>{
        //println(f)
      })
      
      val rdd3=spark.sparkContext.textFile("D:/Spark/csv/sample.txt")
      val rdd4=rdd3.map(f=>{
        f.split(",")
      })
      
      val rdd5=rdd4.foreach(f=>{
        println("ID:"+f(0)+",Address:"+f(1)+",City:"+f(2)+",State:"+f(3)+",ZipCode:"+f(4))
      })
      
  }
}
