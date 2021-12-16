package demo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object practice {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()  
      
      val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))   
      val rdd=spark.sparkContext.parallelize(dataSeq)
      
      rdd.foreach(println)
  }
}
