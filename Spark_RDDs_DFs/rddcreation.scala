package demo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object rddcreation {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark:SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("RddCreation")
      .getOrCreate()
      
      val rdd=spark.sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9))
      
      println("Rdd Count: " +rdd.count())
      
      val rdd1=spark.sparkContext.parallelize(List(1,2,3,4,5))
      
      println("First Element:"+rdd1.first())
      
      println("No of Partitions:"+rdd.getNumPartitions)
      
      val emptyRdd=spark.sparkContext.parallelize(Seq.empty[String])
      
      println("Empty RDD:"+emptyRdd)
  }
}
