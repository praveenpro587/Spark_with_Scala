package demo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object DataFrame {
  def main(args:Array[String]):Unit={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark:SparkSession=SparkSession.builder()
      .master("local[*]")
      .appName("RddCreation")
      .getOrCreate()
      import spark.implicits._
      val columns = Seq("language","users_count")
      val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
      
      val rdd=spark.sparkContext.parallelize(data)
      
      val df=rdd.toDF(columns:_*)
      
      df.printSchema()
      df.show()
      
  }
  
}
