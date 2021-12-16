package demo
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.explode

object explodeFunc {
  
  
  def main(args:Array[String]):Unit={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark:SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("RddCreation")
      .getOrCreate()
      

      
      import spark.implicits._
      
      val test=spark.read.json(("""{"a":1,"b":[2,3]}"""))
      test.printSchema
      
  }
}
