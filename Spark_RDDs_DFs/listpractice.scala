package demo


import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object listpractice {
  def main(args:Array[String]):Unit={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark:SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("RddCreation")
      .getOrCreate()
      import spark.implicits._
      //val columns = Array("language","users_count")
      var list=Array(1,2,3,4,6,7)
      //print(list(1))
      var n=list.length
      for(i <-0 until n-1 by 2)
      {
        if(list(i)<list(i+1))
        {
          val temp=list(i)
          list(i)=list(i+1)
          list(i+1)=temp
        }
        
      }
      println(list.mkString(" "))
  }
  
}
