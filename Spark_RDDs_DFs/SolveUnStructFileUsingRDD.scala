package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.Date

object SolveUnStructFileUsingRDD extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val regex="""^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  
  case class Orders(order_id:Int,order_date:String,customer_id:Int,status:String)
  
  def parser(line:String)={
    
    line match{
      case regex(order_id,order_date,customer_id,status)=>
        Orders(order_id.toInt,order_date,customer_id.toInt,status)
    }
  }
  
  val ordersRdd=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/orders_new.csv")
  
  import spark.implicits._
  
  val DS=ordersRdd.map(parser).toDS().cache()
  DS.select("status")
  .groupBy("status")
  .count().show()
  
}
