package demo
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.util.matching.Regex
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object regexEx extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
    val spark:SparkSession=SparkSession.builder()
      .master("local[*]")
      .appName("RddCreation")
      .getOrCreate()
  
  val sc=new SparkContext("local[*]","Sample")
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/SanjayAssign/sample.txt")
  filerdd.map(x=>x.split("\\|"))
  
  
}
