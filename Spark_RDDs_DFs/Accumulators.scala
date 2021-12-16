package demo
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object Accumulators extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","Accum")
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week2/samplefile.txt")
  val accum=sc.longAccumulator("Counting Empty Lines")
  filerdd.foreach(x=>if(x=="") accum.add(1))
  println(accum.value)

}
