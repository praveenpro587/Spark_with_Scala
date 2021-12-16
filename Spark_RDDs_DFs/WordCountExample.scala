package demo

//import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object WordCountExample {
  
  def main(args:Array[String]):Unit={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    /*val spark:SparkSession=SparkSession.builder()
      .master("local[1]")
      .appName("WordCount")
      .getOrCreate()*/
    val sc=new SparkContext("local[*]","WC")
      
      //val lines=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark/search_data.txt")
      val lines=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-week1/search_data.txt")
      //lines.foreach(println)
      val Splitwords=lines.flatMap(x=>x.split(" "))
      val words=Splitwords.map(x=>(x,1))
      val WC=words.reduceByKey((x,y)=>x+y)
      //val maxVal=WC.collect().maxBy(x=>x._2)//Getting max key based on value
      //val firsttwoval=WC.sortBy(x=>x._2,false).take(2)
      //println(maxVal)
      //firsttwoval.foreach(println)
      //val filterkey=Splitwords.map(x=>x=="praveen")//Checking string "praveen" there or not
      //WC.saveAsTextFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark/hello.txt")//Save output to file
      //val top10=WC.sortBy(x=>x._2,false).take(10)//Without interchanging
      val interchange=WC.map(x=>(x._2,x._1))//interchanging key,value
      val top10=interchange.sortByKey(false).take(10)//Filtering top 10
      top10.foreach(println)
      
  }
}
