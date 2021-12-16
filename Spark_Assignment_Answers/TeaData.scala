package demo
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object TeaData extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark:SparkSession=SparkSession.builder()
      .master("local[*]")
      .appName("RddCreation")
      .getOrCreate()
 
val df = spark.read.option("delimiter", "|").csv("C:/Users/prave/OneDrive/Desktop/SanjayAssign/Tea_plantation.txt")
val names : Array[String] = Array("SNO", "District", "Area","Production","Productivity")
def splitCSV(dataFrame: DataFrame, columnNames : Array[String], spark: SparkSession) : DataFrame = {
    import spark.implicits._
    val columns = dataFrame.columns
    var finalDF : DataFrame = Seq.empty[(String,String,String,String,String)].toDF(columnNames:_*)
    for(order <- 0 until(columns.length) -5 by(5) ){
      finalDF = finalDF.union(dataFrame.select(col(columns(order)).as(columnNames(0)),
                                               col(columns(order+1)).as(columnNames(1)),
                                               col(columns(order+2)).as(columnNames(2)),
                                               col(columns(order+3)).as(columnNames(3)),
                                               col(columns(order+4)).as(columnNames(4))
                                              ))
    }
    finalDF
  }
val finalDF = splitCSV(df, names, spark)
//finalDF.show(false)

finalDF.createOrReplaceTempView("Tea")
val df1=spark.sql("Select min(Production),max(Production),sum(Area) from Tea")
//df1.show()
}
