package ProblemSolving

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.io._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FileSchema extends App{
  
  val p=new Properties()
  p.load(new FileReader("C:/Users/prave/OneDrive/Desktop/Azure DataBricks/ConfigFile.properties"))
  
 Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  
  //val file1Schema="C:/Users/prave/OneDrive/Desktop/Azure DataBricks/SchemaFile.txt"
  
  val schemaString=p.getProperty("file1Schema")
  
  val typeToSparkType = Map("int" -> IntegerType, "string" -> StringType,"date"->DateType,"timestamp"->TimestampType)
  
  val colNameType = schemaString.split(",").map{s=>
    val values=s.split(" ")
    // Tuple of column name and type
    (values(0),values(1))
}

// prepare schema
var schema = StructType(colNameType.map(t => StructField(t._1, typeToSparkType(t._2), true)))
  
val LocString=p.getProperty("filelocation")

val df=spark.read.format("csv")
        .schema(schema)
        .option("sep",",")
        .load(LocString)
        
df.show(false)

//val updatedDF = df.withColumn("last_update", to_timestamp(col("last_update")))

//updatedDF.show(false)

df.printSchema()
  
}
