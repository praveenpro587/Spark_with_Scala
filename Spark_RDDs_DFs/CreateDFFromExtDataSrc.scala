//Connect to MySql for creating DF
//We need Jar mysql-connector-java.jar
//Command to execute spark-shell --driver-class-path /user/home/mysql-connector-java.jar

package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object CreateDFFromExtDataSrc extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark=SparkSession.builder()
  .appName("CreateDFFromExtDataSrc")
  .master("local[*]")
  .getOrCreate()
  
  //Connection_url
  val connection_URL="jdbc:mysql://hostname/dbname"
  
  //Properties like username & PSWD
  
  val mysql_props=new java.util.properties
  mysql_props.setProperty("user","Praveen")
  mysql_props.setProperty("password","hello123")
  
  val DF=spark.read.jdbc(connection_URL,"tableName",mysql_props)
  
  DF.show(false)
  
}
