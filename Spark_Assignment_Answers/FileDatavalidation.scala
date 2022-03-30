package ProblemSolving
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.Level
import org.apache.log4j.Logger

object FileDatavalidation {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  val ROW_COUNT : Int = 11
  val COL_COUNT : Int = 11
  val OUTPUT_DATE_FORMAT : String = "MM/dd/yyyy"
  val INPUT_DATE_FORMAT : String = "dd-MMM-yy"
  val SPACIAL_CHAR : List[String] = List("-",".")
  val COL_DROP : List[String] = List("COMMISSION_PCT")
  val DATE_COL : List[String] = List("HIRE_DATE")
  val INT_COL : List[String] = List("EMPLOYEE_ID","SALARY","DEPARTMENT_ID","MANAGER_ID")
  val SPACIAL_CHAR_COL : List[String] = List("PHONE_NUMBER")
  val SchemaValidate:List[String]=List("integer","string","string","string","string","string","string","integer","string","string","integer")


  def rowAndColCountValidation(employee: DataFrame): Unit ={
    if(employee.columns.length == COL_COUNT){
      if(employee.count() == ROW_COUNT) {
        print("Number of row and col are valid\n")
      }else if(employee.count() < ROW_COUNT){
        print("Number of Rows missing\n")
      }else{
        print("Number of Rows greater than threshold\n")
      }
    }
    else{
      if(employee.columns.length < COL_COUNT) {
        print("Number of cols missing\n")
      }else {
        print("Number of Rows greater than threshold\n")
      }
    }
  }

  def dateTypeValidation(employee: DataFrame): DataFrame = {
    var emp = employee
    for (i <- DATE_COL){
      emp = emp.select(col("*"), date_format(to_date(col(i),INPUT_DATE_FORMAT),OUTPUT_DATE_FORMAT).as("new_"+i)).drop(i)
    }
    emp
  }

  def nullValueValidate(employee: DataFrame): DataFrame = {
    employee.na.drop()
  }

  def dropColValidate(emp: DataFrame): DataFrame = {
    var emp1 = emp
    for (i <- COL_DROP){
      emp1 = emp.drop(i)
    }
    emp1
  }

  def intColValidation(emp: DataFrame): DataFrame ={
    var emp1 = emp
    for(i <- INT_COL) {
      println(emp1.schema(i).dataType+"  "+i)
      if(emp1.schema(i).dataType != "IntegerType"){
        emp1 = emp1.withColumn(i, functions.regexp_replace(emp1.col(i), "[.-/_*&^%$#@!()+=]", "0"))
        emp1 = emp1.withColumn(i,emp1(i).cast(IntegerType))
      }
    }
    emp1
  }

  def removeSpecialCharCalidation(emp: DataFrame): DataFrame = {
    var emp1 = emp
    for(i <- SPACIAL_CHAR_COL){
      emp1 = emp1.withColumn(i, functions.regexp_replace(emp.col(i),"[.-/_*&^%$#@!()+=]",""))
    }
    emp1
  }






    def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession.builder().appName("DataValidation").master("local").getOrCreate()
      val employee: DataFrame = spark.read
        .format("csv")
        .option("inferSchema", value = true)
        .option("header", value = true)
        .option("delimiter", ",")
        .load("C:/Users/prave/OneDrive/Desktop/Azure DataBricks/destemployees.csv")

      employee.printSchema()

      //null value validation
      var emp = nullValueValidate(employee)

      //drop col
      emp = dropColValidate(emp)

      //row & col count validation
      rowAndColCountValidation(emp)

      //date type validation
      emp = dateTypeValidation(emp)

      //INT_COL VALIDATION
      emp = intColValidation(emp)

      //REMOVE SPECIAL CHAR
      emp = removeSpecialCharCalidation(emp)

      emp.show()
    }
  
  
  
}
