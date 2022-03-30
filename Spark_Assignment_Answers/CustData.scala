package ProblemSolving
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object CustData extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  
   val cust_df=spark.read.format("csv")
           .option("inferschema", true)
          .option("header", true)
          .option("path","C:/Users/prave/OneDrive/Desktop/problem Solving/Customer.csv")
          .load()
          
    //cust_df.show(30,false)
    
    
     val Bill_df=spark.read.format("csv")
           .option("inferschema", true)
          .option("header", true)
          .option("path","C:/Users/prave/OneDrive/Desktop/problem Solving/BillData.csv")
          .load()
          
    //Bill_df.show(30,false)
    
    
   val Location_df=spark.read.format("csv")
           .option("inferschema", true)
          .option("header", true)
          .option("path","C:/Users/prave/OneDrive/Desktop/problem Solving/Location.csv")
          .load()
          
    //Location_df.show(30,false)
          
   //val windowSpec  = Window.partitionBy("Cust_id").orderBy("Amount")
   
   //Bill_df.withColumn("row_number",row_number.over(windowSpec)).show(false)
          
          
   val joincon=cust_df.col("Mobile")===Bill_df.col("Phno")
   
   val CustBill=cust_df.join(Bill_df,joincon,"inner").drop(Bill_df.col("Cust_id")).drop(Bill_df.col("Phno")).orderBy("Name")
   
   
   val JoinCon1=CustBill.col("Geoid")===Location_df.col("Geoid")
   
   val FinalJoin=CustBill.join(Location_df,JoinCon1,"inner").drop(Location_df.col("Geoid"))
   
   //FinalJoin.show(30)
   
   val windowSpec  = Window.partitionBy("Cust_id").orderBy("Name")
   
   val windowSpecAgg  = Window.partitionBy("Cust_id")
   
   val aggDf=FinalJoin.withColumn("row", row_number.over(windowSpec))
   .withColumn("Total_Amount", sum(col("Amount")).over(windowSpecAgg)).orderBy(desc("Total_Amount")).where("row=1")
   .select("Name", "Cust_id","Geoid","Location","Total_Amount").limit(5)
   
   //aggDf.show(false)
   
   cust_df.createOrReplaceTempView("Customer")
   
   Bill_df.createOrReplaceTempView("Bill")
   
   Location_df.createOrReplaceTempView("location")
   
   spark.sql("""with CTE as(
     select  Cust_id,sum(Amount) over(partition by Cust_id) as Total_amount,
     row_number() over (partition by Cust_id order by Cust_id) as rn from Bill) 
      select distinct ct.Cust_id,c.Name,l.Geoid,l.location,ct.Total_amount from CTE ct 
      join Customer c on ct.Cust_id = c.Cust_id 
      join location l on l.Geoid = c.Geoid
      where rn=1
      order by ct.Total_amount desc limit 5 """).show(50)
   
  
}
