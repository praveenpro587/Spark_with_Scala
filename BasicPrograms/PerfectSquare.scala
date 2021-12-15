import scala.io.StdIn._

object PerfectSquare extends App{
  
  val n:Int=readInt()
  val billAmount:String=readLine()
  
  val billamt:Array[Int]=billAmount.split(" ").map(x=>x.toInt)
  var count=0
  if(billamt.length==n)
  {
    for(i <- billamt)
    {
      val sqrt=Math.sqrt(i)
      if(sqrt.ceil-sqrt==0)
        count+=1
    }
  }
  else
    println("No of customers and no of bills should be same")
    
  println(count)
  
  
}

