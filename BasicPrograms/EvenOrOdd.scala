package BasicPrograms
import scala.io.StdIn._
object EvenOrOdd {
  
  def main(args:Array[String]){
    val n=readInt()
    if(n%2==0) 
      println("even")
    else 
      println("Odd")
  }
}
