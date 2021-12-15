package BasicPrograms
import scala.io.StdIn._
object SumOfValues {
  def main(args:Array[String]){
    var sum=0
    val n=readInt()
    for(i<-0 to n){
      sum+=i;
      println(i)
    }
    println(sum)
  }
}
