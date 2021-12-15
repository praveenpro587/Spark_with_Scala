package BasicPrograms
import scala.io.StdIn._
object AreaOfCircle {
  def main(args:Array[String]){
    //areaofcircle=2*pi*r*r
    val pi=3.14
    val r=readFloat()
    val area=2*pi*r*r
    println(area)
  }
}
