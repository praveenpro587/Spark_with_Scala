package BasicPrograms

object AddingLists {
  def main(args:Array[String]){
    val l1=List(1,2,3,4)
    val l2=List(5,6,7,8)
    val c=l1.flatMap(x=>l2.map(y=>x+y))
    println(c)
    var line="hello praveen how are you hi praveen how are you hello"
    var d=line.split(" ").groupBy(identity).mapValues(_.size)
    d.foreach(println)
  }
}
