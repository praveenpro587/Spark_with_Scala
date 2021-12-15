object Collections {
  println("Welcome to the Scala worksheet")
  
  val a=Array(1,2,3,4)
  
  println(a.mkString(","))
  a.foreach(println)
  a(0)=10
  a.foreach(println)
  val b=List(1,2,3,5)
  b.head
  
  for (i<- b)
  	println(i)
  b.reverse
  
  val t=("Summit",10000,26)
  println(t._1)
  
  val x="hello";
  val x="Bye"
  
}
