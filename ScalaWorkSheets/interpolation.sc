object interpolation {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  var name:String="praveen"                       //> name  : String = praveen
  //S Interpolation
  println(s"Hello $name how are you?")            //> Hello praveen how are you?
  
  val pi:Float=3.1431f                            //> pi  : Float = 3.1431
  
  //F interpolation
  println(f"The value of pi is $pi%.2f")          //> The value of pi is 3.14
  
  //Raw interpolation
  println(raw"Hello \n who are you")              //> Hello \n who are you
  
  val str1="praveen"                              //> str1  : String = praveen
  val str2="praveen"                              //> str2  : String = praveen
  
  val str3=str1==str2                             //> str3  : Boolean = true
  
  
  val num=7                                       //> num  : Int = 7
  num match {
  	case 1=>println("one")
  	case 2=>println("two")
  	case 3=>println("Three")
  	case _=>println("No matches")
  }                                               //> No matches
  
  for (i <- 1 to 10)
  	println(i)                                //> 1
                                                  //| 2
                                                  //| 3
                                                  //| 4
                                                  //| 5
                                                  //| 6
                                                  //| 7
                                                  //| 8
                                                  //| 9
                                                  //| 10
  
  var y=0                                         //> y  : Int = 0
  while(y<=10){
  	println(y)
  	y=y+1
  }                                               //> 0
                                                  //| 1
                                                  //| 2
                                                  //| 3
                                                  //| 4
                                                  //| 5
                                                  //| 6
                                                  //| 7
                                                  //| 8
                                                  //| 9
                                                  //| 10
  var z=0                                         //> z  : Int = 0
  do{
  println(z)
  z=z+1
  }while(z==10)                                   //> 0
  
  {val v=10; v+20}                                //> res0: Int = 30
  
  var bb={
  val n=10
  val m=n+10
  m
  }                                               //> bb  : Int = 20
  println(bb)                                     //> 20
}
