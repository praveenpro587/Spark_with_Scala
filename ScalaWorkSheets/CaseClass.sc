object CaseClass {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
   case class Person(name:String,age:Int)
  
  val p=new Person("praveen",26)                  //> p  : CaseClass.Person = Person(praveen,26)
  val p1=new Person("praveen",26)                 //> p1  : CaseClass.Person = Person(praveen,26)
  
  p.toString                                      //> res0: String = Person(praveen,26)
  
  p==p1                                           //> res1: Boolean = true
  
  val p3=p                                        //> p3  : CaseClass.Person = Person(praveen,26)
  println(p3)                                     //> Person(praveen,26)
}
