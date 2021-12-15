object ClassExample {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  class Person(val name:String,val age:Int){
  	
  	val x=20
  	
  	def doubleage=age*2
  	
  	def getSalary(sal:Int)=sal
  }
  val p=new Person("praveen",26)                  //> p  : ClassExample.Person = ClassExample$Person$1@21bcffb5
  p.name                                          //> res0: String = praveen
  p.doubleage                                     //> res1: Int = 52
  p.getSalary(10000)                              //> res2: Int = 10000
}
