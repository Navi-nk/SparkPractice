package com.navi.spark.scalarecap

object ScalaRecap extends App {
  val aBoolean: Boolean = false
  var aBool:Boolean = false

  val anIfExpression = if(2 > 3) "this" else "that"

  //instructions vs expression
  val theUnit = println("Hi there") //unit (void in java)

  def myFunc(s: String) = "Arg given " + s

  //oop
  class Animal
  class Dog extends Animal
  trait Carnivore{
    def eat(animal: Animal) : Unit
  }

  class Tiger extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Tasty animal")
  }

  //singleton pattern
  object MySingleton
  // companions
  object Tiger

  //generics
  trait MyList[A]

  //method notation
  val x = 1 + 2
  val y = 1.+(2)

  //Functional Programming
  val incrementer : Function1[Int, Int]  = new Function1[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }

  val incrementer1: Int => Int = (x: Int) => x + 1
  println(incrementer(1) == incrementer1(1)) // true


  //higher order functions
  println(List(1, 2, 3) map incrementer) //List(2, 3, 4)

  //partial function
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 1234
  }

  //implicits
  def methodWithImplicit(implicit x: Int) = x + 99
  implicit val a = 10
  println(methodWithImplicit) //109

  case class Person(name: String) {
    def greeting = "Hi there " + name
  }

  implicit def fromStringtoPerson(name:String): Person = Person(name)
  println("Naval".greeting)

  implicit class Per(name: String) {
    def greet ="Hi there " + name
  }

  println("Naval".greet)

}
