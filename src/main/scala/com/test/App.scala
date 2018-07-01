package com.test

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("Hello ")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(Array("RAM","SRI")))
    val numbers =List(1,2,3)
    println(numbers.map((i=>i*2)))
  }

}
