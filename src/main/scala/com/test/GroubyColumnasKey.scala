package com.test
/*
Split each line by separator
Group lines by first column
Remove first column from each line
Transform all strings to numbers
Sum lines
Print result

 */
object GroubyColumnasKey {
  def main(args: Array[String]) {
    val data = List("a 1 2 3", "b 1 2 3", "a 2 3 4", "b 3 4 5")
    data.map(_.split(" ")) // 1
      .groupBy(_.head) // 2
      .mapValues(
      _.map(
        _.tail // 3
          .map(_.toInt)) // 4
        .reduce((a1, a2) => a1.zip(a2).map(tuple => tuple._1 + tuple._2))) // 5
      .foreach(pair => println(s"${pair._1} ${pair._2.mkString(" ")}")) // 6

  }
}
