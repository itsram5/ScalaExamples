package com.test

import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {

  def hex2bytes(hex: String): Array[Byte] = {
    if(hex.contains(" ")){
      hex.split(" ").map(Integer.parseInt(_, 16).toByte)
    } else if(hex.contains("-")){
      hex.split("-").map(Integer.parseInt(_, 16).toByte)
    } else {
      hex.sliding(2,2).toArray.map(Integer.parseInt(_, 16).toByte)
    }
  }


  /*val byteString: ByteString = ByteString.copyFrom(
    DatatypeConverter.parseHexBinary(hexString)
  )*/
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("GroupBy")
      .setSparkHome("src/main/resources")
     val sc = new SparkContext(conf)
     val input = sc.textFile("/Users/ramanjaneya.naidu/ScalaTestExamples1/src/main/resources/input/raw_sample_test3.txt")
   // val splitkeys = input.map(line => line.split("\\t")).groupBy(_.head).map(x => (x._1, x._2.toList))
    /*val splitkeys = input.map(line => line.split("\\t")).
      groupBy(_.head).mapValues( _.map(_.tail.toString)).map(x => (x._1, x._2.toList))*/
    val splitkeys = input.map(_.split("\\t").toList).groupBy(_.head).mapValues( _.map(_.tail)).map(x => (x._1, x._2));

    //count.saveAsTextFile("src/main/resources/output/raw_sample_test1.txt")
    println("OK")
  }
}
