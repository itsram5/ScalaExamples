package com.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Hadoop {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Word Count")
      .setSparkHome("src/main/resources")
    val hdfsuri ="hdfs://localhost:9000/fibrevillage/input/raw_sample_1.txt"
    conf.set("fs.defaultFS", hdfsuri)
    val sc = new SparkContext(conf)
   val rDD= readRawSampleasTextFile(sc,hdfsuri)
    if(rDD !=null)
    rDD.foreach(x=>println(x))
    else
      println("RDD is null")


    println("OK")
  }

  def writeWordCount(sc: SparkContext, hdfsuri:String ) = {
    val input = sc.textFile(hdfsuri)
    // val input = sc.textFile("src/main/resources/input/humpty.txt")
    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("hdfs://localhost:9000/fibrevillage/output/humpty_output.txt")
  }

  def readRawSampleFile(sc: SparkContext, hdfsuri: String): RDD[(String, Array[Byte])] = {
    var sampleFile: String = "/fibrevillage/input/raw_sample_1.txt"
    // RDD with (xpid, rowkey)
    // TODO: error handling by IO exception
    val readRdd = sc.sequenceFile[String,  Array[Byte]](hdfsuri)

    return readRdd
  }

  def readRawSampleasTextFile(sc: SparkContext, hdfsuri: String): RDD[(String)] = {
    var sampleFile: String = "/fibrevillage/input/raw_sample_1.txt"
    // RDD with (xpid, rowkey)
    // TODO: error handling by IO exception
    val readRdd = sc.textFile(hdfsuri)

    return readRdd
  }

  def writeRawSampleFile(rdd: RDD[(String)], sampleId: String):  RDD[(String)] = {
    var sampleFile: String = "/fibrevillage/input/raw_sample_seq1"
    rdd.saveAsTextFile(sampleFile)
    return rdd;
  }

}
