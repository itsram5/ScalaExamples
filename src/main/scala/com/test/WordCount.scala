import org.apache.spark.{SparkContext, _}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Word Count")
      .setSparkHome("src/main/resources")
    val hdfsuri ="hdfs://localhost:9000/fibrevillage/input/raw_sample_1.txt"
    conf.set("fs.defaultFS", hdfsuri)
    val sc = new SparkContext(conf)
    val input = sc.textFile(hdfsuri)
   // val input = sc.textFile("src/main/resources/input/humpty.txt")
    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("hdfs://localhost:9000/fibrevillage/output/humpty_output.txt")
    println("OK")
  }
}