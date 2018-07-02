package com.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}

object HbaseWrite {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()

    val tableName = "spark_table"
    val table = new HTable(conf, tableName)
    var row = new Put("dummy".getBytes())
    row.add("cf".getBytes(), "content".getBytes(), "Test data".getBytes())
    table.put(row)
    table.flushCommits()

  }
}
