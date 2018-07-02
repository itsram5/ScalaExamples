package com.test


import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

object HBaseSample {
  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()
    val tableName = "spark_table"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)
    val tableDesc = new HTableDescriptor(tableName)
    tableDesc.addFamily(new HColumnDescriptor("cf".getBytes()))
    admin.createTable(tableDesc)

  }

}
