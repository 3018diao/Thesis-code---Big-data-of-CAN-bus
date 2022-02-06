package com.zdautomotive.cloud.tools

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.DataFrame

import java.util
import scala.collection.JavaConverters

object HBaseTools {
  def write(dataframe: DataFrame, zks: String, port: String, tableName: String, family: String, rowKeyColumn: String): Unit = {
    val stagingFolder = s"datas/hbase/output-${System.nanoTime()}"

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zks)
    conf.set("hbase.zookeeper.property.clientPort", port)
    val str = conf.get("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily")
//    println("------hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily:" + str)
    //    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
//
    val connection = ConnectionFactory.createConnection(conf)

    // 将DataFrame中数据转换为RDD[(RowKey, Put)]
    val cfBytes = Bytes.toBytes(family)
    //    cfBytes.foreach(println)
    val columns = dataframe.columns.sorted // 从 DataFrame 中获取列名称
    //    columns.foreach(println)

    //    val key = dataframe.rdd.map(line => line.)
    //    val map1: RDD[TransferTime] = key.map(line => line._2)

    val datasRDD = dataframe.rdd.map {
      row => {
        //        println(row)
        // TODO: row 每行数据转换为二元组 (RowKey, Put)
        // a.获取RowKey的值
        //        val rowKey = row.getAs[String](rowKeyColumn(0)) + row.getAs[String](rowKeyColumn(1))
        val rowKey = row.getAs[String](rowKeyColumn)
        //        println("*******************************")
        //        println(rowKey)
        val rkBytes = Bytes.toBytes(rowKey)
        var linkedList = new util.LinkedList[KeyValue]()
        // b.构建Put对象
        for (column <- columns) {
          if (column != "rowkey") {
            val kv: KeyValue = new KeyValue(
              rkBytes,
              cfBytes,
              Bytes.toBytes(column),
              Bytes.toBytes(row.getAs[String](column)))
            linkedList.add(kv)
          }
        }
        //        val put = new Put(rkBytes)
        //        //        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`")
        //        // c.设置列值
        //        columns.foreach {
        //          column => {
        //            val value = row.getAs[String](column)
        //            //            println("column " + column)
        //            //            println("value" + value)
        //            put.addColumn(cfBytes, Bytes.toBytes(column), Bytes.toBytes(value))
        //            //            println("-------------------------------")
        //          }
        //        }
        // d.返回二元组
        (new ImmutableBytesWritable(rkBytes), linkedList)
      }
    }
//    datasRDD.collect()

    val result = datasRDD.flatMapValues(
      s => {
        val values: Iterator[KeyValue] = JavaConverters.asScalaIteratorConverter(s.iterator()).asScala
        values
      }
    )

    //    result.collect().foreach(println)

    var table: Table = null
    try {
      val job = Job.getInstance(conf)
      job.setJobName("DumpFile")
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[Put])

      result.saveAsNewAPIHadoopFile(
        stagingFolder,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration
      )

      val load = new LoadIncrementalHFiles(conf)
      table = connection.getTable(TableName.valueOf(tableName))
      val regionLocator = connection.getRegionLocator(TableName.valueOf(tableName))
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
      load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])
    } catch {
      case e: Exception => e.printStackTrace()
    }


    //        result.saveAsNewAPIHadoopFile(
    //          s"datas/hbase/output-${System.nanoTime()}",
    //          classOf[ImmutableBytesWritable],
    //          classOf[Put],
    //          classOf[TableOutputFormat[ImmutableBytesWritable]],
    //          conf
    //        )

  }
}
