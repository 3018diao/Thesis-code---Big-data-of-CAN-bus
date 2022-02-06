package com.zdautomotive.cloud.tools

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.DataFrame

import java.util
import scala.collection.JavaConverters

object HBaseTools2 {
  def write(dataframe: DataFrame, zks: String, port: String, tableName: String, family: String, rowKeyColumn: String): Unit = {
    val stagingFolder = s"datas/hbase/output-${System.nanoTime()}"

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zks)
    conf.set("hbase.zookeeper.property.clientPort", port)

    //    conf.set(TableOutputFormat.OUTPUT_TABLE, table)

    val connection = ConnectionFactory.createConnection(conf)

    // 将DataFrame中数据转换为RDD[(RowKey, Put)]
    val cfBytes = Bytes.toBytes(family)

    val datasRDD = dataframe.rdd.map {
      row => {
        val rowKey = row.getAs[String](rowKeyColumn)
        val rkBytes = Bytes.toBytes(rowKey)

        var linkedList = new util.LinkedList[KeyValue]()

        val list = row.getAs[Map[String, Long]]("map")

        val keyArray = list.keySet.toArray.sorted

//        println("=============================")

        for (column <- keyArray) {
          //          println(elem._1 + " " + elem._2)
//          println(column)
//          println(list(column))
          val kv: KeyValue = new KeyValue(
            rkBytes,
            cfBytes,
            Bytes.toBytes(column),
            Bytes.toBytes(list(column).toString))

          linkedList.add(kv)
//          println("----------------------")
        }
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

    var table: Table = null
    try {
      val job = Job.getInstance(conf)
      job.setJobName("DumpFile")
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])

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
  }
}
