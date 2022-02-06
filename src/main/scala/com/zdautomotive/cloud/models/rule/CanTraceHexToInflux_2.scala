package com.zdautomotive.cloud.models.rule

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools, MYSQLDataSourceSingleton}
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.PreparedStatement
import java.util.Date
import scala.collection.mutable.ListBuffer
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.{BatchPoints, Point}

import java.util.concurrent.TimeUnit

class CanTraceHexToInflux_2 extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    //    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    //        val frame = CANTools.getHexTrace("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)
    //    CANTools.getHexTrace_clone("/user/root/canTrace", sc, spark)
    val frame = CANTools.getHexTrace("file:///C:\\material\\testHbase\\datas3", sc, spark)
    //    frame.show(100, false)
    frame
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    //    println(traceDF.count())
    import traceDF.sparkSession.implicits._

    val frame = CANTools.parseRawCan(dbcDF, traceDF)
    //    frame.show(100,false)

    //    println("traceDF " + frame.rdd.getNumPartitions)
    //    frame.show(100,false)
    //        frame.show(100, false)
    frame
  }

  //  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
  //    import resultDF.sparkSession.implicits._
  //    resultDF.filter($"ts"==="1601253345070147")
  //      .show(5, false)
  //  }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._

    //      dbcDF.show(100,false)
    resultDF.show(100, false)
    println("Parse finish. Start writing!")
    //    println(resultDF.rdd.getNumPartitions).
    val dbcMap = dbcDF.select("identifier", "botschaft")
      .dropDuplicates("identifier").orderBy("identifier")
      .map(ele => {
        Integer.parseInt(ele.get(0).toString) -> ele.get(1).toString
      }).collect().toMap


    println("------------------")

    val dbcInfo = dbcDF.select("botschaft", "info")
      .dropDuplicates("botschaft").orderBy("botschaft")
      .map(item => {
        //        val value = item.getList[StructType](1)
        var infos = new ListBuffer[String]
        val info = item.getSeq[Row](1)
        //        value.forEach(ele => println(ele))
        //        value.forEach(ele => ele.)
        info.foreach(infos += _.getString(0))
        //        println(infos)
        item.getString(0) -> infos
      }).collect().toMap

    resultDF.foreachPartition((partition: Iterator[Row]) => {
      //      println("**********************")

      val client = InfluxDBFactory.connect("http://node01:8086", "admin", "")
      client.isGzipEnabled()

      //      val dsPool = MYSQLDataSourceSingleton.getDataSourceInstance
      //      val partitionConn = dsPool.getConnection()
      var num = 1
      var batchPoints = BatchPoints
        .database("audi_a8")
        .consistency(InfluxDB.ConsistencyLevel.ALL)
        .build();

      partition.foreach {
        row =>
          if (num % 5000 == 0) {
            println("---------------------")
            client.write(batchPoints)
            batchPoints = BatchPoints
              .database("audi_a8")
              .consistency(InfluxDB.ConsistencyLevel.ALL)
              .build();
          }
          val id = row.getAs[Int]("id")
          val ts = row.getAs[Long]("ts")
          val table = dbcMap(id)
          var point = Point.measurement(table)
            .tag("device", "Datalogger1")
            .time(ts, TimeUnit.MICROSECONDS)
          //          psmt.setLong(1, ts)
          val dataArray = row.getSeq[Long](2)
          dataArray.zipWithIndex.foreach {
            case (data, i) => {
//              println(dbcMap(id))
//              println(data)
//              println("---------------")
//              println(dbcInfo(table)(i))
              point.addField(dbcInfo(table)(i), data)
            }
          }
          batchPoints.point(point.build())
          num += 1
      }
      client.write(batchPoints)
    })
  }
}

object CanTraceHexToInflux_2 {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val model = new CanTraceHexToInflux_2
    model.executeModel()
    var end_time = new Date().getTime
    println(end_time - start_time)
  }
}
