package com.zdautomotive.cloud.models.rule

import com.datastax.driver.core.{BatchStatement, BoundStatement, Cluster, PreparedStatement}
import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.sql.{DataFrame, Row}

import java.math.BigInteger
import java.sql.Timestamp
import java.util.Date
import scala.collection.mutable.ListBuffer

class CanTraceHexToCassandra_3 extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    //    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    //        val frame = CANTools.getHexTrace("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)
    //    CANTools.getHexTrace_clone("/user/root/canTrace", sc, spark)
    //    val frame = CANTools.getHexTrace("/user/root/canTrace", sc, spark)
    val frame = CANTools.getHexTrace_clone("file:///C:\\material\\testHbase\\datas3", sc, spark)
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
    frame.filter($"identifier" === 168)
  }

  //  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
  //    import resultDF.sparkSession.implicits._
  //    resultDF.filter($"ts"==="1601253345070147")
  //      .show(5, false)
  //  }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._

    println("Parse finish. Start writing!")

    val dbcMap = dbcDF.select("identifier", "botschaft")
      .dropDuplicates("identifier").orderBy("identifier")
      .map(ele => {
        Integer.parseInt(ele.get(0).toString) -> ele.get(1).toString
      }).collect().toMap

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
            val host = "127.0.0.1"

            val port = 9042
//      val cluster = CassandraDataSourceSingleton.getDataSourceInstance
            val cluster = Cluster.builder.addContactPoint(host).withPort(port).build
            val session = cluster.connect("audi_a8")
//      val session = cluster.connect("audi_a8")
      var tmpId: Int = 0
      val sql = "INSERT INTO "
      var sqlExecute = new StringBuilder(sql)
      //      val session = cluster.connect("audi_a8")
      //
      var num = 1
      //
      var psmt: PreparedStatement = null
      var bsmt: BoundStatement = null
      var batch: BatchStatement = null
      //
      partition.foreach {
        row =>
          if (num % 10000 == 0) {
            //            println(num)
            println("---------------------")
            session.execute(batch)
            batch.clear()
          }

          val id = row.getAs[Int]("id")
          if (tmpId != id) {
            //            println(num)
            if (psmt != null) {
              session.execute(batch)
              batch.clear()
            }
            val sqlExecute = new StringBuilder(sql)
            tmpId = id
            val table = dbcMap(id)
            val length = dbcInfo(table).size
            //            sqlExecute.append(table + " VALUES (")
            sqlExecute.append(table + " (deviceID,ts,")
            val listBuffer = dbcInfo(table)
            for (elem <- listBuffer) {
              sqlExecute.append(elem + ",")
            }
            sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") VALUES (")
            for (i <- 0 to length+1) {
              sqlExecute.append("?,")
            }
            sqlExecute.deleteCharAt(sqlExecute.length - 1).append(")")
            //            println(sqlExecute)
            psmt = session.prepare(sqlExecute.toString)
            batch = new BatchStatement

            //            println(sqlExecute)
            //            psmt = partitionConn.prepareStatement()
            //            sqlExecute.append("Datalogger1_" + table + " USING " + table + " TAGS (2,\"Datalogger1\")" + " VALUES ")
          }

          //          val ts = row.getAs[Long]("ts")
          val ts: Long = row.getAs[Long]("ts") / 1000
          val boundStatement = psmt.bind();
          boundStatement.setString(0, "Datalogger1")
          boundStatement.setTimestamp(1, new Timestamp(ts))
          val dataArray = row.getSeq[Long](2)
          dataArray.zipWithIndex.foreach {
            case (data, i) => {
              //              boundStatement.bind(data.asInstanceOf[AnyRef])
              //              boundStatement.setLong(i + 1, data)
              boundStatement.setVarint(i + 2, BigInteger.valueOf(data))
            }
          }
          batch.add(boundStatement)
          num += 1
      }
      session.execute(batch)
      batch.clear()
      session.close()
      cluster.close()
    })
  }
}

object CanTraceHexToCassandra_3 {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val model = new CanTraceHexToCassandra_3
    model.executeModel()
    var end_time = new Date().getTime
    println(end_time - start_time)
  }
}








