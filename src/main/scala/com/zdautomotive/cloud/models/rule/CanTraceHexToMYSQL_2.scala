package com.zdautomotive.cloud.models.rule

import java.sql.PreparedStatement
import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, CKDataSourceSingleton, DBCTools, MYSQLDataSourceSingleton}
import org.apache.spark.sql.{DataFrame, Row}

import java.util.Date
import scala.collection.mutable.ListBuffer

class CanTraceHexToMYSQL_2 extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    //    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    //    val frame = CANTools.getHexTrace("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)
    //        val frame = CANTools.getHexTrace("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas3", sc, spark)
    //    frame.show(100, false)
    val frame = CANTools.getHexTrace("file:///C:\\学习资料\\testHbase\\datas3", sc, spark)
    frame
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    //    println(traceDF.count())
    val frame = CANTools.parseRawCan(dbcDF, traceDF)
    //    frame.show(100,false)

    //    println("traceDF " + frame.rdd.getNumPartitions)
    //    frame.show(100,false)
    //    frame.show(100, false)
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
    //    resultDF.show(100, false)
    println("Parse finish. Start writing!")
    //    println(resultDF.rdd.getNumPartitions).
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

    //    resultDF.show(100,false)

    resultDF.foreachPartition((partition: Iterator[Row]) => {
      //      println("**********************")
      val dsPool = MYSQLDataSourceSingleton.getDataSourceInstance
      val partitionConn = dsPool.getConnection()
      var num = 1

      val sql = "INSERT INTO "
      var sqlExecute = new StringBuilder(sql)

      var tmpId: Int = 0
      var psmt: PreparedStatement = null

      partition.foreach {
        row =>
          if (num % 10000 == 0) {
            //            println(num)
            println("---------------------")
            psmt.executeBatch()
          }

          val id = row.getAs[Int]("id")
          if (tmpId != id) {
            //            println(num)
            if (psmt != null) {
              psmt.executeBatch()
              psmt.close()
            }
            var sqlExecute = new StringBuilder(sql)
            tmpId = id
            val table = dbcMap(id)
            sqlExecute.append(table + " VALUES (")
            val length = dbcInfo(table).size
            for (i <- 0 to length) {
              sqlExecute.append("?,")
            }
            sqlExecute.deleteCharAt(sqlExecute.length - 1).append(")")
            psmt = partitionConn.prepareStatement(sqlExecute.toString)
            //            println(sqlExecute)
            //            psmt = partitionConn.prepareStatement()
            //            sqlExecute.append("Datalogger1_" + table + " USING " + table + " TAGS (2,\"Datalogger1\")" + " VALUES ")
          }
          val ts = row.getAs[Long]("ts")
          psmt.setLong(1, ts)
          val dataArray = row.getSeq[Long](2)
          dataArray.zipWithIndex.foreach {
            case (data, i) => {
              psmt.setLong(i + 2, data)
            }
          }
          psmt.addBatch()
          num += 1

        //          println("------------------------")

      }
      psmt.executeBatch()
      psmt.close()
      partitionConn.close()
    })
  }
}

object CanTraceHexToMYSQL_2 {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val model = new CanTraceHexToMYSQL_2
    model.executeModel()
    var end_time = new Date().getTime
    println(end_time - start_time)
  }
}