package com.zdautomotive.cloud.models.rule

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools, DataSourceSingleton}
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class CanTraceHexToTD extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    //    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    //    val frame = CANTools.getHexTrace("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)
    val frame = CANTools.getHexTrace("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas3", sc, spark)
    frame

  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    //    println(traceDF.count())
    val frame = CANTools.parseRawCan(dbcDF, traceDF)
    //    frame.show(100,false)

    //    println("traceDF " + frame.rdd.getNumPartitions)
    //    frame.show(100,false)
    frame
  }

  //    override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
  //      resultDF.show(5, false)
  //    }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //    resultDF.show(100, false)

    //    dbcDF.show(100, false)

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

    //    dbcInfo.foreach(println)
    //    val connection = dsPool.getConnection
    //    val statement = connection.createStatement()
    //
    //    // create tables
    //    dbcMap.foreach(ele => {
    //      val createTableSql = s"CREATE TABLE IF NOT EXISTS can_q8_info.${ele._2}_1_2 USING ${ele._2}" + " TAGS (2, \"Datalogger1\")"
    //      statement.addBatch(createTableSql)
    //    })
    //    statement.executeBatch
    //    statement.close
    //    connection.close

    //    resultDF.show(100, false)

    resultDF.foreachPartition((partition: Iterator[Row]) => {
      val dsPool = DataSourceSingleton.getDataSourceInstance
      val partitionConn = dsPool.getConnection()
      val partitionStmt = partitionConn.createStatement()


      var num = 1;
      var infoMap = new mutable.ListMap[String, ListBuffer[(Long, ListBuffer[Long])]]

      partition.foreach {
        row =>
          if (num % 20000 == 0) {
            //            println(sqlExecute.toString())
            val sql = "INSERT INTO "
            var sqlExecute = new StringBuilder(sql)
//            println(infoMap)
            infoMap.foreach(info => {
              val tableName = info._1
              sqlExecute.append(tableName + "_1_2 ")
              sqlExecute.append("USING " + tableName + " TAGS (2,\"Datalogger1\")")
              sqlExecute.append(" VALUES ")
              info._2.foreach(detail => {
                val timestamp = detail._1
                sqlExecute.append("(")
                sqlExecute.append(timestamp.toString + ",")
                detail._2.foreach(ele => {
                  sqlExecute.append(ele + ",")
                })
                sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")
              })
//              sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")
            })

//            println(sqlExecute)
            partitionStmt.executeUpdate(sqlExecute.toString)
            infoMap.clear()
//            println(num)
//            partitionStmt.executeUpdate(sqlExecute.toString())
          }
          val ts = row.getAs[Long]("ts")
          val id = row.getAs[Int]("id")
          val table = dbcMap(id)
//          sqlExecute.append(table + "_1_2")
//          sqlExecute.append(" VALUES (")
//
//          sqlExecute.append(ts.toString + ",")

          val dataMap = row.getAs[Map[String, Long]]("map")
          val tableList = dbcInfo(table)

          val dataList = new ListBuffer[Long]

          tableList.foreach(ele => {
            dataList += dataMap(ele)
          })
          if (!infoMap.contains(table)) {
            val listBuffer = new ListBuffer[(Long, ListBuffer[Long])]
            val tmpTuple = (ts, dataList)
            listBuffer += tmpTuple
            infoMap += (table -> listBuffer)
          } else {
            val tmpTuple = (ts, dataList)
            infoMap(table) += tmpTuple
          }
          //          infoMap += (table -> dataList)
          //          dataMap.map(ele => {
          //            dbcInfo(ele._1)
          //          })

          //          val rowValues = row.getAs[Map[String, Long]]("map").values
//          dataMap.values.foreach(value => sqlExecute.append(value + ","))
//          sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")
          num += 1
      }

      val sql = "INSERT INTO "
      var sqlExecute = new StringBuilder(sql)
      //            println(infoMap)
      infoMap.foreach(info => {
        val tableName = info._1
        sqlExecute.append(tableName + "_1_2")
        sqlExecute.append(" VALUES ")
        info._2.foreach(detail => {
          val timestamp = detail._1
          sqlExecute.append("(")
          sqlExecute.append(timestamp.toString + ",")
          detail._2.foreach(ele => {
            sqlExecute.append(ele + ",")
          })
          sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")
        })
        //              sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")
      })
      partitionStmt.executeUpdate(sqlExecute.toString)


      //      partitionStmt.executeUpdate(sqlExecute.toString())
      dsPool.evictConnection(partitionConn)
    })
  }
}

object CanTraceHexToTD {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceHexToTD
    model.executeModel()
  }
}

