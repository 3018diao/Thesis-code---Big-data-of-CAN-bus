package com.zdautomotive.cloud.utils

import java.nio.ByteBuffer
import java.util.regex.Pattern

import com.zdautomotive.cloud.HexToAsc.{convert2ASC, parse, parseBufferFrameHeader}
import com.zdautomotive.cloud.etl.{DataCAN, DataCAN_1, DataCAN_Date, DataCS, DataHeader, DataTrace}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CANTools {

  def getCANData(path: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val frame = sc.textFile(path)
      .filter(
        _.startsWith(" ")
      ).map(
      value => {
        val list = new ListBuffer[String]
        val regex = "(([0-9]{1,}[.][0-9]*)+)|(\\w+)|([RrTt][xX])|[Dd]|((\\s[A-F0-9]{2}){2,}+)"
        val m = Pattern.compile(regex).matcher(value)
        while (m.find()) {
          list += m.group()
        }
        val module = list(1).toShort
        //        val timestamp = list(0).toFloat * 1000

        val ts = Long.box((list(0).toDouble * Math.pow(10, 9)).toLong)
        val id = Integer.parseInt((if (list(2).endsWith("x")) list(2).substring(0, list(2).length - 1) else list(2)), 16)
        val dir = list(3)
        val len = list(5).toShort
        val data = list(6).trim()
        DataCAN(module, ts, id, dir, len, data)
      }
    ).toDS()
      .orderBy("ts")
      .toDF()
    //    frame.show(100, false)
    frame
  }

  def getHexTrace(path: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    val frame = spark.read
      .format("binaryFile")
      .load(path)
    frame.show(5)
    val head_size = 12
    import org.apache.spark.sql.functions._
    import spark.implicits._

    frame.flatMap(ele => {
      val DataBuffer_StartIdx = head_size
      val arrayByte = ele.get(3).asInstanceOf[Array[Byte]]
      var buffer = ByteBuffer.wrap(arrayByte)
      var canList = new ListBuffer[DataCAN_1]

      while (buffer.capacity() >= head_size) {
        val header = parseBufferFrameHeader(buffer)
        //        println(header.toString)
        val length = header.length

        buffer.position(DataBuffer_StartIdx)
        buffer.limit(DataBuffer_StartIdx + length)
        //        println("--------------------")
        val dataBuffer = buffer.slice()
        val dataTrace = parse(dataBuffer)

        val trace = convert2ASC(dataTrace, header)

        canList += trace

        //        println(dataTrace.toString)
        buffer.position(DataBuffer_StartIdx + length)
        buffer.limit(buffer.capacity())
        buffer = buffer.slice()
      }
      canList
    }).orderBy( "ts")
      .toDF()
  }

  def getHexTrace_clone(path: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    val frame = spark.read
      .format("binaryFile")
      .load(path)

    val head_size = 12
    import spark.implicits._

    frame.flatMap(ele => {
      val DataBuffer_StartIdx = head_size
      val arrayByte = ele.get(3).asInstanceOf[Array[Byte]]
      var buffer = ByteBuffer.wrap(arrayByte)
      var canList = new ListBuffer[DataCAN_1]

      while (buffer.capacity() >= head_size) {
        val header = parseBufferFrameHeader(buffer)
        val length = header.length

        buffer.position(DataBuffer_StartIdx)
        buffer.limit(DataBuffer_StartIdx + length)
        val dataBuffer = buffer.slice()
        val dataTrace = parse(dataBuffer)
        val trace = convert2ASC(dataTrace, header)
        for (i <- 199 to 209) {
          val ts: Long = trace.ts + (86400 * 1000000L) * i
          canList += DataCAN_1(trace.module, ts, trace.id, trace.data)
        }
        buffer.position(DataBuffer_StartIdx + length)
        buffer.limit(buffer.capacity())
        buffer = buffer.slice()
      }
      canList
    }).orderBy("id", "ts")
      .toDF()
  }

  def parseCANData(dbcDF: DataFrame, canDF: DataFrame): DataFrame = {
    import dbcDF.sparkSession.implicits._
    val parseCAN_udf = udf {
      (canMsg: String, info: Seq[Row]) => {
        val data = canMsg.split(" ").map(Integer.parseInt(_, 16))
        var details: Map[String, Long] = Map()

        for (row <- info) {
          var value = 0
          val name = row.getAs[String](0)
          val len = row.getAs[Int](2)
          var start = row.getAs[Int](1) % 8
          var bitLen = len
          var position = Math.ceil(row.getAs[Int](1) / 8).toInt
          while (bitLen > 0) {
            if (start + bitLen <= 8) {
              val mask = (Math.pow(2, bitLen) - 1).toInt
              value += ((data(position) >> start) & mask) << (len - bitLen)
              start += bitLen
              bitLen = 0
            } else {
              value += (data(position) >> start) << (len - bitLen)
              bitLen -= 8 - start
              start = 8
            }
            if (start == 8) {
              position += 1
              start = 0
            }
          }
          details += (name -> value)
        }
        details
      }
    }

    canDF
      .join(dbcDF, canDF("id") === dbcDF("identifier"), "inner")
      .select($"module", canDF("ts"), canDF("id"), $"data", dbcDF("info"))
      .withColumn("map", parseCAN_udf('data, 'info))
      .drop("info", "module", "data")
  }

  def parseRawCan(dbcDF: DataFrame, rawDF: DataFrame): DataFrame = {
    import dbcDF.sparkSession.implicits._
    val parseCAN_udf = udf {
      (canMsg: Array[Int], info: Seq[Row]) => {
        val details = new Array[Long](info.size)

        var num = 0
        info.zipWithIndex.foreach {
          case (row, i) => {
            var value: Long = 0
            val name = row.getAs[String](0)
            val len = row.getAs[Int](2)
            var start = row.getAs[Int](1) % 8
            var bitLen = len
            var position = Math.ceil(row.getAs[Int](1) / 8).toInt
            while (bitLen > 0) {
              if (start + bitLen <= 8) {
                val mask = (Math.pow(2, bitLen) - 1).toLong
                value += ((canMsg(position) >> start) & mask) << (len - bitLen)
                start += bitLen
                bitLen = 0
              } else {
                value += (canMsg(position) >> start) << (len - bitLen)
                bitLen -= 8 - start
                start = 8
              }
              if (start == 8) {
                position += 1
                start = 0
              }
            }
            details(i) = value
          }
        }
        details
      }
    }

    rawDF
      .join(dbcDF, rawDF("id") === dbcDF("identifier"), "inner")
      .select($"module", rawDF("ts"), rawDF("id"), $"data", dbcDF("info"))
      .withColumn("map", parseCAN_udf('data, 'info))
      .drop("info", "module", "data")
  }

  def convert2ASC(dataTrace: DataTrace, header: DataHeader) = {
    val TIMESTAMP: Long = header.hightTS.toLong * 1000000 + (header.lowTS / 1000).toLong
    val CANID = dataTrace.canid

    val CANBUS = (header.portID + 1).toShort
    val DATA = dataTrace.data.map(ele => ele & 0xff)
    DataCAN_1(CANBUS, TIMESTAMP, CANID, DATA)
  }

  private def parse(buf: ByteBuffer): DataTrace = {
    val canid = buf.getInt(0)
    val dataCS = parseCS(buf.getInt(4))
    buf.position(8)
    val data = buf.slice()
    //    println(data.get(0))
    val dataArr = new Array[Byte](data.remaining())
    data.get(dataArr, 0, dataArr.length)
    //    dataArr.foreach(println)
    val dataTrace = DataTrace(canid, dataCS, dataArr)
    dataTrace
  }

  private def parseCS(cs: Int): DataCS = {
    val EDL = cs >>> 31 & 0x1
    val BRS = cs >>> 30 & 0x1
    val ESI = cs >>> 29 & 0x1
    val _unused_1 = cs >>> 28 & 0x1
    val CODE = cs >>> 24 & 0xF
    val _unused_2 = cs >>> 23 & 0x1
    val SRR = cs >>> 22 & 0x1
    val IDE = cs >>> 21 & 0x1
    val RTR = cs >>> 20 & 0x1
    val DLC = cs >>> 16 & 0xF
    val TIMESTAMP = cs & 0xFFFF
    DataCS(EDL, BRS, ESI, _unused_1, CODE, _unused_2, SRR, IDE, RTR, DLC, TIMESTAMP)
  }

  private def parseBufferFrameHeader(buffer: ByteBuffer): DataHeader = {
    val highTS = buffer.getInt(0)
    val lowTS = buffer.getInt(4)
    val portType = buffer.get(8)
    val portID = buffer.get(9)
    val length = buffer.getShort(10)
    DataHeader(highTS, lowTS, portType, portID, length)
  }
}
