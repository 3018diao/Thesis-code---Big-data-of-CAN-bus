package com.zdautomotive.cloud.tools

object DataTools {
  def main(args: Array[String]): Unit = {
    val canMsg = "10 50 01 2C 00 29 01 FC"
    val data = canMsg.split(" ").map(Integer.parseInt(_, 16))
//    val data = Array(16, 80, 1, 44, 0, 41, 1, 252)

    val result = getDataByBits(data, 31, 1)
    println(result)
  }

  def getDataByBits(data: Array[Int], startBitInByte: Int, len: Int, isBigEndian: Boolean = false): Int = {
    var ret = 0
    var bitLen = len
    var start = startBitInByte % 8
    var position = Math.ceil(startBitInByte / 8).toInt

    while (bitLen > 0) {
      if (start + len <= 8) {
        val mask = (Math.pow(2, bitLen) - 1).toInt
        ret += ((data(position) >> start) & mask) << (len - bitLen)
        bitLen = 0
      } else {
        ret += (data(position) >> start) << (len - bitLen)
        bitLen -= 8 - start
        start = 8
      }

      if (start == 8) {
        position += 1
        start = 0
      }
    }
    ret
  }
}
