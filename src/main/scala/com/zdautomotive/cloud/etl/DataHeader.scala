package com.zdautomotive.cloud.etl

case class DataHeader(
                     hightTS: Int,
                     lowTS: Int,
                     portType: Byte,
                     portID: Byte,
                     length: Short
                     )
