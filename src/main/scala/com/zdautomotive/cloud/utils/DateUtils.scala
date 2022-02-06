package com.zdautomotive.cloud.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  def getTodayDate(): String = {
    val nowDate = new Date()

    FastDateFormat.getInstance("yyyy-MM-dd").format(nowDate)
  }

}
