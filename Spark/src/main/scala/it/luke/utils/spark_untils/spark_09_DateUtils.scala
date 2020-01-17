package it.luke.utils.spark_untils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

object spark_09_DateUtils {
  /**
    * 获取当天的yyyyMMdd格式的日期字符串
    */
  def getNow()={
    //1、获取当前日期
    val date = new Date()
    //2、定义格式化
    val formatter = FastDateFormat.getInstance("yyyyMMdd")
    //3、格式化
    formatter.format(date)
  }

  /**
    * 获取昨天的yyyyMMdd格式的字符串
    */
  def getYesterDay()={
    //1、获取当前时间
    val date = new Date()
    //2、在当前时间的基础上-1天
    val calendar = Calendar.getInstance()
    calendar.setTime(date)

    calendar.add(Calendar.DAY_OF_YEAR,-1)
    //3、格式化时间
    val formatter  = FastDateFormat.getInstance("yyyyMMdd")
    formatter.format(calendar)
  }
}
