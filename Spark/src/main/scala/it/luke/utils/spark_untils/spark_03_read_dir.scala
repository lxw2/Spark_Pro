package it.luke.utils.spark_untils

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

object spark_03_read_dir {

  def main(args: Array[String]): Unit = {

    //获取hdfs 的文件系统
    val a: FileSystem = FileSystem.get(new URI("hdfs://node01:8020/user"), new Configuration(), "root")
    //调用方法
    val arr: List[String] = get_info_dir(a, new Path("hdfs://node01:8020/user/hive/warehouse/itcast_ods.db/ods_weblog_origin"))

    //通过过滤的方法将小于30天数据过滤出来
    val strings: List[String] = filter_day(arr,"30")

  }

  def get_info_dir(fs: FileSystem, path: Path) = {
    //获取路径中文件/目录的状态
    val b: Array[FileStatus] = fs.listStatus(path)

    //将文件添加到集合
    val arr = new ArrayBuffer[String]()
    b.map(item => {
      val name: String = item.getPath.getName
      arr += (name)
    })
    //    arr.foreach(println(_))
    arr.toList
  }

  def filter_day(arr: List[String],day:String) = {
      arr.filter(item => {
        val daylong = day.toLong
        val date = new Date()
        val currentTime: Long = date.getTime
        //调用方法进行判断是否小于30天
        if (diff_time(item, currentTime) < daylong * 60 * 60 * 24 * 1000l) {
          //        println("true")
          true
        }
        else {
          //        println("false")
          false
        }
      })
  }

  def diff_time(targetTime: String, currentTime: Long) = {

    //    val format: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val format = new SimpleDateFormat("yyyyMMdd")
    //获取时间差

    val arr = targetTime.split("=")
    val tarTime = arr(1)
    val str: String = format.format(currentTime)


    val tarlong: Long = format.parse(tarTime).getTime
    val currentLong: Long = format.parse(str).getTime

    val diff: Long = (currentLong - tarlong)
    //    println(diff) //8726400000
    diff


  }

}
