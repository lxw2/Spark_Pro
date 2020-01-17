package it.luke.utils.spark_untils

import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object spark_02_read_Nearly30day {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Nearly30day")
      //配置hive的源数据路径通过远程访问的形式
      .config("hive.metastore.uris", "thrift://node02:9083")
      //通过配置spark
      //.config("spark.sql.warehouse.dir","/user/hive")
      //指定支持hive
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

//    可以通过sql直接访问hive表数据
        spark.sql(
          """
            |select * from
            |spark_integrition.student
          """.stripMargin).show()

//    val day = "300"
//    //使用读取文件系统的方式(从存在的文件中读取)
//    val path: Path = new Path("hdfs://node01:8020/user/hive/warehouse/itcast_ods.db/ods_weblog_origin")
//
//    val pathlist: List[String] = get_nearly30day(path, day)
//    pathlist.foreach(println(_)) //dt=20191001
//    //使用读取hive分区的方式读取近30天的数据
//    import scala.collection.JavaConversions._
//    val listRow: util.List[Row] = get_nearly30day_hive(spark, day)
//    listRow.map(item => {
//      val dt = item.getString(0)
//      val res = s"dt=${dt}"
//      res
//    }).foreach(println(_)) //dt=20191001


  }

//  def get_nearly30day(path: Path, day: String) = {
//
//    //创建文件系统
//    val fs = FileSystem.get(new URI(path.toString), new Configuration())
//
//    //获取子目录路径集合
//    val path_List: List[String] = spark_03_read_dir.get_info_dir(fs, path)
//
//    //进行对集合进行过滤,获得小于30天的结果集
//    val resList: List[String] = spark_03_read_dir.filter_day(path_List, day)
//    resList
//  }
//
//  def get_nearly30day_hive(spark: SparkSession, day: String) = {
//
//    //调用以支持hive的sparkSession进行访问hive表数据
//    //datediff(current_timestamp, dt)
//    val resourceDF: util.List[Row] = spark.sql(
//      s"""
//         |select dt
//         |from itcast_ods.ods_weblog_origin
//         |where
//         |datediff(
//         |from_unixtime(unix_timestamp(),'yyyy-MM-dd'),
//         |from_unixtime(unix_timestamp(dt,'yyyyMMdd'),'yyyy-MM-dd')) <=${day}
//         |group by dt
//      """.stripMargin).collectAsList()
//    resourceDF
//
//
//  }

}
