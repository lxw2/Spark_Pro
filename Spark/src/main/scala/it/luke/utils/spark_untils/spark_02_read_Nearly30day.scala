package it.luke.utils.spark_untils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

object spark_02_read_Nearly30day {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Nearly30day")
      //      .config("hive.metastore.uris","thrift://node02:9083")
      //      .config("spark.sql.warehouse.dir","/user/hive")
      //      .enableHiveSupport()
      .getOrCreate()

    //    spark.
    val res = spark.read.load("hdfs://node01:8020/user/hive")
    res.show()
    //      read().textFile(filePath)

  }

  def get_nearly30day(path: Path) = {

    //创建文件系统
    val fs = FileSystem.get(new URI(path.toString), new Configuration())

    //获取子目录路径集合
    val path_List: List[String] = spark_03_read_dir.get_info_dir(fs, path)

    //进行对集合进行过滤,获得小于30天的结果集
    val resList: List[String] = spark_03_read_dir.filter_day(path_List)
    resList
  }

}
