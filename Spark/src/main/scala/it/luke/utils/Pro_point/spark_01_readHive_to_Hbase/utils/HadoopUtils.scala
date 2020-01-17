package it.luke.utils.Pro_point.spark_01_readHive_to_Hbase.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HadoopUtils {

  def main(args: Array[String]): Unit = {

    //测试
    dir_Exists("hdfs://node01:8020/export/hiveoutdata/")

  }


  def dir_Exists (path :String) ={

    //删除已存在的hdfs 文件
    val fileSystem = FileSystem.get(new URI(path),new Configuration() )

    fileSystem.deleteOnExit(new Path(path))

  }
}
