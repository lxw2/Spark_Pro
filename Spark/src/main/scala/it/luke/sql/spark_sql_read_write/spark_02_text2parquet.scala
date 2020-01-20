package it.luke.sql.spark_sql_read_write

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object spark_02_text2parquet {

  def main(args: Array[String]): Unit = {

    //创建sparksession
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("text2parquet").getOrCreate()
    val sc= spark.sparkContext
    //不生成success 文件
        sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //读取数据
    val source: Dataset[String] = spark.read.textFile("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\unionother")

    //写成parquet..默认
    source.write.save("D:\\GitPro\\Spark_Pro\\Spark\\myparquet\\myparquet01")


  }

}
