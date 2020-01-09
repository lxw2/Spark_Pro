package it.luke.rdd.spark_02_ReadandWrite

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_01_textfile {

  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setMaster("local[1]").setAppName("read")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val sourceRDD1: RDD[String] =sc.textFile("E:\\Server\\05_Spark\\spark\\day10\\spark_11(2)\\spark_11\\mytest\\union.txt")
    val sourceRDD2: RDD[String] =sc.textFile("E:\\Server\\05_Spark\\spark\\day10\\spark_11(2)\\spark_11\\mytest\\unionother")

    val head: RDD[(String, String)] = sourceRDD1.filter(line=> line.startsWith("i"))
      .map(line=>{
      val arr = line.split(",")
      val id = arr(0)
      val name = arr(1)
      (id,name)
    })
    val res = sourceRDD1.filter(line=> !line.startsWith("i"))
      .union(sourceRDD2.filter(line=> !line.startsWith("i")))
    val res1: RDD[(String, String)] = res.map(item=>{
      val arr = item.split(",")
      val id = arr(0)
      val name = arr(1)
      (id,name)
    })
    val sortres = res1.sortBy(_._1)
    val res2 = head.union(sortres)
    res2.foreach(println(_))

    res2.saveAsTextFile("E:\\Server\\05_Spark\\spark\\day10\\spark_11(2)\\spark_11\\mytest\\res2")

  }

}
