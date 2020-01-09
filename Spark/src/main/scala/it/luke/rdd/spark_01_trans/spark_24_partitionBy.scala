package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object spark_24_partitionBy {


  def main(args: Array[String]): Unit = {

    //创建spark环境
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("WARN")
    //调用方法
    simple_partitionBy(sc)
  }

  def simple_partitionBy(sc: SparkContext): Unit = {

    //创建数据源
    val student = sc.parallelize(Seq[(Int, String, Double, String)](
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_05")
    ))

    //进行分区展示
    //0,ArrayBuffer((1,aa,55.0,class_01))
    //1,ArrayBuffer((2,bb,85.0,class_02), (3,cc,90.0,class_02))
    //2,ArrayBuffer((4,dd,75.5,class_01))
    //3,ArrayBuffer((5,ee,68.0,class_01), (6,ff,65.0,class_05))
    student.mapPartitionsWithIndex((index, iter) => {
      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))

    println("==========")
    val mapstudent: RDD[(String, Double)] = student.map(item => (item._4, item._3))
        mapstudent.mapPartitionsWithIndex((index, iter) => {
      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))
    println("==========")
    mapstudent.partitionBy(new HashPartitioner(4)).mapPartitionsWithIndex((index, iter) => {
      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))
    println("==========")



  }

}
