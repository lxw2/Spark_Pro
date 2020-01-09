package it.luke.rdd.spark_01_trans

import org.apache.spark.{SparkConf, SparkContext}

object spark_23_sortBy {


  def main(args: Array[String]): Unit = {

    //创建spark 环境
    val conf: SparkConf = new SparkConf().setAppName("leftJoin").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_sortBy(sc)
  }

  /**
    * 根据指定的元素进行排序
    * @param sc
    */
  def simple_sortBy(sc: SparkContext): Unit = {

    val student = sc.parallelize(Seq[(Int, String, Double, String)](
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_05")
    ))

    student.sortBy(_._1,false).foreach(println(_))

  }
}
