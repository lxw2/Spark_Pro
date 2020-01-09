package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_22_cartesian {


  def main(args: Array[String]): Unit = {
  //创建spark 环境
  val conf: SparkConf = new SparkConf().setAppName("leftJoin").setMaster("local[4]")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")


    simple_cartesian(sc)
  }

  /**
    * 并不是按照key进行的笛卡尔积,而是两个rdd 的元素 进行笛卡尔积
    * @param sc
    */
  def simple_cartesian(sc: SparkContext): Unit ={

    //创建数据源
    val personRDD: RDD[(Int, String, Double, String)] = sc.parallelize(Seq(
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_05")
    ))
    val classRDD: RDD[(String, String)] = sc.parallelize(Seq(
      ("class_01", "java基础班"),
      ("class_02", "大数据"),
      ("class_03", "python基础班")
    ))
    val carRDD: RDD[((Int, String, Double, String), (String, String))] = personRDD.cartesian(classRDD)

    carRDD.foreach(println(_))


  }


}
