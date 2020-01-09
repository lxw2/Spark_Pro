package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_10_distinct {


  def main(args: Array[String]): Unit = {

    //创建spark环境
    val conf: SparkConf = new SparkConf().setAppName("distinct").setMaster("local[1]")

    //创建context
    val sc: SparkContext = new SparkContext(conf)

    simple_distinct(sc)

  }

  /**
    *
    * @param sc
    */
  def simple_distinct(sc: SparkContext): Unit = {

    //创建数据源
    val listrDD: RDD[Int] = sc.parallelize(Seq(1,2,2,3,3,1,4,3))

    //调用方法
    val distinctrDD: RDD[Int] = listrDD.distinct(1)

    distinctrDD.foreach(println(_))
  }
}
