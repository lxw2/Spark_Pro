package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_09_subtract {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("subtract")
    val sc: SparkContext = new SparkContext(conf)

    simple_subtract(sc)

  }

  /**
    * 简单的方法
    * @param sc
    */
  def simple_subtract(sc: SparkContext): Unit = {

    //创建数据源
    val listRdd1: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7))
    val listRdd2: RDD[Int] = sc.parallelize(Seq(1,2,3,4))

    //调用方法
    val res: RDD[Int] = listRdd1.subtract(listRdd2)

    res.foreach(println(_))

  }
}
