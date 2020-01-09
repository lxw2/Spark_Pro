package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_08_intersection {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setAppName("intersection").setMaster("local[1]")
    //创建sparkcontext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_inserection(sc)
  }

  def simple_inserection(sc: SparkContext): Unit = {

    //创建数据源
    val listRDD1: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7,8))
    val listRDD2: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5))

    val res: RDD[Int] = listRDD1.intersection(listRDD2)
    res.foreach(println(_))
  }
}
