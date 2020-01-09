package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_06_sample {


  def main(args: Array[String]): Unit = {

    //创建一个spark环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("sample")
    //创建一个sparkcontext
    val sc = new SparkContext(conf)

    simple_sample(sc)
  }

  def simple_sample(sc: SparkContext): Unit = {

    //创建数据源
    val listRdd: RDD[Int] = sc.parallelize(Seq(1, 1, 1, 1, 5, 6, 7, 8, 9, 10))

    //调用方法
    //参数:
    //      withReplacement: Boolean, 允许返回
    //      fraction: Double, 期望
    //      seed: Long = Utils.random.nextLong  随机种子
    val res: RDD[Int] = listRdd.sample(false,0.6)
    res.foreach(println(_))

  }
}
