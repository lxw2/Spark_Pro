package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_07_union {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf: SparkConf = new SparkConf().setAppName("union").setMaster("local[5]")
    conf.set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")//开启集群监控功能
    //创建sparkcontext
    val sc = new SparkContext(conf)

    sample_union(sc)

  }

  def sample_union(sc: SparkContext): Unit = {

    //创建数据源
    val listRdd1: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5))
    val listRdd2: RDD[Int] = sc.parallelize(Seq(6,7,8,9,10))

    //调用union方法 只能针对相同类型
    val res: RDD[Int] = listRdd1.union(listRdd2)
    res.foreach(println(_))
  }
}
