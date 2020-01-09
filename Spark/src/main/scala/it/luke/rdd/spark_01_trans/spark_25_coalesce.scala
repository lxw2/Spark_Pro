package it.luke.rdd.spark_01_trans

import org.apache.spark.{SparkConf, SparkContext}

object spark_25_coalesce {


  def main(args: Array[String]): Unit = {

    //创建spark环境
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("WARN")

    simple_coalesce(sc)

  }
  def simple_coalesce(sc: SparkContext): Unit = {

    val rdd = sc.parallelize(Seq(("a", 3), ("b", 2), ("c", 1),("d", 5), ("e", 2)))
    val oldNum = rdd.partitions.length

    println(oldNum)
    rdd.mapPartitionsWithIndex((index,iter)=>{

      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))

    //调用
    println("=============")
    rdd.coalesce(3).mapPartitionsWithIndex((index,iter)=>{

      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))

  }

}
