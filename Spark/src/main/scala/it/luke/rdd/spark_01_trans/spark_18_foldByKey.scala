package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_18_foldByKey {


  def main(args: Array[String]): Unit = {

    //创建spark 环境
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("foldBykey")
    val sc: SparkContext = new SparkContext(conf)

    //简单的调用方法
    simple_foldbykey(sc)
  }

  def simple_foldbykey(sc: SparkContext): Unit = {

    //创建数据源
    val sourRdd: RDD[(String, Int)] = sc.parallelize(Seq(("zhansan", 1), ("lisi", 1), ("wangwu", 1), ("zhangsan", 2), ("li", 4), ("zhansan", 1)))

    //和reducebykey 的区别是 可以添加初始值
    val resfold: Array[(String, Int)] = sourRdd.foldByKey(10)((curr, agg) =>
      curr + agg
    ).collect()
    resfold.foreach(println(_))

  }

}
