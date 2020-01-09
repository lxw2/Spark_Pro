package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_21_cogroup {


  def main(args: Array[String]): Unit = {

    //创建sparkContext
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("cogroup")
    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("WARN")
    simple_cogroup(sc)

  }

  /**
    * rdd…​ 最多可以传三个 RDD 进去, 加上调用者, 可以为四个 RDD 协同分组
    * partitioner or numPartitions 可选, 可以通过传递分区函数或者分区数来改变分区
    *
    * 是join 等自动识别相同key的方法的基础
    *
    * @param sc
    */
  def simple_cogroup(sc: SparkContext): Unit = {

    //创建数据源
    val sourceRDD1: RDD[(String, Int)] = sc.parallelize(Seq(("zhansan",1),("lisi",2),("zhansan",3)))
    val sourceRDD2: RDD[(String, Int)] = sc.parallelize(Seq(("zhansan",1),("lisi",2),("zhaoliu",3)))
    val sourceRDD3: RDD[(String, Int)] = sc.parallelize(Seq(("zhaoliu",1),("lisi",2),("lisi",3)))
    val sourceRDD4: RDD[(String, Int)] = sc.parallelize(Seq(("zhaoliu",1),("zhaoliu",2),("zhansan",3)))


    sourceRDD1.cogroup(sourceRDD2).foreach(println(_))
    println("==============")
    sourceRDD1.cogroup(sourceRDD2,sourceRDD3).foreach(println(_))
    println("==============")
    sourceRDD1.cogroup(sourceRDD2,sourceRDD3,sourceRDD4).foreach(println(_))
    println("==============")

  }

}
