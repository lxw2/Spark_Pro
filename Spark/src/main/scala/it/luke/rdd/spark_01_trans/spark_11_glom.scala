package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_11_glom {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("glom").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_glom(sc)
  }

  /**
    * 将一个分区中的数据添加成一个数组的形式
    * @param sc
    */
  def simple_glom(sc: SparkContext): Unit = {
    //创建数据源
    val listRdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,56))
    val res: RDD[Array[Int]] = listRdd.glom()

    res.foreach(item=>{
      val item1: Array[Int] = item

      println(item1)
    })

  }
}
