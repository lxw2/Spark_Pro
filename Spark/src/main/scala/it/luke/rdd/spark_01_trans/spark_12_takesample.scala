package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_12_takesample {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("takesample").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_takesample(sc)
  }
  def simple_takesample(sc: SparkContext): Unit = {

    //创建数据源
    val listRDD: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7))

    val res = listRDD.takeSample(false,2)

    res.foreach(println(_))

  }

}
