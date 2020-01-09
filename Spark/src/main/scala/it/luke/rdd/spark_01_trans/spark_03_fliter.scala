package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_03_fliter {
  def main(args: Array[String]): Unit = {

    //创建Spark环境
    val conf: SparkConf = new SparkConf().setAppName("aaa").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("WARN")
    //调用简单的过滤算子
    Simple_Filter(sc)
  }

  def Simple_Filter(sc:SparkContext) ={

    //获取数据源
    val numList: RDD[Int] = sc.parallelize(Seq(1,23,34,5,6,7,34,44,5,76,8778))

    //调用filter算子
    numList.filter(x=>true)//需要传入boolean
    .foreach(println(_))

    println("=================")
    numList.filter(x=>false)  //filter true 的会留下来 false 的会被过滤掉
    println("=================")

    numList.filter(x=>{ //嵌套简单的内部判断
      if (x>20)
        {
          true
        }
      else
        false
    }).foreach(println(_))
  }

}
