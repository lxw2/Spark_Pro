package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_02_flatmap {

  def main(args: Array[String]): Unit = {
  //创建spark环境
  val conf: SparkConf = new SparkConf().setAppName("AC").setMaster("local[1]")
  val sc: SparkContext = new SparkContext(conf)

    //调用简单的flatmap算子
    Simple_flatmap(sc)
  }

  def Simple_flatmap(sc:SparkContext) ={
    //创建数据源

    val numlist: RDD[Int] =sc.parallelize(List(1,23,4,5,67,3,6,65,23))
    val mapSeq: RDD[Map[String, Int]] = sc.parallelize(List(Map("zhansan"->3),Map("lisi"->3),Map("wangwu"->3),Map("zhaoliu"->3),Map("qiantu"->3)))
    val tupleSeq = sc.parallelize(List((1,2,"as"),(1,2,"as"),(1,"sd","as"),("aa",2,"as"),(1,34,"as")))


    //针对不同的集合 flatMap 的效果
//    numlist.flatMap(_)//.foreach(println(_))
    val mapdemo: RDD[(String, Int)] = mapSeq.flatMap(x=>x)//.foreach(println(_)) RDD[Map[String, Int]] --> RDD[(String, Int)]

  }



}


