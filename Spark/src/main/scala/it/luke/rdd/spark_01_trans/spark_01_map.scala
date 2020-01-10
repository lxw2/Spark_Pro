package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object spark_01_map {

  def main(args: Array[String]): Unit = {
    //构建spark rdd 环境
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("abc")
    val sc: SparkContext = new SparkContext(conf)


    val a = new mutable.HashMap[String,Int]()
    //在map中传入def function
//    fun_map(sc)
    //用map进行迭代集合内容
    for_map(sc)

  }

  def fun_map(sc: SparkContext) = {
    //创建源数据
    val listnumRDD: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val listChRDD: RDD[String] = sc.parallelize(List("a", "b", "c", "d", "e", "f"))
    //调用map算子
    val res01: RDD[Unit] = listChRDD.map(println(_))
    //println --> def println(x: Any) = Console.println(x)直接传入方法
    val res02: RDD[Unit] = listnumRDD.map(println(_))


    //调用action算子
    println(sc.startTime)

    res01.count() //一个action 对应执行对应的调用的rdd 对象
    res02.count()
  }


  def for_map(sc: SparkContext) = {
    //创建源数据
    val listnumRDD: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val listChRDD: RDD[String] = sc.parallelize(List("a", "b", "c", "d", "e", "f"))
    //调用算子,直接进行操作

    listChRDD.map(item => {
      val res02: String = item + "你猜我猜你猜不猜我猜不猜你猜我"
      res02
    }).foreach(println(_))
    listnumRDD.map(item => {
      val res = item + 10
      res
    }).foreach(println(_)) //通过foreach 作为action 算子进行迭代
  }
}
