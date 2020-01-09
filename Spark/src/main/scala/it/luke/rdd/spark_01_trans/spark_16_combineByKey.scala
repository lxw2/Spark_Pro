package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object spark_16_combineByKey {


  def main(args: Array[String]): Unit = {

    //创建spark环境
    val conf = new SparkConf().setAppName("intersection").setMaster("local[4]")
    //创建sparkcontext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")


    //简单调用方法使用

    simple_combineBykey(sc)
  }

  /**
    * 将数据处理分为三个阶段
    * 阶段一,数据的初始化,对kv值的value进行初始化
    * 阶段二,作用在分区中,参数是当前值以及上个处理结果值(如果无则为初始值)
    * 阶段三,针对每个分区的聚合结果的聚合
    * @param sc
    */
  def simple_combineBykey(sc: SparkContext): Unit = {

    //添加一个累加器,用于调试分析
    val first_num: LongAccumulator = sc.longAccumulator("num")
    val sec_num: LongAccumulator = sc.longAccumulator("num")
    val th_num: LongAccumulator = sc.longAccumulator("num")
    val rdd1: RDD[(String, Double)] = sc.parallelize(Seq(
      ("zhangsan", 10),
      ("lisi", 25),
      ("zhangsan", 20),
      ("lisi", 35),
      ("zhangsan", 30),
      ("lisi", 45),
      ("zhangsan", 40),
      ("lisi", 55),
      ("zhangsan", 50),
      ("lisi", 65),
      ("zhangsan", 60),
      ("lisi", 75)
    ))

    //添加数据源
    rdd1.mapPartitionsWithIndex((index, iter) => {

      println(s"${index},iter(${iter.toBuffer})")
      iter
    }).count()

    /**
      * createCombiner: V => C,  针对每个分区每个key的第一个value值进行转换
      * mergeValue: (C, V) => C, 针对每个分区的每个key的value值进行combiner
      * mergeCombiners: (C, C) => C 针对每个分区combiner结果进行reduce
      */

      /*
      * 第一个函数的调用次数 = 分区个数
      * 第二个函数的调用次数为1(因为是作用在各自分区内部的)
      * 第三个分区调用次数 = 累加次数
      * */
    val res = rdd1.combineByKey((value: Double) => {
      first_num.add(1)
      println(s"第一个函数${value},${first_num.value}")
      (value, 1)
    },
      (agg: (Double, Int), curr: Double) => {
        sec_num.add(1)
        println(s"第二个函数${agg}, and ${curr} , and ${sec_num.value}")
        (agg._1 + curr, agg._2 + 1)
      },
      (aa: (Double, Int), bb: (Double, Int)) => {
        th_num.add(1)
        println(s"第三个函数${aa} ,  and ${bb} , and ${th_num.value}")
        (aa._1 + bb._1, aa._2 + bb._2)
      })
    res.count()
    res.foreach(println(_))

  }
}
