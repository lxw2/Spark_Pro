package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_13_mapvalue {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("takesample").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_mapvalue(sc)
  }

  /**
    * keyvalue 针对key value对的算子
    * 不针对key,直接对value进行操作
    * @param sc
    */
  def simple_mapvalue(sc: SparkContext): Unit = {

    //正常的键值对
    val tuplerdd: RDD[(Int, String)] = sc.parallelize(Seq((1,"zhansan"),(2,"lisi"),(3,"wangwu")))
    //三元组
    val tuplerdd1: RDD[(Int, Int, String)] = sc.parallelize(Seq((1,2,"zhansan"),(2,2,"lisi"),(3,2,"wangwu")))

    tuplerdd.mapValues(item=>{

      val a: String = item + "我是额外添加的"
      a
    }).foreach(println(_))

    //通过map转换成键值对,对value进行操作
    tuplerdd1.map(item=>{
      (item._1+item._2,item._3)
    }).mapValues(_+"add").foreach(println(_))
  }
}
