package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_15_groupByKey {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setAppName("intersection").setMaster("local[4]")
    //创建sparkcontext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val currenttime: Long = System.currentTimeMillis()
    //简单的调用
    simple_groupbykey(sc)
    val endtime: Long = System.currentTimeMillis()
    val intervaltime = endtime - currenttime
    println(intervaltime)

  }
  def simple_groupbykey(sc: SparkContext) = {

    val data: Seq[(Int, String, String, Int)] = Seq[(Int,String,String,Int)](
      (1,"aa","shenzhen",13),
      (2,"bb","shanghai",15),
      (7,"tt","beijing",34),
      (7,"tt","beijing",23),
      (3,"cc","shenzhen",17),
      (4,"dd","shenzhen",12),
      (5,"ee","shanghai",20),
      (6,"ff","shanghai",30),
      (7,"tt","beijing",26),
      (7,"tt","beijing",28)
    )

    val datardd: RDD[(Int, String, String, Int)] = sc.parallelize(data,2)

    val res = datardd.map(item=>(item._3,item._4)).groupByKey().foreach(println(_))
  }

}
