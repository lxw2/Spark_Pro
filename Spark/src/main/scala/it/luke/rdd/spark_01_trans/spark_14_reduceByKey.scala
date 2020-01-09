package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_14_reduceByKey {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setAppName("intersection").setMaster("local[4]")
    //创建sparkcontext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val currenttime: Long = System.currentTimeMillis()
    //简单的调用
    //simple_reducebykey(sc)

    //用reducebykey进行分组去重
    //distinct_reducebykey(sc)

    //字符串拼接
    Strcontact_reducebykey(sc)
    val endtime: Long = System.currentTimeMillis()
    val intervaltime = endtime - currenttime
    println(intervaltime)


  }

  def simple_reducebykey(sc: SparkContext): Unit = {

    val data: Seq[(Int, String, String, Int)] = Seq[(Int, String, String, Int)](
      (1, "aa", "shenzhen", 13),
      (2, "bb", "shanghai", 15),
      (7, "tt", "beijing", 34),
      (7, "tt", "beijing", 23),
      (3, "cc", "shenzhen", 17),
      (4, "dd", "shenzhen", 12),
      (5, "ee", "shanghai", 20),
      (6, "ff", "shanghai", 30),
      (7, "tt", "beijing", 26),
      (7, "tt", "beijing", 28)
    )

    val datardd: RDD[(Int, String, String, Int)] = sc.parallelize(data, 3)

    //转化成kv
    val kvrdd: RDD[(String, Int)] = datardd.map(item => (item._3, item._4))

    val res: RDD[(String, Int)] = kvrdd.reduceByKey(_ + _)
    val resArr: RDD[Array[(String, Int)]] = res.glom()
    resArr.foreach(_.foreach(println(_)))
  }

  def distinct_reducebykey(sc: SparkContext) = {

    val data: Seq[(Int, String, String, Int)] = Seq[(Int, String, String, Int)](
      (1, "aa", "shenzhen", 13),
      (2, "bb", "shanghai", 15),
      (7, "tt", "beijing", 34),
      (7, "tt", "beijing", 23),
      (3, "cc", "shenzhen", 17),
      (4, "dd", "shenzhen", 12),
      (5, "ee", "shanghai", 20),
      (6, "ff", "shanghai", 30),
      (7, "tt", "beijing", 26),
      (7, "tt", "beijing", 28)
    )

    val datardd: RDD[(Int, String, String, Int)] = sc.parallelize(data, 2)

    val kvrdd = datardd.map(item => (item._3, item._4)).reduceByKey((a, b) => b)
      .foreach(println(_))
  }

  /**
    * 但下述代码的问题是，RDD的每个值都将创建一个Set，
    * 如果处理一个巨大的RDD,这些对象将大量吞噬内存，并且对垃圾回收造成压力。
    * @param sc
    */
  def Strcontact_reducebykey(sc: SparkContext) = {

    //通过创建set的方法来实现拼接,但是如果rdd 比较大的话,每个数据都要创建一个set对象这是非常巨大的内存损耗
    val userAccesses = sc.parallelize(Array(("u1", "site1"), ("u2", "site1"), ("u1", "site1"), ("u2", "site3"), ("u2", "site4")))
    val mapSet: RDD[(String, Set[String])] = userAccesses.map(item=>(item._1,Set(item._2)))
    val res = mapSet.reduceByKey(_++_)

    res.foreach(println(_))

  }
}
