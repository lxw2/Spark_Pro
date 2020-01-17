package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark._

object spark_24_partitionBy {



  def main(args: Array[String]): Unit = {

    //创建spark环境
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("WARN")
    //调用方法
    simple_partitionBy(sc)

    anx_partitionBy(sc)
  }

  def simple_partitionBy(sc: SparkContext): Unit = {

    //创建数据源
    val student = sc.parallelize(Seq[(Int, String, Double, String)](
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_05")
    ))

    //进行分区展示
    //0,ArrayBuffer((1,aa,55.0,class_01))
    //1,ArrayBuffer((2,bb,85.0,class_02), (3,cc,90.0,class_02))
    //2,ArrayBuffer((4,dd,75.5,class_01))
    //3,ArrayBuffer((5,ee,68.0,class_01), (6,ff,65.0,class_05))
    student.mapPartitionsWithIndex((index, iter) => {
      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))

    println("==========")
    val mapstudent: RDD[(String, Double)] = student.map(item => (item._4, item._3))
        mapstudent.mapPartitionsWithIndex((index, iter) => {
      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))
    println("==========")
    mapstudent.partitionBy(new HashPartitioner(4)).mapPartitionsWithIndex((index, iter) => {
      println(s"${index},${iter.toBuffer}")
      iter
    }).foreach(println(_))
    println("==========")



  }

  /**
    * 通过测试得出
    * 单数据类型的rdd 没有分区策略
    * 默认只有进行转换算子之后才会分区
    * parititonBy是针对kv类型的
    * @param sc
    */
  def anx_partitionBy(sc: SparkContext): Unit = {
    //创建数据源
    val source: RDD[Int] = sc.parallelize(Seq(1, 2, 4, 5, 6, 7, 8))
    //kv数据源
    val kvsource = sc.parallelize(Seq(("zhansan",1), ("lisi",1)))

    //获取分区
    //单类型数据和KV在创建的时候并没有指定分区类型,所以分区类型为None
    println(source.partitioner.getOrElse("单类型数据没有"))
    val partitioner: Option[Partitioner] = kvsource.partitioner
    println(partitioner.getOrElse("KV数据类型没有"))
    kvsource.partitionBy(new HashPartitioner(100)).persist()
    val partitioner1: Option[Partitioner] = kvsource.partitioner
    println(partitioner1.getOrElse("KV数据类型没有"))


    val maprdd: RDD[Int] = source.map(item=>{item})
    println(maprdd.partitioner.getOrElse("单类型数据没有"))

    //KV类型的数据在调用具有shuffle算子的时候,默认调用hashpartition
    val unit: RDD[(String, Int)] = kvsource.reduceByKey(_+_)
    println(unit.partitioner.getOrElse("KV数据类型没有"))
    //KV类型的数据可以通过调用partitionBy的方法,改变分区测略
    val value = unit.partitionBy(new RangePartitioner(12,unit))
    println(value.partitioner.getOrElse("KV数据类型没有"))


  }

}
