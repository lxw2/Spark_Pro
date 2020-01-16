package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark._

object spark_27_partitionBy {

  def main(args: Array[String]): Unit = {

    //创建spark 环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("partitionBy")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

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
