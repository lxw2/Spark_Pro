package it.luke.utils.spark_untils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_01_metrics {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestCreateDirectStream")
    conf.set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource") //开启集群监控功能

    //创建sparkContext
    val sc = new SparkContext(conf)

    //简单是wordCount
    //创建数据源
    val sourRdd: RDD[String] = sc.parallelize(Seq("hadoop","spark","flume","hbase","spark","spark","hbase","hadoop"))

    //进行wordcount
    val Maprdd: RDD[(String, Int)] = sourRdd.map(item=>(item,1))
    val arr = Maprdd.groupBy(_._1).map(item=>{
      (item._1,item._2.size)
    }).foreach(println(_))



    Thread.sleep(1000000000)



  }

}
