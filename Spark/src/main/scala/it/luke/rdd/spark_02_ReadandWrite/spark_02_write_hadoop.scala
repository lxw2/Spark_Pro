package it.luke.rdd.spark_02_ReadandWrite

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_02_write_hadoop {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hadoop").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sourcd: RDD[(Int, String)] = sc.parallelize(Seq((1->"lisi")))
//    sourcd.saveAsNewAPIHadoopFile()

  }

}
