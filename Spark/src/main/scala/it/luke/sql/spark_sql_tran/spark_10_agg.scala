package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession

object spark_10_agg {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().master("local[4]").appName("filter")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark_agg(spark)
  }

  def spark_agg(spark:SparkSession): Unit ={

    import spark.implicits._
    val data = Seq(Person("zhangsan",20),Person("lisi",50),Person("wangwu",50),Person("zhangsan",30)).toDS()
    //需求:根据年龄分组，获取年龄平均值
    data.groupBy('name).avg("age").show
    import org.apache.spark.sql.functions._
    //需求:根据年龄分组，获取年龄平均值,年龄的总值
    data.groupBy('name).agg(avg("age"),sum("age")).show
  }
}
