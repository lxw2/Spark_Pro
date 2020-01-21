package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession

object spark_08_distinct {

  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark: SparkSession =SparkSession.builder().appName("as").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //调用方法

    spark_distinct(spark)

  }
  def spark_distinct(spark: SparkSession) = {

    import spark.implicits._
  val data = Seq(Person("zhangsan",20),Person("zhangsan",20),Person("lisi",50),Person("wangwu",50),Person("zhangsan",30)).toDS()

    //针对的是一整行相同的过滤
    data.distinct().show

    //针对的是某一列数据相同的过滤
    data.dropDuplicates("age").show
  }

}

case class Person(name:String , age:Int)
