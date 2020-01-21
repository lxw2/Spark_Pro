package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession

object spark_03_filter {


  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().master("local[4]").appName("filter")
      .getOrCreate()

    spark_filter(spark)
  }
  def spark_filter(spark:SparkSession): Unit ={

    import spark.implicits._
    val data = Seq(Person("zhangsan",20),Person("lisi",50)).toDS()

    data.filter(p => p.age>=30).show

    data.filter("age>=30").show
  }
}

case class Person (name:String , age : Int)
