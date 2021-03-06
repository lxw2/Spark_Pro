package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession

object spark_02_select {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().master("local[4]").appName("filter")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //调用方法
    spark_select(spark)
  }

  def spark_select(spark:SparkSession): Unit ={

    import spark.implicits._
    val data = Seq(Person("zhangsan",20),Person("lisi",50),Person("wangwu",50),Person("zhangsan",30)).toDS()

    data.select('name).show

    data.selectExpr("name","age").show

    data.selectExpr("sum(age)").show()

    import org.apache.spark.sql.functions._
    data.select(expr("sum(age)")).show
  }
}
