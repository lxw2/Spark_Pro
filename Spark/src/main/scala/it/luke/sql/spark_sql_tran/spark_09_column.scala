package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession

object spark_09_column {

  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().master("local[4]").appName("filter")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark_column(spark)
  }

  def spark_column(spark:SparkSession): Unit ={

    import spark.implicits._
    val data = Seq(Person("zhangsan",20),Person("lisi",50),Person("wangwu",50),Person("zhangsan",30)).toDS()
    import org.apache.spark.sql.functions._
    //增加一列
    data.withColumn("rand",expr("rand()")).show

    data.selectExpr("name","age","rand()").show
    //增加一列，列值用拥有的列的值
    data.withColumn("name_new",'name).show()
    data.selectExpr("name","age","name name_new").show
    //重命名
    data.withColumnRenamed("name","name_new").show
    data.selectExpr("age","name name_new").show

  }

}
