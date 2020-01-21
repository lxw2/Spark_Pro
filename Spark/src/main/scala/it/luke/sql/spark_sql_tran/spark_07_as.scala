package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession

object spark_07_as {


  def main(args: Array[String]): Unit = {

    //创建sparksession
    val spark: SparkSession =SparkSession.builder().appName("as").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark_as(spark)

  }

  def spark_as(spark: SparkSession): Unit = {

    import spark.implicits._

      val source = spark.read.option("sep","\t").csv("Spark/mytest/studenttab10k")


      val ds = source.as[(String,String,String)]
//      val ds1 = source.as[(String,Int,Float)]
//      val ds1 = source.as[student] 不可行 需要encoder

      ds.show

  }
}

case class student(name:String ,age:String ,score:String)
