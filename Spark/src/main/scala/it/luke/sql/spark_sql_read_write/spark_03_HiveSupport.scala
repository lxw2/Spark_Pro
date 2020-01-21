package it.luke.sql.spark_sql_read_write

import org.apache.spark.sql.{SaveMode, SparkSession}

object spark_03_HiveSupport {

  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    //   1、配置metastore的uri
    //   2、配置warehouse路径
    //   3、开启hive支持
    val spark = SparkSession.builder()
      .appName("hive")
      .config("hive.metastore.uris", "thrift://node01:9083")
      .config("spark.sql.warehouse.dir", "/dataset/hive")
      .enableHiveSupport()
      .getOrCreate()

    //2、数据读取
    val source = spark.read
      .option("sep", "\t")
      .csv("data/studenttab10k")
      .toDF("name", "age", "gpa")
      .selectExpr("name", "cast(age as int) age", "cast(gpa as float) gpa")

    //2.1 配置了源数据信息,也可以直接通过sql访问hive表
    spark.sql(
      """
        |select * from mytest.score
      """.stripMargin).show()
    //3、数据处理
    import spark.implicits._
    val resultDF = source.where('age > 55)
    //4、写入hive
    resultDF.write.mode(SaveMode.Append).saveAsTable("spark03.student")
  }
}
