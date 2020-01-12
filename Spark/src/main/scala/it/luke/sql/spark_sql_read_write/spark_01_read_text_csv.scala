package it.luke.sql.spark_sql_read_write

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object spark_01_read_text_csv {



  def main(args: Array[String]): Unit = {

    //创建一个sparksession
    val spark =SparkSession.builder().appName("Text_csv").master("local[4]")
      .getOrCreate()

    //指定分隔符,指定表头
    simple_text_csv(spark)

    //指定schema
    schema_text_csv(spark)



  }

  /**
    * 通过csv的api读取text文件
    * @param spark
    */
  def simple_text_csv(spark: SparkSession) = {
    //读取
    //csv参数:
    //option:
    //header 指定是否要表头
    //delimiter 指定分隔符
    val sour: DataFrame = spark.read
      .option("header",true)
      .option("delimiter","|")
      .csv("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\union.txt")

    sour.printSchema()
    sour.show()
  }

  /**
    * 指定schema进行读取
    * @param spark
    */
  def schema_text_csv(spark: SparkSession): Unit = {

    //创建schema
    val schema: StructType = StructType(List(
      StructField("id",IntegerType),
      StructField("name",StringType)
    ))

    //root
    // |-- id: integer (nullable = true)
    // |-- name: string (nullable = true)

    //读取
    val readSour: DataFrame = spark.read
      .option("header",true)
      .option("delimiter","|")
      .schema(schema)
      .csv("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\union.txt")


    readSour.printSchema()
    readSour.show()
  }
}
