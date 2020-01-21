package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.Test

class spark_16_NullProcess{

  val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  @Test
  def numprocess(): Unit ={

    val source: Dataset[String] = spark.read
        .textFile("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\union.txt")


    val res = source.filter(item=>{
      val bool: Boolean = item.startsWith("id")
      if (bool)
        {
          false
        }
      else {
        true
      }
    }).map(item=>{
      val arr: Array[String] = item.split("\\|")
      val id  = arr(0)
      val name= arr(1)
      (id,name)

    }).toDF("id","name")

    //+---+-------+
    //| id|   name|
    //+---+-------+
    //|  1|   lisi|
    //|  2|zhansan|
    //|  3|zhaolei|
    //|  4|     NA|
    //+---+-------+

    //NA是空值  对空值进行处理

    //1、丢弃
    res.filter("name!='NA'").show
    res.printSchema()
    println("==========丢弃")

    //2、替换

    res.rdd.map(row=>{
      val id = row.getAs[String]("id")
      var name = row.getAs[String]("name")
      name = if (name=="NA") "默认" else name
      (id,name)
    }).toDF("id","name").show()

    println("======替换")
    //3.替换
    //调用naapi进行replace
    val frame = res.na.replace("name",Map("NA"->"aaa"))
    frame.show()

  }

}
