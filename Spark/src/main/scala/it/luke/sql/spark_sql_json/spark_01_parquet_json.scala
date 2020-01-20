package it.luke.sql.spark_sql_json

import java.util

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object spark_01_parquet_json {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[4]").appName("parquetJson").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //读取parquet文件,并生成json字符串
    val sour: DataFrame = spark.read
      .parquet("D:\\GitPro\\Spark_Pro\\Spark\\myparquet\\myparquet01\\")

    import spark.implicits._
    val res1: DataFrame = sour.filter(item => {

      val bool = item.getString(0).startsWith("i")
      if (bool) {
        false
      }
      else {
        true
      }
    }).map(item => {
      val arr = item.getString(0).split(",")
      val id = arr(0)
      val name = arr(1)
      (id, name)

    }).toDF("id","name")
    res1.show()

    val str = res1.toJSON.collectAsList().toString
    println(str)
    //[{"id":"4","name":"kuiliu"}, {"id":"5","name":"danteng"}, {"id":"6","name":"bushu"}]

    //获得json字符串
    //需求
    //1.将json字符串转换成json数组
      //因为字符串中又类型,需要定义case class
    //1.1
    val array: JSONArray = JSON.parseArray(str)
    println(array.toString)
    //[{"name":"kuiliu","id":"4"},{"name":"danteng","id":"5"},{"name":"bushu","id":"6"}]

    //1.2
    val array1: util.List[person] = JSON.parseArray(str,classOf[person])
    println(array1)
    //[person(4,kuiliu), person(5,danteng), person(6,bushu)]

    //1.3
    val unit = JSON.parse(str)
    println(unit)
    //[{"name":"kuiliu","id":"4"},{"name":"danteng","id":"5"},{"name":"bushu","id":"6"}]

//    val nObject = JSON.parseObject(str)
//    println(nObject) 错误 不能够!!

//    val personjson = JSON.parseObject(str,classOf[person])
//    println(personjson)  错误 不能够!!

    //2.将json字符串转换成DF

    val ds: Dataset[String] = spark.createDataset(Seq(str))
    ds.show()
    val frame = spark.read.json(ds)
    frame.show()
    //[4,kuiliu]
    //[5,danteng]
    //[6,bushu]


    //3.将json字符串转换成rdd

    val ds1: Dataset[String] = spark.createDataset(Seq(str))
    ds1.show()
    val frame1 = spark.read.json(ds)
    frame1.rdd.foreach(println(_))
    //[4,kuiliu]
    //[5,danteng]
    //[6,bushu]
  }

}

case class person(id :Int , name :String)
