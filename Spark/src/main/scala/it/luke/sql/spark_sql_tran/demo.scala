package it.luke.sql.spark_sql_tran

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

case class People(name: String, age: Int)

object demo {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("hello")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val peopleRDD: RDD[People] = spark.sparkContext.parallelize(Seq(People("zhangsan", 9), People("lisi", 15)))
    val peopleDS: Dataset[People] = peopleRDD.toDS
    val teenagers: Dataset[String] = peopleDS.where('age > 10)
      .where('age < 20)
      .select('name)
      .as[String]

    teenagers.show()
  }

}
