package it.luke.sql.spark_sql_tran

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object spark_01_rddToDS_DF {



  def main(args: Array[String]): Unit = {
    //获取session环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("rddtoDF")
      .getOrCreate()
    //获取context
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //context创造数据源转换成rdd,转换成DS/DF
    create_sour_toDF(sc,spark)

    //通过sc读取文件的形式的rdd 转化成DS/DF
    read_sour_toDF(sc,spark)
  }

  /**
    * 对创造的数据源的转化
    * @param sc
    * @param spark
    */
  def create_sour_toDF(sc: SparkContext,spark: SparkSession): Unit = {

    //创造数据源
    val sour: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5))


    //转化成DF
    import spark.implicits._
    val numDF: DataFrame = sour.toDF("num")
    numDF.printSchema()
    numDF.show()

    println("=="*10)
    //转化成DS

    val numDS = sour.toDS()
    numDS.printSchema()
    numDS.show()
  }

  /**
    * 通过sc读取文件的内容,进行转化
    * @param sc
    * @param spark
    */
  def read_sour_toDF(sc: SparkContext, spark: SparkSession): Unit = {

    val textRDD: RDD[String] =sc.textFile("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\union.txt")

    import spark.implicits._
    //转化rdd成DF
    val textDF: DataFrame = textRDD.toDF("num")
    textDF.printSchema()
    textDF.show()


    val textDS: Dataset[String] = textRDD.toDS()
    textDS.printSchema()
    textDS.show()



  }
}
