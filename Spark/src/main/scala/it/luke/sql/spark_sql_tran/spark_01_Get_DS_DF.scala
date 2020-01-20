package it.luke.sql.spark_sql_tran

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object spark_01_Get_DS_DF {



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

    //创建DS
    create_DS(spark)

    //创建DF
    create_DF(spark)

    //DF 和 DS 相互转换
    exchange_DF_DS(spark)
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

  /**
    * 直接船舰DS
    * @param spark
    */
  def create_DS(spark: SparkSession): Unit = {

    import spark.implicits._
    //创建
    val DSsour: Dataset[Int] = spark.createDataset(Seq(1,2,3))

    DSsour.show()
    //+-----+
    //|value|
    //+-----+
    //|    1|
    //|    2|
    //|    3|
    //+-----+
    //添加样例类
    val DSsour1: Dataset[person] = spark.createDataset[person](Seq(person("zhansan",12),person("lisi",21)))

    DSsour1.show()
    //+-------+---+
    //|   name|age|
    //+-------+---+
    //|zhansan| 12|
    //|   lisi| 21|
    //+-------+---+
  }

  /**
    * 直接创建DF (待完善)
    * @param spark
    */
  def create_DF(spark: SparkSession): Unit = {

    import  scala.collection.JavaConversions._
//    import scala.

//    val frame1 = spark.createDataFrame(Seq(1,2,3),Class[Integer])
//    frame1.show()

//    val frame2: DataFrame = spark.createDataFrame(Seq(person("zhansan",12),person("lisi",21)),classOf[person])
//    import spark.implicits._
//    frame2.select($"name").show()


    import spark.implicits._

    val df1: DataFrame = Seq("nihao", "hello").toDF("text")

    /*
    +-----+
    | text|
    +-----+
    |nihao|
    |hello|
    +-----+
     */
    df1.show()

    val df2: DataFrame = Seq(("a", 1), ("b", 1)).toDF("word", "count")

    /*
    +----+-----+
    |word|count|
    +----+-----+
    |   a|    1|
    |   b|    1|
    +----+-----+
     */
    df2.show()

  }

  /**
    * DF 与 DS 的相互转换
    * @param spark
    */
  def exchange_DF_DS(spark: SparkSession): Unit = {



  }
}

case class person(name :String ,age:Int)
