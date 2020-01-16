package it.luke.sql.spark_sql_tran

import org.apache.spark.sql._

object spark_00_ColumnSel {



  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Column")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //无绑定方式
//    Unbound_column_sel(spark)

    //有绑定的方式
    bound_column_sel(spark)

  }
  def Unbound_column_sel(spark: SparkSession): Unit = {
    import spark.implicits._
    //无绑定的列选取
    val column1: Symbol = 'name

    val column2: ColumnName = $"name" //import spark.implict._

    import org.apache.spark.sql.functions._
    val column3 = col("name") //import org.apache.spark.sql.functions._

    val column4 = column("name") //import org.apache.spark.sql.functions._

    //读取数据源进行测试
    val source: DataFrame = spark.read
      .option("header",true)
      .option("delimiter","|")
      .csv("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\union.txt")

    //select (cols: Column*)
    val frame1: DataFrame = source.select('name) //只有这个方法不需要导入
    frame1.show()
    val frame2: DataFrame = source.select($"id")
    frame2.show()
    val frame3: DataFrame = source.select(col("name"))
    frame3.show()
    val frame4: DataFrame = source.select(column("id"))
    frame4.show()


  }

  def bound_column_sel(spark: SparkSession): Unit = {

    //有绑定模式需要先存在DS
    //读取数据
    val source: Dataset[Row] = spark.read.option("header",true)
      .option("delimiter","|")
      .csv("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\union.txt")
    //有绑定方式
//    val column5 = source.col("name")
//
//    val column6 = source.apply("name")
//
//    val column7: Column = source("name")

    val frame1: DataFrame = source.select(source.col("name"))
    frame1.show()

    val frame2: DataFrame = source.select(source.apply("name")) //apply 和 source("name") 一样
    frame2.show()

    val frame3: DataFrame = source.select(source("name"))
    frame3.show()


  }
}
