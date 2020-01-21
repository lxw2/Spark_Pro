package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.SparkSession
import org.junit.Test

class spark_13_Window {

  val spark = SparkSession.builder().master("local[4]").appName("window").getOrCreate()
  /**
    * 常用开窗函数：（最常用的应该是1.2.3 的排序）
    * --排序函数
    * 1、row_number() over(partition by ... order by ...)
    * 2、rank() over(partition by ... order by ...)
    * 3、dense_rank() over(partition by ... order by ...)
    * --聚合函数
    * 4、count() over(partition by ... order by ...)
    * 5、max() over(partition by ... order by ...)
    * 6、min() over(partition by ... order by ...)
    * 7、sum() over(partition by ... order by ...)
    * 8、avg() over(partition by ... order by ...)
    * 9、first_value() over(partition by ... order by ...)
    * 10、last_value() over(partition by ... order by ...)
    */
  /**
    * over中order by 是必须要写，partition by 可以不写
    *   partition by 如果写明是按照哪个字段分组，那么序号就是在每个分组中进行
    *   partition by 如果不写，序号就是全局的
    */
  @Test
  def rowNumber(): Unit ={
    //需求:获取班级的前三名的学生信息
    val source = spark.read.json("data/window.json")

    source.createOrReplaceTempView("student")
    //source.show()

    spark.sql(
      """
        |select id,name,clazz,score,row_number() over(order by score desc) rn
        | from student
      """.stripMargin).show

    /*spark.sql(
      """
        |select id,name,clazz,score,row_number() over(partition by clazz order by score desc) rn
        | from student
      """.stripMargin).createOrReplaceTempView("tmp")

    spark.sql(
      """
        |select id,name,clazz,score
        | from tmp
        | where rn<=3
      """.stripMargin).show*/

    //+-----+---+----+-----+
    //|clazz| id|name|score| rn
    //+-----+---+----+-----+
    //|    1|  3|   c|   95| 1
    //|    1|  1|   a|   80| 2
    //|    1|  2|   b|   78| 3

    //|    2|  6|   e|   92| 1
    //|    2|  5|   d|   74| 2

    //|    3|  8|   g|  100| 1
    //|    3|  7|   f|   99| 2
    //|    3| 11|   j|   78| 3
    //|    3| 10|   i|   55| 4
    //|    3|  9|   h|   45| 5
    //+-----+---+----+-----+

  }

  @Test
  def rank(): Unit ={
    //需求:获取班级的前三名的学生信息
    val source = spark.read.json("data/window.json")

    source.createOrReplaceTempView("student")

    spark.sql(
      """
        |select id,name,clazz,score,rank() over(partition by clazz order by score desc) rn
        | from student
      """.stripMargin).show

    //+-----+---+----+-----+
    //|clazz| id|name|score| rn
    //+-----+---+----+-----+
    //|    1|  3|   c|   95| 1
    //|    1|  1|   a|   80| 2
    //|    1|  2|   b|   78| 3

    //|    2|  6|   e|   92| 1
    //|    2|  5|   d|   74| 2

    //|    3|  8|   g|   99| 1
    //|    3|  7|   f|   99| 1
    //|    3| 11|   j|   99| 1
    //|    3| 10|   i|   55| 4
    //|    3|  9|   h|   45| 5
    //+-----+---+----+-----+
  }

  @Test
  def descerank(): Unit ={
    //需求:获取班级的前三名的学生信息
    val source = spark.read.json("data/window.json")

    source.createOrReplaceTempView("student")

    spark.sql(
      """
        |select id,name,clazz,score,dense_rank() over(partition by clazz order by score desc) rn
        | from student
      """.stripMargin).show

    //+---+----+-----+-----+---+
    //| id|name|clazz|score| rn|
    //+---+----+-----+-----+---+
    //|  3|   c|    1|   95|  1|
    //|  1|   a|    1|   80|  2|
    //|  2|   b|    1|   78|  3|

    //|  7|   f|    3|   99|  1|
    //|  8|   g|    3|   99|  1|
    //| 11|   j|    3|   78|  2|
    //| 10|   i|    3|   55|  3|
    //|  9|   h|    3|   45|  4|

    //|  6|   e|    2|   92|  1|
    //|  5|   d|    2|   74|  2|
    //+---+----+-----+-----+---+
  }

  /**
    * max中需要写明字段
    * over中partition by与order by 是可以不写的，
    *   如果写明了partition by 那么求得就是每个分区的最大值
    *   如果不写取得就是全局的最大值
    */
  @Test
  def max(): Unit ={

    val source = spark.read.json("data/window.json")

    source.createOrReplaceTempView("student")

    spark.sql(
      """
        |select id,name,clazz,score,max(score) over() score_max
        | from student
      """.stripMargin).show
    //+---+----+-----+-----+---------+
    //| id|name|clazz|score|score_max|
    //+---+----+-----+-----+---------+
    //|  1|   a|    1|   80|       99|
    //|  2|   b|    1|   78|       99|
    //|  3|   c|    1|   95|       99|

    //|  7|   f|    3|   99|       99|
    //|  8|   g|    3|   99|       99|
    //|  9|   h|    3|   45|       99|
    //| 10|   i|    3|   55|       99|
    //| 11|   j|    3|   78|       99|

    //|  5|   d|    2|   74|       99|
    //|  6|   e|    2|   92|       99|
    //+---+----+-----+-----+---------+
  }
}
