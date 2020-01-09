
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ossdemo {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("testdemo").master("local[4]").getOrCreate()
    val source = spark.read.option("sep","|")
      .csv("D:\\GitPro\\Spark_Pro\\Spark_sql\\dmeo\\scriptFile.csv")
      .toDF("gid","name","date","id")
    spark.sparkContext.setLogLevel("warn")
    // source.show(false)
    source.createOrReplaceTempView("ex")

    val demo= spark.sql(
      """
        |select
        |d.gid
        |from ex d
      """.stripMargin).collect()
    //demo.createOrReplaceTempView("demo")

    val arr = new ArrayBuffer[String]
    demo.map(row => {
      val gid = row.getString(0)
      arr+=gid
    })
    val a =arr.mkString(",")
    println(a)


    // arr.mkString(",")
    import scala.collection.JavaConversions._
    /*
        val list: List[Char] = demo.toLocalIterator().toString.toList

        println(list)
    */
    /*val str = list.mkString(",")
      println(str)*/




  }
}
