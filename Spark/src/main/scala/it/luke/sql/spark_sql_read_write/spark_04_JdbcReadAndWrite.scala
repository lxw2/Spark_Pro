package it.luke.sql.spark_sql_read_write

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test

class spark_04_JdbcReadAndWrite {

  val spark: SparkSession = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  /**
    * 此种方式只适合读取的数据量比较小
    */
  @Test
  def simpleRead(): Unit ={

    val url = "jdbc:mysql://localhost:3306/db3"

    val table = "sc"

    //设置jdbc读取的参数 账号 密码 driver
    val prop = new  Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    val df = spark.read.jdbc(url,table,prop)

    df.show()
    println(df.rdd.partitions.size)
    //分区数：1
  }

  @Test
  def read_Partition(): Unit ={
    val url = "jdbc:mysql://localhost:3306/db3"

    val table = "sc"

    //设置jdbc读取的参数 账号 密码 driver
    val prop = new  Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    //设置分区的范围
    //predicates.size等于分区数，每个分区的数据有predicates元素决定
    val predicates:Array[String] = Array[String](
      "score<60",
      "score>=60 and score<80",
      "score>=90"
    )

    val df = spark.read.jdbc(url,table,predicates,prop)

    df.rdd.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index}  data:${it.toBuffer}")
      it
    }).collect()

    //spark.read.format("jdbc")
    //  .option("url", "jdbc:mysql://localhost:3306/db3")
    //  .option("dbtable", "sc")
    //  .option("user", "root")
    //  .option("password", "123456")
    //  .option("partitionColumn", "score")
    //  .option("lowerBound", 1)
    //  .option("upperBound", 60)
    //  .option("numPartitions", 10)
    //  .load()
    //  .show()


    //index:1  data:ArrayBuffer([02,01,70.0], [02,02,60.0], [05,01,76.0])
    //index:0  data:ArrayBuffer([04,01,50.0], [04,02,30.0], [04,03,20.0], [06,01,31.0], [06,03,34.0])
    //index:2  data:ArrayBuffer([01,02,90.0], [01,03,99.0], [07,03,98.0])
  }

  /**
    * 此种方式适合大数据量的表
    */
  @Test
  def read3(): Unit ={
    val url = "jdbc:mysql://localhost:3306/db3"

    val table = "sc"

    //设置jdbc读取的参数 账号 密码 driver
    val prop = new  Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    /**
      * columnName:指定分区的字段
      * lowerBound:字段最小值，用于分区规则
      * upperBound:字段的最大值,用于分区规则
      * numPartitions:指定分区数
      *
      * columnName只能是数字类型
      */
    val df = spark.read.jdbc(url,table,"score",0,100,10,prop)

    //println(df.rdd.partitions.size)
    df.rdd.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index} data:${it.toBuffer}")
      it
    }).collect()

    //表现形式
    //spark.read.format("jdbc")
    //  .option("url", "jdbc:mysql://localhost:3306/db3")
    //  .option("dbtable", "sc")
    //  .option("user", "root")
    //  .option("password", "123456")
    //  .option("partitionColumn", "score")
    //  .option("lowerBound", 1)
    //  .option("upperBound", 60)
    //  .option("numPartitions", 10)
    //  .load()


    //if (partitioning == null || partitioning.numPartitions <= 1 ||
    //      partitioning.lowerBound == partitioning.upperBound) {
    //      return Array[Partition](JDBCPartition(null, 0))
    //    }
    //
    //    val lowerBound = partitioning.lowerBound = 0
    //    val upperBound = partitioning.upperBound = 100
    //
    //
    //    val numPartitions =
    //      if ((upperBound - lowerBound) >= partitioning.numPartitions) {
    //      if ((100 - 0) >= 1000) {
    //
    //        partitioning.numPartitions
    //      } else {
    //
    //        100-0
    //      }
    //    // Overflow and silliness can happen if you subtract then divide.
    //    // Here we get a little roundoff, but that's (hopefully) OK.
    //    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions
    //    val stride: Long = 100 / 5 - 0 / 5 = 20
    //    val column = partitioning.column = "age"
    //    var i: Int = 0
    //    var currentValue: Long = lowerBound = 0
    //    var ans = new ArrayBuffer[Partition]()
    //    while (i < numPartitions) {
    //      val lBound = if (i != 0) s"$column >= $currentValue" else null
    //      currentValue += stride
    //      val uBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
    //      val whereClause =
    //        if (uBound == null) {
    //          lBound
    //        } else if (lBound == null) {
    //          s"$uBound or $column is null"
    //        } else {
    //          s"$lBound AND $uBound"
    //        }
    //      ans += JDBCPartition(whereClause, i)
    //      i = i + 1
    //    }
    //    ans.toArray
    //  }
    //第一次循环
    //  0< 5
    //   val lBound = null
    //   currentValue  = currentValue+stride = 0 +20
    //   val uBound = s"age < 20"
    //   val whereClause = s"age < 20 or age is null"
    // 1<5
    //   val lBound = s"age >= 20"
    //   currentValue = currentValue + stride = 20 + 20 = 40
    //   val uBound = s"age < 40"
    //   val whereClause =  s"age >= 20 AND age < 40"
    // 2<5
    //         val lBound = s"age >= 40"
    //         currentValue = 60
    //         val uBound = s"age < 60"
    //         val whereClause = s"age>=40 AND age<60"
    // 3<5
    //     val lBound = s"age >= 60"
    //     currentValue = 80
    //      val uBound = s"age < 80"
    //     val whereClause = s"age>=60 AND age<80"
    //4<5
    //   val lBound = s"age >= 80"
    //   currentValue =100
    //   val uBound = null
    //   val whereClause = s"age >= 80"
    //
  }

  /**
    * 写入mysql
    */
  @Test
  def write(): Unit ={
    val df = spark.read.option("sep","\t").csv("data/studenttab10k")
    val url = "jdbc:mysql://hadoop01:3306/test"

    val table = "student"

    //设置jdbc读取的参数 账号 密码 driver
    val prop = new  Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    df.write.mode(SaveMode.Overwrite).jdbc(url,table,prop)

    //实例
    //val spark = SparkSession
    //  .builder()
    //  .appName("hive example")
    //  .master("local[6]")
    //  .getOrCreate()
    //
    //val schema = StructType(
    //  List(
    //    StructField("name", StringType),
    //    StructField("age", IntegerType),
    //    StructField("gpa", FloatType)
    //  )
    //)
    //
    //val studentDF = spark.read
    //  .option("delimiter", "\t")
    //  .schema(schema)
    //  .csv("dataset/studenttab10k")
    //
    //studentDF.write.format("jdbc").mode(SaveMode.Overwrite)
    //  .option("url", "jdbc:mysql://node01:3306/spark_test")
    //  .option("dbtable", "student")
    //  .option("user", "root")
    //  .option("password", "123456")
    //  .save()
  }
}
