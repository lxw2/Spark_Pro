package it.luke.rdd.spark_01_trans

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_04_mapPartitions {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("mappartition")
    //    conf.set("spark.serializer",
    //      "org.apache.spark.serializer.KryoSerializer")
    //    conf.registerKryoClasses(Array(classOf[Connection]))
    //创建sparkContext
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //简单的mappartition
    //    sample_mappartition(sc)
    //获取mysql连接
    connect_mapption(sc)
  }

  /**
    * 简单的用累加器来测试mappartition
    *
    * @param sc
    */
  def sample_mappartition(sc: SparkContext) = {

    //创建数据源
    val listRDD: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 45, 6, 7, 8))


    //添加一个累加器用于累计
    val num = sc.longAccumulator("num")
    //mappartition
    listRDD.mapPartitions(item => {

      //累加器进行叠加
      num.add(1)
      //拿到一个分区中的rdd
      val res = item.map(a => {
        val res = a * 10
        res
      })

      res.foreach(println(_))
      res
    }).count()

    println(num.value)
    //20
    //30
    //70
    //10
    //450
    //80
    //60
    //4 --累加器
  }

  /**
    * 通过mappartition来创建连接,避免数据过多
    *
    * @param sc
    */
  def connect_mapption(sc: SparkContext): Unit = {

    //创建数据源
      val listRDD: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9))

    //创建一个集合累加器,来计算多少连接
      val connectAccumulator = sc.collectionAccumulator[String]("connectAccumulator")
    //配置连接mysql的参数
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/test"
      val userName = "root"
      val password = "123"
    //通过mappartiiton进行创建jdbc连接
      listRDD.mapPartitions(item => {
      var connect: Connection = null
      var statement: PreparedStatement = null
      var name = ""
      //创建连接
      connect = DriverManager.getConnection(url, userName, password)
      //添加连接到累加器中
      connectAccumulator.add(connect.toString)
      statement = connect.prepareStatement("select id,name from spark_connect where id = ? ")
      item.map(e => {
          statement.setInt(1, e)
          val set: ResultSet = statement.executeQuery()
          while (set.next()) {
            name = set.getString("name")
            println(name)
          }
        (name)
      })
    }).count()

    //打印累加器中的连接
    //    [com.mysql.jdbc.JDBC4Connection@17caf2ac,
    // com.mysql.jdbc.JDBC4Connection@6ea22bc6,
    // com.mysql.jdbc.JDBC4Connection@288e09ff,
    // com.mysql.jdbc.JDBC4Connection@5664c61e]
    println(connectAccumulator.value)
  }
}

