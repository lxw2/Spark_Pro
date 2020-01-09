package it.luke.rdd.spark_01_trans

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object spark_05_mapPartitionsWithIndex {



  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf: SparkConf = new SparkConf().setAppName("mappartitionwithindex").setMaster("local[10]")
    //创建sparkContext
    val sc: SparkContext = new SparkContext(conf)
//    sc.setLogLevel("WARN")

    //简单的mappartitionwithindex
//    simple_mappartitionwithindex(sc)

    //创建jdbc连接到mysql 的mappartitionwithindex
    connect_mappartitionwithindex(sc)

    Thread.sleep(1000000)
  }

  /**
    * 相当于携带了分区标号的mappartition
    * @param sc
    */
  def simple_mappartitionwithindex(sc: SparkContext): Unit = {
    //创建数据源
    val listRdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9))

    //调用mapparitionwithindex
    listRdd.mapPartitionsWithIndex((index,iter)=>{
      println(index)
      iter.map(iter=>{
        val value  = iter*10
        val index1 = index
        (index1,value)
      }).foreach(println(_))
      println("=========")
      iter
    }).count()
  }

  /**
    * 在每个分区中创建jdbc的连接,以分区索引作为id
    * @param sc
    */
  def connect_mappartitionwithindex(sc: SparkContext): Unit = {
    //创建数据源
    val listRdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,10))

    //创建累加器
    val connect_accumulator = sc.collectionAccumulator[String]("connect_accumulator")
    //配置mysql 的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val username = "root"
    val password = "123"
      //调用mappartitionwithindex
    val res= listRdd.mapPartitionsWithIndex((index, Iter)=>{
        var name = ""
      //创建已给可变集合来存放结果
      val strings: ListBuffer[String] = new scala.collection.mutable.ListBuffer[String]()
      var connect:Connection = null
      var statment: PreparedStatement = null
      //创建连接
      Class.forName(driver)
      connect= DriverManager.getConnection(url,username,password)
      statment = connect.prepareStatement("select id, name from spark_connect where id = ?")
      Iter.map(i =>{
        connect_accumulator.add(connect.toString)
          //获取查询结果
        statment.setInt(1,i)
        val set: ResultSet = statment.executeQuery()
        while (set.next()) {
          name = set.getString("name")
//          println(name+"----")
          strings+=name
        }
        name
      }).foreach(println(_)) //嵌套的算子也需要action
      strings.toIterator
    })


    res.count()
    res.foreach(println(_))

    println(connect_accumulator.value)
  }

}
