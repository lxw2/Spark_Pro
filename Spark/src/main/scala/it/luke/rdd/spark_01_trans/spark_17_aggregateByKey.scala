package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object spark_17_aggregateByKey {


  /**
    * 作用
    * 聚合所有 Key 相同的 Value, 换句话说, 按照 Key 聚合 Value
    * 调用
    * rdd.aggregateByKey(zeroValue)(seqOp, combOp)
    * 参数
    * zeroValue 初始值
    * seqOp 转换每一个值的函数
    * comboOp 将转换过的值聚合的函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("aggregatebykey")
    //创建context
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    simple_aggregateBykey(sc)
//    contact_aggregateBykey(sc)

    //join + 求平均值
//    avg_aggregateBykey(sc)

    //left + option + 平均值
    Option_agg(sc)
  }
  def simple_aggregateBykey(sc: SparkContext): Unit = {

    //创建数据源
    val rdd1 = sc.parallelize(Seq(("a",10),("a",40),("a",20),("b",30)),2)


  }


  def contact_aggregateBykey(sc:SparkContext): Unit ={
    //创建数据源

    val zeroValue = collection.mutable.Set[String]()
    val userAccesses = sc.parallelize(Array(("u1", "site1"), ("u2", "site1"), ("u1", "site1"), ("u2", "site3"), ("u2", "site4")))


    //调用方法
     val res: RDD[(String, mutable.Set[String])] = userAccesses.aggregateByKey(zeroValue)(
        (s,v) =>s += v,
        (a,b) => a++=b
      )

    res.count()
    res.foreach(println(_))

  }
  def avg_aggregateBykey(sc: SparkContext): Unit = {

    //创建数据源
    val student = sc.parallelize(Seq[(Int, String, Double, String)](
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_03")
    ))

    val clazz: RDD[(String, String)] = sc.parallelize(Seq[(String, String)](
      ("class_01", "java基础班"),
      ("class_02", "大数据"),
      ("class_03", "python基础班")
    ))

    //转换student
    val mapstudent: RDD[(String, Double)] = student.map(item=>(item._4,item._3))

    //调用方法
    val leftclazz: RDD[(String, (String, Double))] = clazz.join(mapstudent)

    //通过aggregate 进行聚合
    //  (zeroValue: U)
    // (seqOp: (U, V) => U,
    // combOp: (U, U) => U): RDD[(K, U)

    val zerovalue: ((String, Double), Int) = (("",0.0),0)

//    //调用方法
    //     val res: RDD[(String, mutable.Set[String])] = userAccesses.aggregateByKey(zeroValue)(
    //        (s,v) =>s += v,
    //        (a,b) => a++=b
    //      )
    //(String, Option[Double]),Int)
    //value (String, Option[Double])
    leftclazz.aggregateByKey(zerovalue)(
      (s,v) =>{
        val className = v._1
        val sum = v._2+s._1._2
        val num = s._2+1
        ((className,sum),num)
      },
      (a,b) =>
        {
          val className = a._1._1
          val sum = b._1._2+a._1._2
          val num = a._2+b._2
          ((className,sum),num)
        }
    ).map{
      case (classid,((className,score_sum),num)) =>
        (className,score_sum/num)
    }.foreach(println(_))

  }

  def Option_agg(sc:SparkContext)={

    val parent: Option[Int] = None
    parent.getOrElse("")

    //创建数据源
    val student = sc.parallelize(Seq[(Int, String, Double, String)](
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_03")
    ))

    val clazz: RDD[(String, String)] = sc.parallelize(Seq[(String, String)](
      ("class_01", "java基础班"),
      ("class_02", "大数据"),
      ("class_03", "python基础班")
    ))

    //转换student
    val mapstudent: RDD[(String, Double)] = student.map(item=>(item._4,item._3))

    //调用方法
    val leftclazz: RDD[(String, (String, Option[Double]))] = clazz.leftOuterJoin(mapstudent)


    val a :Option[Double] = None
    val zero: ((String, Option[Double]), Int) = (("",a),0)
    //(zeroValue: U)
    // (seqOp: (U, V) => U,
    // combOp: (U, U) => U)
    leftclazz.aggregateByKey(zero)(
      (U, V) => {
//        val a = V._2.getOrElse(0.0)
        val className: String = V._1
        val score_sum: Double = U._1._2.getOrElse(0.0) + V._2.getOrElse(0.0)
        val num = U._2+1
        ((className,Some(score_sum)),num)
      },
      (A, C) => {
        val className: String = A._1._1
        val score_sum: Double = A._1._2.getOrElse(0.0) + C._1._2.getOrElse(0.0)
        val num = A._2+C._2
        ((className,Some(score_sum)),num)
      }
    ).map{
      case (classid,((className,sum),num)) =>
        (className,sum.getOrElse(0.0)/num)
    }.foreach(println(_))

  }
}
