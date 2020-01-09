package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_20_leftOuterJoin {


  def main(args: Array[String]): Unit = {

    //创建spark 环境
    val conf: SparkConf = new SparkConf().setAppName("leftJoin").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_leftjoin(sc)
  }

  def simple_leftjoin(sc: SparkContext): Unit = {

    //创建数据源
    val student = sc.parallelize(Seq[(Int, String, Double, String)](
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_05")
    ))

    val clazz = sc.parallelize(Seq[(String, String)](
      ("class_01", "java基础班"),
      ("class_02", "大数据"),
      ("class_03", "python基础班")
    ))

    val mapStudent: RDD[(String, Double)] = student.map(item => (item._4, item._3))
    val leftjoinres: RDD[(String, (String, Option[Double]))] = clazz.leftOuterJoin(mapStudent)
    leftjoinres.foreach(println(_))
    //求平均值 combinebykey + map


    //    createCombiner: V => C,
    //      mergeValue: (C, V) => C,
    //      mergeCombiners: (C, C) => C,
    leftjoinres.combineByKey((item: (String, Option[Double])) => (item, 1),
      (agg: ((String, Option[Double]), Int), curr: (String, Option[Double])) => {
        val className: String = agg._1._1
        val demo: Option[Double] = agg._1._2
        val sum: Double = agg._1._2.getOrElse(0.0) + curr._2.getOrElse(0.0)
        val num = agg._2 + 1
        ((className, Some(sum)), num)
      },
        (agg: ((String, Option[Double]), Int), curr: ((String, Option[Double]), Int)) => {
          val className: String = agg._1._1
          val sum: Double = agg._1._2.getOrElse(0.0) + curr._1._2.getOrElse(0.0)
          val num: Int = agg._2 + curr._2
          ((className, Some(sum)), num)
        }
      ).map{
//      val a: (String, ((String, Option[Double]), Int)) = item
      case (classId,((className,score),num)) =>
        (className,score.getOrElse(0.0)/num)
    } //map + case 的时候 不要使用()
      .foreach(println(_))

    //(class_03,(python基础班,None))
    //(class_01,(java基础班,Some(55.0)))
    //(class_02,(大数据,Some(85.0)))
    //(class_01,(java基础班,Some(75.5)))
    //(class_02,(大数据,Some(90.0)))
    //(class_01,(java基础班,Some(68.0)))
    //(python基础班,0.0)
    //(大数据,87.5)
    //(java基础班,66.16666666666667)

  }

}
