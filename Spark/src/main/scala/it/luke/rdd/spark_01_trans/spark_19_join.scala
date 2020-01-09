package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_19_join {


  def main(args: Array[String]): Unit = {
    //创建spark环境
    val conf = new SparkConf().setAppName("intersection").setMaster("local[4]")
    //创建sparkcontext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    simple_join(sc)

  }

  def simple_join(sc: SparkContext): Unit = {

    //创建数据源
    val personRDD: RDD[(Int, String, Double, String)] = sc.parallelize(Seq(
      (1, "aa", 55, "class_01"),
      (2, "bb", 85, "class_02"),
      (3, "cc", 90, "class_02"),
      (4, "dd", 75.5, "class_01"),
      (5, "ee", 68, "class_01"),
      (6, "ff", 65, "class_05")
    ))
    val classRDD: RDD[(String, String)] = sc.parallelize(Seq(
      ("class_01", "java基础班"),
      ("class_02", "大数据"),
      ("class_03", "python基础班")
    ))

    val pertupleRdd: RDD[(String, Double)] = personRDD.map(item => (item._4, item._3))

    val res: RDD[(String, (Double, String))] = pertupleRdd.join(classRDD)
    val res1: RDD[(String, (String, Double))] = classRDD.join(pertupleRdd)
    res.foreach(println(_))


    //    res.saveAsHadoopFile()
    //    res.count()
    //    res.foreach(println(_))
    //计算平均值
        res.combineByKey(
          (item: (Double, String)) => (item, 1),
          (agg: ((Double, String), Int), curr: (Double, String)) => {
            val className: String = agg._1._2
            val core_sum: Double = agg._1._1 + curr._1
            val num: Int = agg._2 + 1
            ((core_sum, className), num)
          },
          (agg: ((Double, String), Int), curr: ((Double, String), Int)) => {
            val className: String = agg._1._2
            val core_sum: Double = agg._1._1 + curr._1._1
            val num: Int = agg._2 + curr._2
            ((core_sum, className), num)
          }).map{
          case (classid,((score_sum,className),num)) =>{
            (className,score_sum/num)
          }
        }.foreach(println(_))


//    res1.combineByKey(
//      (item: (String, Double)) => (item, 1),
//      (agg: ((String, Double), Int), curr: (String, Double)) => {
//        val className = agg._1._1
//        val core_sum = agg._1._2 + curr._2
//        val num = agg._2 + 1
//        ((className, core_sum), num)
//      },
//      (agg: ((String, Double), Int), curr: ((String, Double), Int)) => {
//        val className: String = agg._1._1
//        val core_sum: Double = agg._1._2 + curr._1._2
//        val num: Int = agg._2 + curr._2
//        ((className, core_sum), num)
//      })

//    res1.combineByKey((item: (String, Double)) => (item, 1)
//      , (agg: ((String, Double), Int), curr: (String, Double)) => {
//        val className: String = agg._1._1
//        val score_sum: Double = agg._1._2 + curr._2
//        val num: Int = agg._2 + 1
//        ((className, score_sum), num)
//      },
//      (agg: ((String, Double), Int), curr: ((String, Double), Int)) => {
//        val className = agg._1._1
//        val score_sum = agg._1._2 + curr._1._2
//        val num = agg._2 + curr._2
//        ((className, score_sum), num)
//      })


  }
}
