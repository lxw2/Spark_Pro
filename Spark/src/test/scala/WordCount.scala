import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建sparkcontext
    val conf = new SparkConf().setMaster("local[4]").setAppName("WOrdCount")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //读取数据
    val source: RDD[String] = sc.textFile("D:\\GitPro\\Spark_Pro\\Spark\\mytest\\wordCount.txt")

    val res = source.flatMap(item=>{
      val strings = item.split(",")
      strings
    }).map(item=>{
      (item,1)
    })

    res.reduceByKey((agg,curr)=>{
      agg+curr
    }).foreach(println(_))






  }

}
