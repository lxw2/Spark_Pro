package it.luke.rdd.spark_02_ReadandWrite

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

//import mysqlUtils.OperatorMySql
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

//继承MultipleTextOutputFormat -->继承MultipleOutputFormat -->继承FileOutputFormat -->implements OutputFormat
class spark_Custom_outputformat extends MultipleTextOutputFormat[Any, Any] {

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val timelong = System.currentTimeMillis()

    val fileName = s"${key.asInstanceOf[String]}盛梅花+${timelong}" + ".txt"
    fileName
  }

  //  因为saveAsHadoopFile是以key,value的形式保存文件，写入文件之后的内容也是，
  // 按照key value的形式写入，k,v之间用空格隔开，这里我只需要写入value的值，
  // 不需要将key的值写入到文件中个，所以我需要重写
  //该方法，让输入到文件中的key为空即可，当然也可以进行领过的变通，
  // 也可以重写generateActuralValue(key:Any,value:Any),根据自己的需求来实现

  override def generateActualKey(key: Any, value: Any): Any = {
    null
  }

  //  对生成的value进行转换为字符串，当然源码中默认也是直接返回value值，
  // 如果对value没有特殊处理的话，不需要重写该方法
  //override def generateAcutalValue(key: Any, value: Any): String = {
  //     return value.asInstance[String]<br>  }
  // * 该方法使用来检查我们输出的文件目录是否存在，源码中，是这样判断的，
  // 如果写入的父目录已经存在的话，则抛出异常
  // * 在这里我们冲写这个方法，修改文件目录的判断方式，
  // 如果传入的文件写入目录已存在的话，直接将其设置为输出目录即可，不会抛出异常

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    var outDir: Path = FileOutputFormat.getOutputPath(job)
    if (outDir != null) {
      //注意下面的这两句，如果说你要是写入文件的路径是hdfs的话，
      // 下面的两句不要写，或是注释掉，它俩的作用是标准化文件输出目录，
      // 根据我的理解是，他们是标准化本地路径，写入本地的话，可以加上，
      // 本地路径记得要用file:///开头，比如file:///E:/a.txt
      //val fs: FileSystem = ignored
      //outDir = fs.makeQualified(outDir)
      FileOutputFormat.setOutputPath(job, outDir)
    }
  }

}


object outputdemo {


  def main(args: Array[String]): Unit = {
    //配置spark环境
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //配置输出文件不生成success文件
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val kvRDD: RDD[(Int, String)] = sc.parallelize(Seq((1, "zhansan")))
    //配置一些参数
    //    kvRDD.saveAsHadoopFile()

    //如果设置为true，sparkSql将会根据数据统计信息，自动为每一列选择单独的压缩编码方式
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")

    //控制列式缓存批量的大小。增大批量大小可以提高内存的利用率和压缩率，但同时也会带来OOM的风险
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "10485760")

    //设为true，则启用优化的Tungsten物理执行后端。Tungsten会显示的管理内存，并动态生成表达式求值得字节码
    sqlContext.setConf("spark.sql.tungsten.enabled", "true")

    //配置shuffle是的使用的分区数
    sqlContext.setConf("spark.sql.shuffle.partitions", "200")

    sc.setLogLevel("WARN")
    //    val pro = new Properties()
    //    pro.put("user", "root")
    //    pro.put("password", "123456")
    //    pro.put("driver", "com.mysql.jdbc.Driver")
    //    val url = "jdbc:mysql://localhost:3306/test?serverTimezone=UTC"

    cusom_output(sc)
  }

  def cusom_output(sc: SparkContext): Unit = {

    //创建数据源
    //创建数据源
    val personRDD: RDD[(String, Int)] = sc.parallelize(Seq(
      ("class_01", 55),
      ("class_02", 85),
      ("class_02", 90),
      ("class_01", 75),
      ("class_01", 68),
      ("class_03", 65)
    ))
    val res = personRDD.groupByKey(3)
    res.foreach(println(_))
    res
      .partitionBy(new HashPartitioner(3))
      .saveAsHadoopFile("E:\\Server\\05_Spark\\spark\\day10\\spark_11(2)\\spark_11\\mytest", classOf[String], classOf[String], classOf[spark_Custom_outputformat])

  }
}
