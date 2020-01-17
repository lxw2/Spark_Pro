package it.luke.utils.Pro_point.spark_01_readHive_to_Hbase


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import utils.HbaseUtils


object ReadHiveData {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    //1.1胚子metastore的uri
    //1.2 配置warehouse的路径
    //1.3 开启hive支持
    val spark = SparkSession.builder()
      .appName("readhive")
      .master("local[4]")
      .config("hive.metastore.uris", "thrift://node02:9083")
      .config("spark.sql.warehouse.dir", "hdfs://node01:8020/user/hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    val res = spark.sql(
      """
        |select * from
        |mytest.accs_voucher_b where dt =20200114
      """.stripMargin)

    res.show()
    res.printSchema()

    //    val rdd: RDD[Row] = res.rdd
    //    rdd.map(item=>{
    //      val id = item.getAs[String]("id")
    //      id
    //    }).foreach(println(_))


    //存入hbase

    //初始化rowkey 列族
    var rowkey = ""
    var value = ""
    val family = "info"


    val dataRdd: RDD[(String, (String, String, String))] = res.rdd.flatMap((row: Row) => {
      val ID = nullHandle(row.getAs[String]("id"))
      println(ID)
      if (!"null".equals(ID)) {
        val rowkey = getRowkey(row)
        Array(
          (rowkey, ("info", "id", nullHandle(row.getAs[String]("id")))),
          (rowkey, ("info", "voucher_h_id", nullHandle(row.getAs[String]("voucher_h_id")))),
          (rowkey, ("info", "fund_id", nullHandle(row.getAs[String]("fund_id")))),
          (rowkey, ("info", "biz_date", nullHandle(row.getAs[String]("biz_date")))),
          (rowkey, ("info", "entry_no", nullHandle(row.getAs[String]("entry_no")))),
          (rowkey, ("info", "eval_code_id", nullHandle(row.getAs[String]("eval_code_id")))),
          (rowkey, ("info", "eval_code", nullHandle(row.getAs[String]("eval_code")))),
          (rowkey, ("info", "eval_name", nullHandle(row.getAs[String]("eval_name")))),
          (rowkey, ("info", "code_main_type", nullHandle(row.getAs[String]("code_main_type")))),
          (rowkey, ("info", "code_property", nullHandle(row.getAs[String]("code_property")))),
          (rowkey, ("info", "code_sub_type", nullHandle(row.getAs[String]("code_sub_type")))),
          (rowkey, ("info", "code_attribute", nullHandle(row.getAs[String]("code_attribute")))),
          (rowkey, ("info", "subject_code_id", nullHandle(row.getAs[String]("subject_code_id")))),
          (rowkey, ("info", "subject_code", nullHandle(row.getAs[String]("subject_code")))),
          (rowkey, ("info", "subject_name", nullHandle(row.getAs[String]("subject_name")))),
          (rowkey, ("info", "out_security_cd", nullHandle(row.getAs[String]("out_security_cd")))),
          (rowkey, ("info", "security_id", nullHandle(row.getAs[String]("security_id")))),
          (rowkey, ("info", "security_cd", nullHandle(row.getAs[String]("security_cd")))),
          (rowkey, ("info", "security_name", nullHandle(row.getAs[String]("security_name")))),
          (rowkey, ("info", "0ut_assistant_acc", nullHandle(row.getAs[String]("0ut_assistant_acc")))),
          (rowkey, ("info", "out_assistant_acc_value", nullHandle(row.getAs[String]("out_assistant_acc_value")))),
          (rowkey, ("info", "contract_no", nullHandle(row.getAs[String]("contract_no")))),
          (rowkey, ("info", "ite_collection_code", nullHandle(row.getAs[String]("ite_collection_code")))),
          (rowkey, ("info", "summary", nullHandle(row.getAs[String]("summary")))),
          (rowkey, ("info", "curr_type_id", nullHandle(row.getAs[String]("curr_type_id")))),
          (rowkey, ("info", "debit_quantity", nullHandle(row.getAs[String]("debit_quantity")))),
          (rowkey, ("info", "debit_original_amount", nullHandle(row.getAs[String]("debit_original_amount")))),
          (rowkey, ("info", "debit_domestic_amount", nullHandle(row.getAs[String]("debit_domestic_amount")))),
          (rowkey, ("info", "credit_quantity", nullHandle(row.getAs[String]("credit_quantity")))),
          (rowkey, ("info", "credit_original_amount", nullHandle(row.getAs[String]("credit_original_amount")))),
          (rowkey, ("info", "credit_domestic_amount", nullHandle(row.getAs[String]("credit_domestic_amount")))),
          (rowkey, ("info", "assistant_acc", nullHandle(row.getAs[String]("assistant_acc")))),
          (rowkey, ("info", "remark", nullHandle(row.getAs[String]("remark")))),
          (rowkey, ("info", "eval_price", nullHandle(row.getAs[String]("eval_price")))),
          (rowkey, ("info", "create_id", nullHandle(row.getAs[String]("create_id")))),
          (rowkey, ("info", "create_time", nullHandle(row.getAs[String]("create_time")))),
          (rowkey, ("info", "lastmodifted_id", nullHandle(row.getAs[String]("lastmodifted_id")))),
          (rowkey, ("info", "lastmodified_time", nullHandle(row.getAs[String]("lastmodified_time")))),
          (rowkey, ("info", "isdelete", nullHandle(row.getAs[String]("isdelete")))),
          (rowkey, ("info", "convert_flag", nullHandle(row.getAs[String]("convert_flag"))))
        )
      } else {
        null
      }
    })

    //读取数据的测试打印
//    dataRdd.foreach(println(_))

    //4.对rowkey,行键,列族,列名整体排序,必须先排序后处理,防止数据异常过滤rowkey--后续map可以改为mapPartition
    val sortedRdds = dataRdd.filter(x => x._1 != null).sortBy(x => (x._1, x._2._1, x._2._2))
      .map(x => {
        //4.1将rdd转换为Hfile需要的格式,Hfile的key是ImmutableByteWritable,那么我们定义也是要以ImmutableBytesWritable的实例为value
        //keyvalue的实例为value
        val rowkey = Bytes.toBytes(x._1)
        val family = Bytes.toBytes(x._2._1)
        val colum = Bytes.toBytes(x._2._2)
        val value = Bytes.toBytes(x._2._3)
        (new ImmutableBytesWritable(rowkey), new KeyValue(rowkey, family, colum, value))
      })

    //5.设置临时文件保存的位置,在HDFS上
    val tmpHDFSDir = "hdfs://node01:8020/export/hiveoutdata/" // 也可以 /out/result/1

    //6.设置Hbase连接参数
    //
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181") //zookeeper集群节点
    //hconf2.set("hbase.zookeeper.property.clientPort", "2181") //zookeeper集群端口
    //    hconf2.set("zookeeper.znode.parent","/hbase-unsecure")
    hconf.set("hbase.mapreduce.hfileoutputformat.table.name","accs_voucher_b")
    //预防hfile文件数过多无法进行导入,可以设置该参数
    hconf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1000)


    //saveAsNewAPIHadoopFilec参数列表:
    // path: String,
    // keyClass: Class[_],
    // valueClass: Class[_],
    // outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
    // conf: Configuration = self.context.hadoopConfiguration
    //7.运行完以后,在tmpHDFSdir生成一个临时文件Hfile文件

    //测试 删除已存在的文件夹
//    utils.HadoopUtils.dir_Exists(tmpHDFSDir)
    sortedRdds.saveAsNewAPIHadoopFile(tmpHDFSDir, //path
      classOf[ImmutableBytesWritable], //keyClass
      classOf[KeyValue], //valueClass
      classOf[HFileOutputFormat2], //outputFormatClass
      hconf //conf
    )

    // 8.将Hfile文件导入到Hbase,此处为HbaseAPI操作
    val loadHfile = new LoadIncrementalHFiles(hconf)
    //8.1hbase的表名,和hivez中保持一样的表名
    val HTableName = "accs_voucher_b"
    //8.2创建Hbase连接,利用默认的配置文件,实际上读取hbase的master的地址
    val conn = ConnectionFactory.createConnection(hconf)
    //获取表对象
    var HTable: Table  = null
    //判断表是否存在,不存在则创建
    if (HbaseUtils.table_Exists(HTableName)) {
      HTable = conn.getTable(TableName.valueOf(HTableName))
    }
    else {
      //创建表,需要表名以及列族
      HbaseUtils.create_Table(HTableName,family)
      HTable = conn.getTable(TableName.valueOf(HTableName))
    }

    try {
      //8.4获取表的region分布,创建一个job,并且设置输出方式
      val regionLocation = conn.getRegionLocator(TableName.valueOf(HTableName))
      val job = Job.getInstance(hconf)//创建一个mrjob
      job.setJobName("readhive2hbase")//设置任务名字

      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])//输出key为ImmutableBytesWritable
      job.setMapOutputValueClass(classOf[KeyValue])//输出文件内容为KeyValue

      //8.5配置HfileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoad(job,HTable,regionLocation)
      //8.6开始批量导入
      loadHfile.doBulkLoad(new Path(tmpHDFSDir),conn.getAdmin,HTable,regionLocation)
    } finally {
      HTable.close()
      conn.close()
    }

    spark.close()
    //2.读取hive数据
    // spark.read.option("")//设置读取的文件的格式
  }
  /**
    * 用来判断读取的hive的字段是否为空
    * @param str
    * @return
    */
  def nullHandle(str: String): String = {
    if (str == null || "".equals(str)) {
      return "NULL"
    } else {
      return str
    }
  }

  /**
    * 生成rowkey的方法
    * @param line
    * @return
    */
  def getRowkey(line : Row): String= {
    //取出MD5码的前三位
    var c1=""
    var c2=""
    var c3=""
    var rowkey=""
    if(line.get(0)!=null){
      c1=line.get(0).toString
    }
    if(line.get(1)!=null){
      c2=line.get(1).toString
    }
    if(line.get(2)!=null){
      c3=line.get(2).toString
    }
    val row=c1+"_"+c2+"_"+c3
    //val rowkey=line.get(0).toString
    val rkid = Bytes.toBytes(row)
    rowkey=MD5Hash.getMD5AsHex(rkid).substring(0, 10)
    rowkey
  }
}
