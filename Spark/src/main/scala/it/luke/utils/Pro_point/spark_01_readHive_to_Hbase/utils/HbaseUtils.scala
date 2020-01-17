package it.luke.utils.Pro_point.spark_01_readHive_to_Hbase.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.Row

object HbaseUtils {


  def create_Table(htableName: String, family: String) = {

    //连接hbase集群
    val configuration: Configuration = HBaseConfiguration.create();
    //指定configuration 的zk连接地址
    configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
    //获取连接 通过 connectionFactory
    val connection: Connection = ConnectionFactory.createConnection(configuration)
    //创建表需要管理员对象
    val admin: Admin = connection.getAdmin
    //通过管理员对象创建表
    val htabledescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(htableName))

    //添加列族
    val f1: HColumnDescriptor = new HColumnDescriptor(family)
    //将两个列族设置到  hTableDescriptor里面去
    htabledescriptor.addFamily(f1)
    //创建表
    admin.createTable(htabledescriptor)
    admin.close

  }

  //判断表是否存在
  def table_Exists(tableName: String) = {
    //连接hbase集群
    val configuration: Configuration = HBaseConfiguration.create();
    //指定configuration 的zk连接地址
    configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
    //获取连接 通过 connectionFactory
    val connection: Connection = ConnectionFactory.createConnection(configuration)

    //创建表需要管理员对象

    val admin: Admin = connection.getAdmin
    val bool = admin.tableExists(TableName.valueOf(tableName))

    admin.close()
    bool
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
