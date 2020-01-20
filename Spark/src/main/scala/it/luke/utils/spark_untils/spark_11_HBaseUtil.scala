package it.luke.utils.spark_untils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * HBase的工具类
  *
  * 获取Table
  * 保存单列数据
  * 查询单列数据
  * 保存多列数据
  * 查询多列数据
  * 删除数据
  */
object spark_11_HBaseUtil {

  // HBase的配置类, 不需要指定配置文件名,文件名要求是hbase-site.xml
  val conf: Configuration = HBaseConfiguration.create()

  // HBase的连接
  val conn: Connection = ConnectionFactory.createConnection(conf)

  // HBase的操作API
  val admin: Admin = conn.getAdmin


  /**
    * 返回table,如果不存在,则创建表
    *
    * @param tableNameStr
    * @param columnFamilyName
    * @return
    */
  def getTable(tableNameStr: String, columnFamilyName: String): Table = {

    // 获取TableName
    val tableName: TableName = TableName.valueOf(tableNameStr)

    // 如果表不存在,则创建表
    if (!admin.tableExists(tableName)) {

      // 构建出 表的描述的建造者
      val descBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)

      val familyDescriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes).build()
      // 给表去添加列族
      descBuilder.setColumnFamily(familyDescriptor)

      // 创建表
      admin.createTable(descBuilder.build())
    }

    conn.getTable(tableName)
  }

  /**
    * 存储单列数据
    *
    * @param tableNameStr     表名
    * @param rowkey           rowkey
    * @param columnFamilyName 列族名
    * @param columnName       列名
    * @param columnValue      列值
    */
  def putData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnName: String, columnValue: String) = {

    // 获取表
    val table: Table = getTable(tableNameStr, columnFamilyName)

    try {
      // Put
      val put: Put = new Put(rowkey.getBytes)
      put.addColumn(columnFamilyName.getBytes, columnName.getBytes, columnValue.getBytes)

      // 保存数据
      table.put(put)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      table.close()
    }
  }

  /**
    * 通过单列名获取列值
    *
    * @param tableNameStr     表名
    * @param rowkey           rowkey
    * @param columnFamilyName 列族名
    * @param columnName       列名
    * @return 列值
    */
  def getData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnName: String): String = {

    // 1.获取Table对象
    val table = getTable(tableNameStr, columnFamilyName)

    try {

      // 2. 构建Get对象
      val get = new Get(rowkey.getBytes)

      // 3. 进行查询
      val result: Result = table.get(get)

      // 4. 判断查询结果是否为空,并且包含我们要查询的列

      if (result != null && result.containsColumn(columnFamilyName.getBytes, columnName.getBytes)) {
        val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes(), columnName.getBytes)

        Bytes.toString(bytes)
      } else {
        ""
      }

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        ""
      }
    } finally {
      // 5. 关闭表
      table.close()
    }

  }

  /**
    * 存储多列数据
    *
    * @param tableNameStr     表名
    * @param rowkey           rowkey
    * @param columnFamilyName 列族名
    * @param map              多个列名和列值集合
    */
  def putMapData(tableNameStr: String, rowkey: String, columnFamilyName: String, map: Map[String, Any]) = {

    // 1. 获取Table
    val table = getTable(tableNameStr, columnFamilyName)

    try {

      // 2. 创建Put
      val put = new Put(rowkey.getBytes)

      // 3. 在Put中添加多个列名和列值
      for ((colName, colValue) <- map) {
        put.addColumn(columnFamilyName.getBytes, colName.getBytes, colValue.toString.getBytes)
      }

      // 4. 保存Put
      table.put(put)

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      // 5. 关闭表
      table.close()
    }

  }


  /**
    * 获取多列数据的值
    *
    * @param tableNameStr     表名
    * @param rowkey           rowkey
    * @param columnFamilyName 列族名
    * @param columnNameList   多个列名
    * @return 多个列名和多个列值的Map集合
    */
  def getMapData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnNameList: List[String]): Map[String, String] = {

    // 1. 获取Table
    val table = getTable(tableNameStr, columnFamilyName)

    try{
    // 2. 构建Get
    val get = new Get(rowkey.getBytes)

    // 3. 执行查询
    val result: Result = table.get(get)

    // 4. 遍历列名集合,取出列值,构建成Map返回
    columnNameList.map {
      col =>
        val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes(), col.getBytes)

        if (bytes != null && bytes.size > 0) {
          col -> Bytes.toString(bytes)
        }else{
          ""->""
        }
    }.filter(_._1!="").toMap

    }catch{
      case ex:Exception=>{
        ex.printStackTrace()
        Map[String,String]()
      }
    }finally {
      // 5. 关闭Table
      table.close()
    }
  }

  /**
    * 删除数据
    * @param tableNameStr
    * @param rowkey
    * @param columnFamilyName
    */
  def deleteData(tableNameStr:String,rowkey:String,columnFamilyName:String)={

    // 1. 获取Table
    val table:Table = getTable(tableNameStr,columnFamilyName)

    try{
      // 2. 构建Delete对象
      val delete:Delete = new Delete(rowkey.getBytes)

      // 3. 执行删除
      table.delete(delete)
    }catch {
      case ex:Exception=>
        ex.printStackTrace()
    }finally {
      // 4. 关闭table
      table.close()
    }
  }



  def main(args: Array[String]): Unit = {

        println(getTable("test", "info"))

        putData("test","1","info","t1","hello world")

//    println(getData("test", "1", "info", "t1"))
//
//
//    val map = Map(
//      "t2" -> "scala",
//      "t3" -> "hive",
//      "t4" -> "flink"
//    )
//
//    putMapData("test", "1", "info", map)
//    deleteData("test","1","info")

    println(getMapData("test","1","info",List("t1","t2")))

    val stringToString: Map[String, String] = getMapData("channel_freshness","8:2019052118","info",List("newCount","oldCount"))

    println(stringToString)

  }

}















