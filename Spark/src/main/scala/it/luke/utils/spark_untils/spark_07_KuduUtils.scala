package it.luke.utils.spark_untils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

object spark_07_KuduUtils {
  /**
    * 数据写入kudu
    */
  def write(context:KuduContext,tableName:String,
            schema:StructType,
            keys:Seq[String],
            options:CreateTableOptions,data:DataFrame)={
    //如果表存在，删除
    if(context.tableExists(tableName)){
      context.deleteTable(tableName)
    }
    context.createTable(tableName,schema,keys,options)

    //写入数据
    data.write
      .mode(SaveMode.Append)
      .option("kudu.master",spark_08_ConfigUtils.MASTER_ADDRESS)
      .option("kudu.table",tableName)
      .kudu
  }


  def businessWrite(context:KuduContext,tableName:String,
            schema:StructType,
            keys:Seq[String],
            options:CreateTableOptions,data:DataFrame)={
    //如果表存在，删除
    if(!context.tableExists(tableName)){
      context.createTable(tableName,schema,keys,options)
    }

    //写入数据
    data.write
      .mode(SaveMode.Append)
      .option("kudu.master",spark_08_ConfigUtils.MASTER_ADDRESS)
      .option("kudu.table",tableName)
      .kudu
  }
}
