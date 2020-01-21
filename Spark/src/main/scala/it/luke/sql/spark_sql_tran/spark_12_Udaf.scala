package it.luke.sql.spark_sql_tran

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义udaf函数，也就是聚合函数
  *
  * 需求:获取每种商品的平均价格
  */
class Udaf extends UserDefinedAggregateFunction{

  //inputSchema --传入的参数的数据类型
  //bufferSchema --缓冲区的定义 定义中间变量的类型:
  //dataType --udaf函数返回值的数据类型
  //deterministic --数据一致性保证，一般设置为true
  //initialize --初始化缓冲区中间变量
  //update --每传进来一个数据，就更新缓冲区的中间变量
  //merge --合并所有分区的结果
  //evaluate --返回最终结果
  /**
    * 传入的参数的数据类型
    * @return
    */
  override def inputSchema: StructType = {
    StructType(
      List(StructField("input",IntegerType))
    )
  }

  /**
    * 缓冲区的定义
    *   定义中间变量的类型:
    *
    *   商品价格总和:sum
    *   商品个数:total
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(
      List(StructField("sum",IntegerType),
        StructField("total",IntegerType)
      )
    )
  }

  /**
    * udaf函数返回值的数据类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 数据一致性保证，一般设置为true
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 初始化缓冲区中间变量
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //初始化sum=0
    buffer(0) = 0
    //初始化total=0
    buffer(1) = 0
  }

  /**
    * 每传进来一个数据，就更新缓冲区的中间变量
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //更新sum
    buffer(0) = buffer.getAs[Int](0) + input.getAs[Int](0)
    //更新total
    buffer(1) = buffer.getAs[Int](1) + 1
  }

  /**
    * 合并所有分区的结果
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //合并sum
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    //合并total
    buffer1(1) = buffer1.getAs[Int](1) + buffer2.getAs[Int](1)
  }

  /**
    * 返回最终结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Double = {
    val result: Int = buffer.getAs[Int](0)/buffer.getAs[Int](1)
    result.toDouble
  }
}

