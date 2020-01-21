## 代码

### rdd

#### spark_01_trans

整理了关于sparkcore中常用的几种算子,(包括了转换算子,action算子等)

1. map

2. flatmap

3. fliter

4. mapPartitions

5. mapPartitionsWithIndex

6. sample

7. union

8. intersection

9. subtract

10. distinct

11. glom

12. takesample

13. mapvalue

14. reduceBykey

15. groupBykey

16. aggregateBykey

17. foldBykey

18. join

19. leftOutjoin

20. cogroup 

21. cartesian

22. sortBy

23. paritionBy

24. coalesce

25. repartition

    

### SparkSQL

### sparkSql read_writer

1. 使用 `spark.read` 可以获取 SparkSQL 中的外部数据源访问框架 `DataFrameReader`
2. `DataFrameReader` 有三个组件 `format`, `schema`, `option`
3. `DataFrameReader` 有两种使用方式, 一种是使用 `load` 加 `format` 指定格式, 还有一种是使用封装方法 `csv`, `json` 等



1. 类似 `DataFrameReader`, `Writer` 中也有 `format`, `options`, 另外 `schema` 是包含在 `DataFrame` 中的
2. `DataFrameWriter` 中还有一个很重要的概念叫做 `mode`, 指定写入模式, 如果目标集合已经存在时的行为
3. `DataFrameWriter` 可以将数据保存到 `Hive` 表中, 所以也可以指定分区和分桶信息
4. `DataFrameWriter` 也有两种使用方式, 一种是使用 `format` 配合 `save`, 还有一种是使用封装方法, 例如 `csv`, `json`, `saveAsTable` 等



### sparkSql tran

1. spark_Columnsel :针对字段的选择,对DF/DS获取列对象
2. spark_Get DS/DF :针对获取DF 和 DS 进行整理
3. spark_select :整理使用select api
4. spark_filter :整理使用filter api
5. spark_join :整理使用join api
6. spark_group : 整理使用group api
7. spark_sort :整理使用 sort api
8. spark_as :整理使用as api
9. spark_distinct: 整理使用 distinct api
10. spark_agg :整理是使用 agg聚合函数的api
11. spark_column : 对添加字段,列进行简单整理
12. spark_udf:整理注册udf的简单过程
13. spark_udaf:整理创建UDAF的简单过程
14. spark_window: 整理 使用窗口函数的要点
15. spark_NUllprocess:整理对空值处理的要点

### SparkStreaming



### utils(工具类)

#### 针对一些通过spark或间接通过spark(与spark相关)的工具类

#### spark_utils
1. spark_01_metrics--添加监控功能
2. spark_02_read_Nearly30day --通过空是读取文件的方法进行限制
3. spark_03_read_dir--通过filesystem写的简单文件系统工具(可扩展)
4. spark_04_Custom_outputform -- 自定义的文件输出format,可以自定义输出文件名
5. spark_05_readHive_to_Hbase -- 读取hive数据存入hbase的工具


#### Pro_point
提炼的技术要点




