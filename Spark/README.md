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




