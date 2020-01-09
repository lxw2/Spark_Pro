package it.luke.rdd.spark_01_trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TransformationObject {

  val sc = new SparkContext(new SparkConf().setMaster("local[5]").setAppName("TransformationObject"))

  @Test
  def mapPartitions(): Unit ={

    val rdd1: RDD[Int] = sc.parallelize(Seq[Int](2,3,4,5,6,6,7,8),2)
    //[2,3,4,5,6,6,7,8]
    //[(2,"name",age),(...),...]
    /*rdd1.map(id=>{

      var connection:Connection = null
      var statement:PreparedStatement = null
      try{
        connection = DriverManager.getConnection("...")
        statement = connection.prepareStatement("select id,name,age from table where id=?")
        statement.setInt(1,id)
        val resultSet = statement.executeQuery()
        var name=""
        var age=0
        while (resultSet.next()){
          name = resultSet.getString("name")
          age = resultSet.getInt("age")
        }
        (id,name,age)
      }catch {
        case e:Exception=>(id,"",0)
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null){
          connection.close()
        }
      }
    })*/

    /*rdd1.mapPartitions(it=>{
      var connection:Connection = null
      var statement:PreparedStatement = null
      var result:List[(Int,String,Int)] = List[(Int,String,Int)]()
      try{
        connection = DriverManager.getConnection("...")
        statement = connection.prepareStatement("select id,name,age from table where id=?")
        var i = 0
        while (it.hasNext){
          val id = it.next()
          statement.setInt(1,id)
          val resultSet = statement.executeQuery()
          var name=""
          var age=0
          while (resultSet.next()){
            name = resultSet.getString("name")
            age = resultSet.getInt("age")
          }
          result = result .+:((id,name,age))
        }
      }catch {
        case e:Exception => result.toIterator
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null){
          connection.close()
        }
      }

      result.toIterator
    })*/

    rdd1.mapPartitions(it=>{
      it.map(_*10)
    }).collect()
      .foreach(println(_))
  }

  @Test
  def mapPartitionsWithIndex(): Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq[Int](2,3,4,5,6,6,7,8),2)

    rdd1.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index}  data:${it.toBuffer}")
      it
    }).collect()
      .foreach(println(_))
  }

  /**
    * 一般用于数据倾斜
    *    withReplacement:采样之后，数据是否放回继续采样，true:放回  false:不放回
    *    fraction:
    *         如果withReplacement=true,每个元素期望被抽取的次数[>=0]
    *         如果是withReplacement=false,每个元素被采样的概率[0-1]
    */
  @Test
  def sample(): Unit ={

    val rdd1: RDD[Int] = sc.parallelize(Seq[Int](2,3,4,5,6,6,7,8,9,10),2)

    rdd1.sample(false,0.2).foreach(println(_))

    /**
      * [2,3,4,5,6,6,7,8,9,10]
      *  true:
      *    第一次采样: 3
      *    下一次采样的数据集:[2,3,4,5,6,6,7,8,9,10]
      *  false
      *    第一次采样: 3
      *    下一次采样的数据集:[2,4,5,6,6,7,8,9,10]
      */
  }

  @Test
  def mapValues(): Unit ={
    val data = Seq[(String,Int)](("aa",1),("bb",2),("cc",3))

    val rdd1 = sc.parallelize(data)

    rdd1.mapValues(value=> {
      println(value+" --------")
      value * 10
    }).collect().foreach(println(_))
  }

  /**
    * groupByKey与reduceByKey的区别：
    *   reduceByKey在map端combiner,combiner之后发到reduce端的数据变少，减少了IO
    *   groupByKey没有在map端combiner
    */
  @Test
  def goupByKey(): Unit ={

    val data = Seq[(Int,String,String,Int)](
      (1,"aa","shenzhen",13),
      (2,"bb","shanghai",15),
      (3,"cc","shenzhen",17),
      (4,"dd","shenzhen",12),
      (5,"ee","shanghai",20),
      (6,"ff","shanghai",30),
      (7,"tt","beijing",34),
      (7,"tt","beijing",23),
      (7,"tt","beijing",26),
      (7,"tt","beijing",28)
    )

    val rdd1 = sc.parallelize(data)

    rdd1.map(item=>(item._3,item._4))
      .groupByKey()
      .foreach(println(_))
  }

  @Test
  def reduceByKey(): Unit ={
    val data = Seq[(Int,String,String,Int)](
      (1,"aa","shenzhen",13),
      (2,"bb","shanghai",15),
      (7,"tt","beijing",34),
      (7,"tt","beijing",23),
      (3,"cc","shenzhen",17),
      (4,"dd","shenzhen",12),
      (5,"ee","shanghai",20),
      (6,"ff","shanghai",30),
      (7,"tt","beijing",26),
      (7,"tt","beijing",28)
    )

    val rdd1 = sc.parallelize(data,2)

    val rdd2 = rdd1.map(item=>(item._3,item._4))
    //[(shenzhen,13),(shanghai,15),......]

/*    rdd2.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index}  data:${it.toBuffer}")
      it
    }).collect()*/
    /**1、在每个分区上分组，combiner
      *         index:0  data:ArrayBuffer((shenzhen,13), (shanghai,15), (beijing,34), (beijing,23), (shenzhen,17))
      *                    1、分组
      *                       [
      *                         (shenzhen,[13,17]),
      *                           2、combiner
      *                               (agg,curr)=>agg+curr
      *                               第一次计算: agg:13  curr:17   =>30
      *                         (shanghai,[15]),
      *                           2、combiner
      *                             (agg,curr)=>agg+curr   =>15
      *                         (beijing,[34,23])
      *                            2、combiner
      *                              (agg,curr)=>agg+curr
      *                              第一次计算: agg:34  curr:23   =>57
      *                       ]
      *                   combiner结果: [(shenzhen,30),(shanghai,15),(beijing,57)]
      *
      *
      *         index:1  data:ArrayBuffer((shenzhen,12), (shanghai,20), (shanghai,30), (beijing,26), (beijing,28))
      *                   1、分组
      *                     [
      *                       (shenzhen,[12]),
      *                         2、combiner
      *                                (agg,curr)=>agg+curr  =>12
      *                       (shanghai,[20,30]),
      *                         2、combiner
      *                              (agg,curr)=>agg+curr
      *                              第一次计算: agg:20  curr:30   =>50
      *                       (beijing,[26,28])
      *                         2、combiner
      *                              (agg,curr)=>agg+curr
      *                              第一次计算: agg:26  curr:28   =>54
      *                     ]
      *                   2、combiner结果: [shenzhen->12,shanghai->50,beijing->54]
      *
      *
      *
      * 2、将每个分区的combiner结果再次分组，聚合
      *           将每个分区的数据发送到下一个RDD的对应分区，发送规则: key % 分区数 = 分区号
      *           index:0  [(beijing,57),(beijing,54)]
      *               分组：
      *                 [
      *                   (beijing,[57,54])
      *                       聚合:(agg,curr)=>agg+curr
      *                         第一次计算:agg=57 curr=54  =>111
      *                 ]
      *           index:1  [(shenzhen,30),(shenzhen,12),(shanghai,15),(shanghai,50)]
      *              分组:
      *              [
      *                (shenzhen,[30,12])
      *                   聚合:(agg,curr)=>agg+curr
      *                       第一次计算:agg=30 curr=12  =>42
      *                (shanghai,[15,50])
      *                   聚合:(agg,curr)=>agg+curr
      *                       第一次计算:agg=15 curr=50  =>65
      *              ]
      */
    val rdd3 = rdd2.reduceByKey((agg,curr)=>{
      println(s"agg:${agg}  curr:${curr}")
      agg+curr
    })

    /**combiner:
      * agg:34  curr:23
      * agg:20  curr:30
      * agg:13  curr:17
      * agg:26  curr:28
      * reduce:
      * agg:15  curr:50
      * agg:57  curr:54
      * agg:30  curr:12
      * 结果:
      * (beijing,111)
      * (shenzhen,42)
      * (shanghai,65)
      */
    /**
      * index:0  data:ArrayBuffer((beijing,111))
      * index:1  data:ArrayBuffer((shenzhen,42), (shanghai,65))
      */
    /*rdd3.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index}  data:${it.toBuffer}")
      it
    }).collect()*/
    rdd3.collect().foreach(println(_))

  }


  @Test
  def combinerByKey(): Unit ={
    val data = Seq[(String,Double)](
      ("zhangsan",10),
      ("lisi",25),
      ("zhangsan",20),
      ("lisi",35),
      ("zhangsan",30),
      ("lisi",45),
      ("zhangsan",40),
      ("lisi",55),
      ("zhangsan",50),
      ("lisi",65),
      ("zhangsan",60),
      ("lisi",75)
    )

    val rdd1 = sc.parallelize(data,2)

    rdd1.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index} data:${it.toBuffer}")
      it
    }).collect()
    /**
      * createCombiner: V => C,  针对每个分区每个key的第一个value值进行转换
      * mergeValue: (C, V) => C, 针对每个分区的每个key的value值进行combiner
      * mergeCombiners: (C, C) => C 针对每个分区combiner结果进行reduce
      */
    val rdd2 = rdd1.combineByKey((value:Double)=>{
      println(s"第一个函数:${value}")
      (value,1)
    },
      (agg:(Double,Int),curr:Double)=>{
        println(s"第二个函数: agg:${agg}  curr:${curr}")
        (agg._1+curr,agg._2+1)
      },
      (agg:(Double,Int),curr:(Double,Int))=>{
        println(s"第三个函数： agg:${agg}  curr:${curr}")
        (agg._1+curr._1,agg._2+curr._2)
      })

    rdd2.collect()

    //rdd1.combineByKey(x=>x,(agg:Double,curr:Double)=>agg+curr,(agg:Double,curr:Double)=>agg+curr)

  }

  /**
    * 初始值：在每一个分区的每一个key上作用一次
    */
  @Test
  def foldByKey(): Unit ={
    val rdd1 = sc.parallelize(Seq(("a",1),("a",1),("b",1)),2)
    rdd1.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index}  data:${it.toBuffer}")
      it
    }).collect()
    rdd1.foldByKey(10)(_+_).foreach(println(_))
  }


  @Test
  def aggragateByKey(): Unit ={
    val rdd1 = sc.parallelize(Seq(("a",10),("a",40),("a",20),("b",30)),2)

    /**
      * index0
      *   ("a",10),("a",40)
      *    第一个函数: agg:0.8 curr:10  = 8
      *    第一个函数: agg:8.0 curr:40  = 320
      * index1
      *   ("a",20),("b",30)
      *   第一个函数: agg:0.8 curr:20  = 16
      *   第一个函数: agg:0.8 curr:30  = 24
      *
      * index0：
      *   （"a",320）("a",16)
      *    第二个函数: agg:320.0 curr:16.0
      * index1:
      *   ("b",24)
      *
      *
      */
    /**
      * zeroValue：使用在每一个分区的每个key,只使用一次
      * seqOp：作用在每一个分区的每个key进行combiner,在每一个分区的每一个key进行第一次计算的时候,agg就是初始值
      * combOp:对每个分区的combiner结果进行reduce
      */
    rdd1.aggregateByKey(0.8)((agg,curr)=>{
      println(s"第一个函数: agg:${agg} curr:${curr}")
      agg*curr
    },
      (agg,curr)=>{
        println(s"第二个函数: agg:${agg} curr:${curr}")
        agg+curr
      }).collect().foreach(println(_))
    //
    //(b,24.0)
    //(a,24.0)
  }

  @Test
  def join(): Unit ={
    val student = sc.parallelize(Seq[(Int,String,Double,String)](
      (1,"aa",55,"class_01"),
      (2,"bb",85,"class_02"),
      (3,"cc",90,"class_02"),
      (4,"dd",75.5,"class_01"),
      (5,"ee",68,"class_01"),
      (6,"ff",65,"class_05")
    ))

    val clazz = sc.parallelize(Seq[(String,String)](
      ("class_01","java基础班"),
      ("class_02","大数据"),
      ("class_03","python基础班")
    ))

    //需求:获取每个班级的平均分
    /**
      * select c.className,avg(s.score)
      *   from clazz c left join student s
      *   on c.classId = s.classId
      *   group by c.className
      */
    val scoreRdd = student.map(item=>(item._4,item._3))
    //(classId,className) join (classId,score) => (classId,(className,Option(score)))
    val rdd: RDD[(String, (String, Option[Double]))] = clazz.leftOuterJoin(scoreRdd)

    /**
      * [(class_02,(大数据,Some(85.0)))
      * (class_02,(大数据,Some(90.0)))
      * (class_03,(python基础班,None))
      * (class_01,(java基础班,Some(55.0)))
      * (class_01,(java基础班,Some(75.5)))
      * (class_01,(java基础班,Some(68.0)))]
      */
    //reduceByKey,groupByKey,combinerByKey,foldByKey,aggragateByKey
    rdd.combineByKey((item:(String,Option[Double]))=>(item,1),(agg:((String,Option[Double]),Int),curr:(String,Option[Double]))=>{
      val className: String = agg._1._1
      val score_sum: Double = agg._1._2.getOrElse(0.0) + curr._2.getOrElse(0.0)
      val num: Int = agg._2+1
      ((className,Some(score_sum)),num)
    },(agg:((String,Option[Double]),Int),curr:((String,Option[Double]),Int))=>{
      val className = agg._1._1
      val score_sum = agg._1._2.getOrElse(0.0) + curr._1._2.getOrElse(0.0)
      val num = agg._2+curr._2
      ((className,Some(score_sum)),num)
    }).map {
      case (classId,((className,score),num)) =>
        (className,score.getOrElse(0.0)/num)
    }.foreach(println(_))

    /*val createCombiner =  (item:(String,Option[Double])) => (item._1,item._2,1)
    val combiner = (agg:(String,Option[Double],Int),curr:(String,Option[Double])) => {
      val className = agg._1
      val score = agg._2.getOrElse(0.0) + curr._2.getOrElse(0.0)
      val num = agg._3+1
      (className,Some(score),num)
    }

    val merge = (agg:(String,Option[Double],Int),curr:(String,Option[Double],Int)) =>{
      val className = agg._1
      val score = agg._2.getOrElse(0.0)  + curr._2.getOrElse(0.0)
      val num = agg._3 + curr._3
      (className,Some(score),num)
    }

    rdd.combineByKey(createCombiner,combiner,merge)
      .map(item=>(item._1,item._2._2.getOrElse(0.0)/item._2._3))
      .collect().foreach(println(_))*/

  }

  /**
    * coalease默认不shuffle，只能减少分区，一般搭配 filter使用。
    * repartition既可以增大分区也可减少分区
    */
  @Test
  def partitions(): Unit ={
    val student = sc.parallelize(Seq[(Int,String,Double,String)](
      (1,"aa",55,"class_01"),
      (2,"bb",85,"class_02"),
      (3,"cc",90,"class_02"),
      (4,"dd",75.5,"class_01"),
      (5,"ee",68,"class_01"),
      (6,"ff",65,"class_05")
    ),2)

    student.repartition(3)
    student.coalesce(3,true)
  }


}
