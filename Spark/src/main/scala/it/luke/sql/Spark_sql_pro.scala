package it.luke.sql

import UDF.udf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class Spark_sql_pro {
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("demo")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  /**
    * 获取course表
    *
    * @return
    */
  def getCourse_table = {
    val course_schema = StructType(
      List(
        StructField("CID", StringType),
        StructField("Cname", StringType),
        StructField("TID", StringType)
      )
    )

    spark.read
      .schema(course_schema)
      .option("delimiter", "\t")
      .csv("sql/course.csv")

  }

  /**
    * 获取sc表
    *
    * @return
    */
  def getSc_table = {
    val sc_schema = StructType(
      List(
        StructField("SID", StringType),
        StructField("CID", StringType),
        StructField("score", DoubleType)
      )
    )
    spark.read
      .schema(sc_schema)
      .option("delimiter", "\t")
      .csv("sql/sc.csv")

  }

  /**
    * 获取student表
    *
    * @return
    */
  def getStudent_table = {
    val sc_schema = StructType(
      List(
        StructField("SID", StringType),
        StructField("Sname", StringType),
        StructField("Sage", StringType),
        StructField("Ssex", StringType)
      )
    )

    spark.read
      .schema(sc_schema)
      .option("delimiter", "\t")
      .csv("sql/student.csv")

  }

  /**
    * 获取teacher表
    *
    * @return
    */
  def getTeacher_table = {
    val sc_schema = StructType(
      List(
        StructField("TID", StringType),
        StructField("Tname", StringType)
      )
    )
    spark.read
      .schema(sc_schema)
      .option("delimiter", "\t")
      .csv("sql/teacher.csv")

  }

  /**
    * 1、查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    */
  @Test
  def sqlPro01 = {

    /*
    * a.*,b.`score` 01课程的分数,c.`score` 02课程的分数
        FROM
        student a ,sc b,sc c
        WHERE
        a.`SID`=b.`SID` AND b.`SID` =c.`SID` AND b.`CID`=01 AND c.`CID`=02 AND b.`score`>c.`score`;
    * */
    //获取数据
    val sc = getSc_table
    val sc1 = getSc_table //用于api 模式
    val student = getStudent_table
    //获取临时表
    sc.createOrReplaceTempView("sc1")
    sc.createOrReplaceTempView("sc2")
    student.createOrReplaceTempView("student")
    //需求:## 1、查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    //    sql
    spark.udf.register("fixID", udf.fixID _)
    spark.sql(
      """
        |select
        |student.* ,sc1.score ,sc2.score,fixID(student.SID)
        |from
        |student
        |join sc1
        |on
        |student.SID = sc1.SID and sc1.CID = '01'
        |left join
        |sc2
        |on
        |sc2.SID= sc1.SID and sc2.CID ='02'
        |where
        |sc2.score < sc1.score
      """.stripMargin).show()
    //api
    student.join(sc, (sc("SID") === student("SID") && sc("CID") === "01"), "inner")
      //.show()
      .join(sc1, (sc1("SID") === student("SID") && sc1("CID") === "02"), "left")
      .where(sc("score") > sc1("score"))
      .select(student("SID") as "id", sc("score") as "01score", sc1("score") as "02score")
      .selectExpr("fixID(id) as fix", "01score", "02score")


      .show()
  }

  /**
    * 1、查询"01"课程比"02"课程成绩低的学生的信息及课程分数
    */
  @Test
  def sqlPro02 = {
    /*
    * a.*,b.`score` 01课程的分数,c.`score` 02课程的分数
        FROM
        student a ,sc b,sc c
        WHERE
        a.`SID`=b.`SID` AND b.`SID` =c.`SID` AND b.`CID`=01 AND c.`CID`=02 AND b.`score`<c.`score`;
    * */
    //获取数据
    val sc = getSc_table
    val sc1 = getSc_table //用于api 模式
    val student = getStudent_table
    //获取临时表
    sc.createOrReplaceTempView("sc1")
    sc.createOrReplaceTempView("sc2")
    student.createOrReplaceTempView("student")
    //需求:## 1、查询"01"课程比"02"课程成绩低的学生的信息及课程分数
    //    sql
    spark.sql(
      """
        |select
        |student.* ,sc1.score ,sc2.score
        |from
        |student
        |join sc1
        |on
        |student.SID = sc1.SID and sc1.CID = '01'
        |left join
        |sc2
        |on
        |sc2.SID= sc1.SID and sc2.CID ='02'
        |where
        |sc2.score < sc1.score
      """.stripMargin).show()

    //api
    student.join(sc, (sc("SID") === student("SID") && sc("CID") === "01"), "inner")
      //.show()
      .join(sc1, (sc1("SID") === student("SID") && sc1("CID") === "02"), "left")
      .where(sc("score") < sc1("score"))
      .select(student("*"), sc("score") as "01score", sc1("score") as "02score")
      .show()
  }

  /**
    * 3、查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
    */
  @Test
  def sqlPro03: Unit = {

    /*
    * 3、查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
    *需求student表 sc表
    * */

    val sc = getSc_table
    val student = getStudent_table
    //创建临时表
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")
    //sql
    /*
    * select
      a.s_id,a.s_name,round(avg(b.s_score),2) as avg_score
      from student a
      join score b on a.s_id=b.s_id
      group by a.s_id,a.s_name
      ) t
      where t.avg_score>=60
    * */
    spark.sql(
      """
        |select
        |student.SID ,student.Sname, round(avg(sc.score),2) as avg_score
        |from
        |student
        |inner join
        |sc
        |on student.SID = sc.SID
        |group by student.SID ,student.Sname
        |having
        |avg(sc.score) >=60
      """.stripMargin).show()
    //  api
    import org.apache.spark.sql.functions._
    student.join(sc, (sc("SID") === student("SID")), "inner")
      // 多个分组字段可以通过 , 隔开
      .groupBy(student("SID"), student("Sname"))
      //agg的函数处理可以通过 as "重命名"
      .agg(round(avg(sc("score")), 2) as "avg_score")
      //可以通过where 来进行条件判断,在where中可以使用函数表达式 调用别名来进行函数计算
      .where("avg_score > 60").show()
    //     show 会根据你的分组 选择展示 已经处理的字段  SID NAME avg_score
  }

  /**
    * 4.查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩
    */
  @Test
  def sqlPro04: Unit = {

    /*
    ## 4.查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩
    * */
    //    获取表
    val student = getStudent_table
    val sc = getSc_table
    //创建临时表
    student.createOrReplaceTempView("student")
    sc.createOrReplaceTempView("sc")

    //sql
    spark.sql(
      """
        |select
        |student.SID , student.Sname , round(avg(sc.score),2) avg_score
        |from
        |student
        |inner join
        |sc
        |on
        |student.SID = sc.SID
        |group by
        |student.SID,student.Sname
        |having
        |avg_score <60
      """.stripMargin).show()
    //api
    //导入隐式函数
    import org.apache.spark.sql.functions._
    student.join(sc, sc("SID") === student("SID"), "inner")
      .groupBy(student("SID"), student("Sname"))
      .agg(round(avg(sc("score")), 2) as "avg_score")
      .where("avg_score < 60").show()
  }

  /**
    * 5、查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
    */
  @Test
  def sqlPro05: Unit = {
    /*
    * # 5、查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
    * */
    //获取表
    val student = getStudent_table
    val sc = getSc_table
    //获取临时表对象
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")
    //sql
    spark.sql(
      """
        |select
        |student.SID,student.Sname,count(sc.CID) Cnum,sum(sc.score) sumsc
        |from
        |student
        |inner join
        |sc
        |on student.SID = sc.SID
        |group by student.SID,student.Sname
      """.stripMargin).show()

    //api  导入隐式函数转换
    import org.apache.spark.sql.functions._
    student.join(sc, sc("SID") === student("SID"), "inner")
      .groupBy(student("SID"), student("Sname"))
      .agg(count(sc("CID")) as "Cnum", sum(sc("score")) as "sumsc")
      .show()
  }

  /**
    * 6查询"李"姓老师的数量
    */
  @Test
  def sqlPro06 = {
    /*
      ## 6查询"李"姓老师的数量
    * */

    //获取表
    val teacher = getTeacher_table
    //创建临时表
    teacher.createOrReplaceTempView("teacher")

    //sql
    spark.sql(
      """
        |select
        |teacher.Tname,count(1)
        |from
        |teacher
        |group by
        |teacher.Tname
        |having
        |teacher.Tname like 'Li%'
      """.stripMargin).show()

    //api
    import org.apache.spark.sql.functions._
    teacher
      .groupBy(teacher("Tname") as "Tname")
      .agg(count(teacher("Tname")) as "cname")
      .where("Tname like 'Li%'")
      //where 的表达式,需要设置别名才能引用  groupBy('') as "重命名"  where 重命名..
      .show()
  }

  /**
    * 7、查询学过"张三"老师授课的同学的信息
    */
  @Test
  def sqlPro07: Unit = {

    /*
    * ## 7、查询学过"张三"老师授课的同学的信息
    * */
    //获取表sc student teacher
    val student = getStudent_table
    val sc = getSc_table
    val teacher = getTeacher_table
    val course = getCourse_table
    //创建临时表的对象
    student.createOrReplaceTempView("student")
    sc.createOrReplaceTempView("sc")
    teacher.createOrReplaceTempView("teacher")
    course.createOrReplaceTempView("course")

    spark.sql(
      """
        |select
        |distinct student.* ,course.Cname , teacher.Tname
        |from
        |student
        |inner join
        |sc
        |on sc.SID = student.SID
        |inner join
        |course
        |on course.CID = sc.CID
        |inner join
        |teacher
        |on teacher.TID= course.TID and teacher.Tname = 'Zhansan'
        |order by student.SID
      """.stripMargin).show() //order by 进行排序

    //api
    student.join(sc, sc("SID") === student("SID"), "inner")
      .join(course, course("CID") === sc("CID"), "inner")
      .join(teacher, (teacher("TID") === course("TID") && teacher("Tname") === "Zhansan"), "inner")
      //      .join(teacher,teacher("TID") === course("TID"),"inner")
      .dropDuplicates("Sname") //最好是指定特殊名字
      .orderBy(student("SID"))
      .show()
    //      .distinct()
    spark.close()
  }

  /**
    * 8、查询没学过"张三"老师授课的同学的信息
    */
  @Test
  def sqlPro08: Unit = {

    /*
    * ## 8、查询没学过"张三"老师授课的同学的信息
    * */
    //获取表
    val teacher = getTeacher_table
    val student: DataFrame = getStudent_table
    val sc = getSc_table
    val course = getCourse_table
    //创建临时表
    teacher.createOrReplaceTempView("teacher")
    sc.createOrReplaceTempView("sc")
    sc.createOrReplaceTempView("sc2")
    student.createOrReplaceTempView("student")
    course.createOrReplaceTempView("course")


    spark.sql(
      """
        |select
        |distinct student.SID,student.Sname
        |from
        |student
        |inner join
        |sc
        |on sc.SID = student.SID
        |where
        |student.SID NOT in
        |(select distinct sc2.SID from sc2 inner join course on course.CID =sc2.CID
        |inner join teacher on teacher.TID = course.TID and teacher.Tname = 'Zhansan' )
      """.stripMargin).show()

    //api
    //因为由嵌套,所以先创建子 表 然后
    val zi = sc.join(course, course("CID") === sc("CID"), "inner")
      .join(teacher, (teacher("TID") === course("TID") && teacher("Tname") === "Zhansan"), "inner")
      .select(sc("SID") as "zSID")

    zi.createOrReplaceTempView("zi") //将处理完的子表进行注册 zi表
    zi.show()

    student.join(sc, sc("SID") === student("SID"), "inner")
      .select(student("SID") as "ssid", student("Sname") as "stuName")
      .dropDuplicates("stuName")
      .where("ssid not in (select * from zi)")
      .show()
  }

  /**
    * 9、查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
    */
  @Test
  def sqlPro09: Unit = {
    //## 9、查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
    //获取表
    val student = getStudent_table
    val sc = getSc_table
    //创建临时表对象
    student.createOrReplaceTempView("student")
    sc.createOrReplaceTempView("sc")
    //sql
    spark.sql(
      """
        |
      """.stripMargin)
  }

  /**
    * 10、查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
    */
  @Test
  def sqlPro10: Unit = {
    //SELECT student.*
    //	FROM student,sc
    //	WHERE student.SID=sc.SID AND sc.CID='01' AND
    //	      sc.SID NOT IN (SELECT  sc.SID FROM sc WHERE sc.CID='02')
    val student: DataFrame = getStudent_table
    val sc: DataFrame = getSc_table

    student.createOrReplaceTempView("student")
    sc.createOrReplaceTempView("sc")
    spark.sql(
      """
        |select student.*
        |from student , sc
        |where student.SID = sc.SID and sc.CID = '01' and
        |sc.SID NOT IN (select sc.SID from sc where sc.CID = '02')
      """.stripMargin).show(10)
  }


  /**
    * -- 11、查询没有学全所有课程的同学的信息
    */
  @Test
  def sqlPro11 = {

    //分析,需要课程表,学生表,sc表
    val student: DataFrame = getStudent_table
    student.show(10)
    val course: DataFrame = getCourse_table
    course.show(10)
    val sc: DataFrame = getSc_table
    sc.show(10)
    //注册成表
    student.createOrReplaceTempView("student")
    course.createOrReplaceTempView("course")
    sc.createOrReplaceTempView("sc")

    spark.sql(
      """
        |select
        |student.*
        |from  student
        |join
        |(select
        |sc.SID
        |from
        |sc
        |group by sc.SID
        |having count(sc.CID) != (select count(1) from course))scc
        |on scc.SID = student.SID
      """.stripMargin).show(10)
  }


  /**
    * -- 12、查询至少有一门课与学号为"01"的同学所学相同的同学的信息
    */
  @Test
  def sqlPro12 = {

    //分析:需要student,sc
    val sc = getSc_table
    val student = getStudent_table
    //注册
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")
    //编写sql
    spark.sql(
      """
        |select
        |student.*
        |from student , sc as score
        |where student.SID != 01
        |and student.SID = score.SID
        |and score.CID in
        |(select
        |sc.CID
        |from sc
        |where sc.SID = 01)
        |group by student.SID,student.Sname,student.Sage,Ssex
      """.stripMargin).show(10)

  }

  /**
    * -- 13、查询和"01"号的同学学习的课程完全相同的其他同学的信息
    */
  @Test
  def sqlPro13: Unit = {

    //分析 sc 表,student 表
    val sc: DataFrame = getSc_table
    val student: DataFrame = getStudent_table

    //注册表
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")
    //编写sql
    spark.sql(
      """
        |select
        |student.*
        |from student
        |join
        |sc score
        |on
        |score.SID = student.SID
        |where student.SID != 01
        |and score.CID in (select sc.CID from sc where sc.SID = 01)
        |group by student.SID,student.Sname,student.Sage,Ssex
        |having count(score.CID) = (select count(1) from sc where sc.SID = 01)
      """.stripMargin).show(10)

  }

  /**
    * 14、查询没学过"张三"老师讲授的任一门课程的学生姓名
    */
  @Test
  def sqlPro14: Unit = {

    //分析需要teacher student sc
    //找出张三教课的课程编号,学生join sc 求出课程编号不包含

    val teacher = getTeacher_table
    val sc = getSc_table
    val student = getStudent_table
    val course = getCourse_table
    //注册表
    teacher.createOrReplaceTempView("teacher")
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")
    course.createOrReplaceTempView("course")

    //编写sql

    //获取张三老师的课程
    val rescourse: DataFrame = spark.sql(
      """
        |select
        |course.CID
        |from
        |course
        |join
        |teacher
        |on
        |course.TID = teacher.TID
        |where
        |teacher.Tname = 'Zhansan'
      """.stripMargin)
    rescourse.show(10)
    rescourse.createOrReplaceTempView("rescourse")

    spark.sql("cache table rescourse")

    val resSID = spark.sql(
      """
        |select sc.SID from sc join rescourse on sc.CID = rescourse.CID group by sc.SID
      """.stripMargin)
    resSID.show()
    resSID.createOrReplaceTempView("resSID")
    spark.sql("cache table resSID")


    spark.sql(
      """
        |select
        |Sname,SID
        |from student
        |where
        |student.SID not in (select * from resSID)
      """.stripMargin).show(10)

  }


  /**
    * 15、查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
    */
  @Test
  def sqlPro15: Unit = {

    //分析需要sc表,student表
    val sc = getSc_table
    val student = getStudent_table
    //注册成表
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")


    //编写sql 1.获取不及格的分数,对应的学号
    val failscore: DataFrame = spark.sql(
      """
        |select
        |sc.SID
        |from sc
        |where
        |sc.score <60
        |group by sc.SID
        |having count(sc.SID)>=2
      """.stripMargin)
    failscore.createOrReplaceTempView("failSID")

    spark.sql(
      """
        |select
        |student.*
        |from
        |student
        |where
        |student.SID  in (select SID from failSID )
      """.stripMargin).show()

  }

  /**
    * -- 16、检索"01"课程分数小于60，按分数降序排列的学生信息
    */
  @Test
  def sqlPro16={

    //需求表 sc表 student表
    val sc = getSc_table
    val student = getStudent_table

    //注册成表
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")

    //编写sql
//    val failSID =spark.sql(
//      """
//        |select
//        |sc.SID , sc.score
//        |from sc
//        |where sc.CID=01
//        |and sc.score<60
//      """.stripMargin)
//
//    failSID.createOrReplaceTempView("failSID")

    spark.sql(
      """
        |select
        |student.*
        |from student
        |join sc
        |on sc.SID = student.SID
        |where
        |sc.CID = 01 and sc.score < 60
        |sort by sc.score DESC
      """.stripMargin).show()

  }


  /**
    * -- 17、按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    */
  @Test
  def sqlPro17={

    //需求 sc student
    val sc = getSc_table
    val student = getStudent_table

    //注册表
    sc.createOrReplaceTempView("sc")
    student.createOrReplaceTempView("student")

    //编写sql
//    spark.sql(
//      """
//        |select
//        |student.* ,sc.CID, sc.score,ssc.avgsc
//        |from student
//        |join sc on sc.SID = student.SID
//        |join (select avg(sc.score) avgsc,sc.SID  from sc group by sc.SID ) ssc
//        |on ssc.SID = student.SID
//        |group by student.SID,student.Sname,student.Sage,Ssex,sc.CID,sc.score ,ssc.avgsc
//        |sort by student.SID,ssc.avgsc desc
//      """.stripMargin).show()

    val a= spark.sql(
      """
        |select
        |student.SID,student.Sname,avg(ifnull(sc.score,0)) avgscore,ifnull(sc1.score,0),
        |ifnull(sc2.score,0),ifnull(sc3.score,0)
        |from student
        |left join sc
        |on sc.SID = student.SID
        |left join sc as sc1
        |on sc1.SID = student.SID and sc1.CID = 01
        |left join sc as sc2
        |on sc2.SID = student.SID and sc2.CID = 02
        |left join sc as sc3
        |on sc3.SID = student.SID and sc3.CID = 03
        |group by student.SID,student.Sname,sc1.score,sc2.score,sc3.score
        |order by avg(sc.score) DESC
      """.stripMargin)

    val execution: QueryExecution = a.queryExecution
    println(execution)
    a.explain()

  }

  @Test
  def sqlPro18 ={

    val sc = getSc_table
    sc.createOrReplaceTempView("sc")

    spark.sql(
      """
        |select sc.CID,max(score) from sc group by sc.CID
      """.stripMargin).explain()

  }
}

