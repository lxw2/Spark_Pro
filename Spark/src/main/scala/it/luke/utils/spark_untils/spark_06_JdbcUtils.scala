package it.luke.utils.spark_untils

import java.sql.{Connection, DriverManager, PreparedStatement}


object spark_06_JdbcUtils {
  /**
    * 根据geoCode查询商圈列表
    */
  def getAreas(geoCode:String)={

    //1、加载驱动
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    //2、创建Connection
    var connection:Connection = null
    var statement:PreparedStatement = null

    var areas = ""
    try{

      connection = DriverManager.getConnection("jdbc:impala://hadoop03:21050/default")
      //3、创建Statement
      statement = connection.prepareStatement("select areas from business_area where geoCode=?")
      //4、参数赋值
      statement.setString(1,geoCode)

      //5、执行查询获取结果
      val resultSet = statement.executeQuery()

      while (resultSet.next()){
        areas = resultSet.getString("areas")
      }
    }catch {
      case e:Exception=>""
    }finally {
      if(statement!=null)
        statement.close()
      if(connection!=null)
        connection.close()
    }

    areas


  }
}
