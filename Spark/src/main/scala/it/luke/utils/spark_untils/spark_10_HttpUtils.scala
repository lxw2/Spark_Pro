package it.luke.utils.spark_untils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

object spark_10_HttpUtils {

  /**
    * 发起get请求
    * @param url
    * @return
    */
  def get(url:String):String ={

    //1、创建HttpClient
    val client = new HttpClient()
    //2、指定请求方式
    val get = new GetMethod(url)
    //3、执行请求
    val code = client.executeMethod(get)
    //4、判断状态码，200请求正常
    if(code==200){
    //5、数据返回
      get.getResponseBodyAsString
    }else{
      ""
    }
  }
}
