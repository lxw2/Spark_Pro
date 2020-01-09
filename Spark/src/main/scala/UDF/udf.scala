package UDF

object udf {
  def fixID(id:String)={
      "0"*(8-id.size)+id
  }
}
