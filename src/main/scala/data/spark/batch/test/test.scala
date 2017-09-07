package data.spark.batch.test

/**
  * Created by ranzechen on 2017/9/6.
  */
object test {
  def main(args: Array[String]): Unit = {
    val a = for(i <- Array("1","2","3","4")) yield i.toInt * 3
    println(a.mkString(","))
  }
}
