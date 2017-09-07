package data.spark.batch.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ranzechen on 2017/9/6.
  */
object SparkTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkTest")
    val sparkContext = new SparkContext(sparkConf)
  }
}
