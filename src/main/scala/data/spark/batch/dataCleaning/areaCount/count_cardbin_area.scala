package data.spark.batch.dataCleaning.areaCount

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by ranzechen on 2017/9/20.
  * 统计acomn或者acom报文文件中的每个12位卡bin(全卡号前12位)对应的地区和次数
  */
object count_cardbin_area {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("count_cardbin_area")
    val sparkContext = new SparkContext(sparkConf)
    val Array(filelist) = args
    Source.fromFile(filelist).getLines.foreach(line => {
      val Array(inputfile, outputpath) = line.split("\\s+")
      sparkContext.textFile(inputfile)
        .filter(_.substring(42, 61).trim.length >= 12)
        .map(line => ((line.substring(42, 61).trim.substring(0, 12), line.substring(252, 263).trim.substring(4, 8), line.substring(252, 263).trim.substring(0, 4)), 1l))
        .reduceByKey(_ + _)
        .saveAsObjectFile(outputpath)
    })
  }
}
