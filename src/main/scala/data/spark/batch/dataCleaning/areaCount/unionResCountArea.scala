package data.spark.batch.dataCleaning.areaCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/9/20.
  * 结果形式：
  */
object unionResCountArea {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("unionResCountArea")
    val sparkContext = new SparkContext(sparkConf)
    val Array(configfile, filelist) = args
    //读取每个文件
    val files = Source.fromFile(filelist).getLines.toArray
      .map(line => {
        val Array(inputfile, outputfile) = line.split("\\s+")
        outputfile
      })
    val config = Source.fromFile(configfile).mkString
    val areapath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("area_path").toString
    val banknopath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("banknopath").toString

    val area_map = sparkContext.textFile(areapath).map(_.split(","))
      .map(arr => (arr(1), arr(0))).collect().toMap
    val banknomap = sparkContext.textFile(banknopath).map(_.split(","))
      .map(arr => (arr(1), arr(0))).collect().toMap


    all_rdd(files.map(file => {
      old_rdd(file, sparkContext)
    }))
      .reduceByKey(_ + _)
      .map(data => (data._1._1, (data._1._3, data._1._2, data._2)))
      .groupByKey()
      .map(data => {
        val cardbin = data._1
        val info = data._2.map(data => (banknomap.getOrElse(data._1, data._1), (area_map.getOrElse(data._2, data._2), data._3))).groupBy(_._1)
          .map(data => (data._1,data._2.map(data => data._2)))
        (cardbin, info)
      }).sortByKey()
      .foreach(println)
  }

  //读取每个文件中的rdd
  def old_rdd(file: String, sc: SparkContext): RDD[((String, String, String), Long)] = {
    sc.objectFile[((String, String, String), Long)](file)
  }

  //合并每个文件中的rdd
  def all_rdd(rdds: Array[RDD[((String, String, String), Long)]]): RDD[((String, String, String), Long)] = {
    rdds.reduce(_ ++ _)
  }
}
