package data.spark.batch.dataCleaning.areaCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/9/20.
  * 结果形式：
  */
object unionResCountArea {
  def main(args: Array[String]): Unit = {
    val Array(configfile, filelist) = args

    val config = Source.fromFile(configfile).mkString
    val areapath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("area_path").toString
    val banknopath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("banknopath").toString
    val es_ip = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("cardbin_ES_IP").toString
    val es_port = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("cardbin_ES_PORT").toString
    val es_cluster_name = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("cardbin_ES_CLUSTER_NAME").toString
    val index_type = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("cardbin_index_type").toString

    val sparkConf = new SparkConf().setAppName("unionResCountArea")
    sparkConf.set("es.nodes", es_ip)//esip地址
    sparkConf.set("es.port", es_port)
    sparkConf.set("cluster.name", es_cluster_name)//es集群名称
    val sparkContext = new SparkContext(sparkConf)

    //读取每个文件
    val files = Source.fromFile(filelist).getLines.toArray
      .map(line => {
        val Array(inputfile, outputfile) = line.split("\\s+")
        outputfile
      })

    val area_map = sparkContext.textFile(areapath).map(_.split(","))
      .map(arr => (arr(1).trim, arr(0).trim)).collect().toMap
    val banknomap = sparkContext.textFile(banknopath).map(_.split(","))
      .map(arr => (arr(1).trim, arr(0).trim)).collect().toMap

    all_rdd(files.map(file => {
      old_rdd(file, sparkContext)
    })).reduceByKey(_ + _)
      .map(data => (data._1._1, (data._1._3, data._1._2, data._2)))
      .groupByKey()
      .map(data => {
        val cardbin = data._1
        val bankinfo = data._2.map(data => (data._1,banknomap.getOrElse(data._1, data._1))).toMap
        val areainfo = data._2.map(data => (data._2 , area_map.getOrElse(data._2, data._2))).toMap
        val info = data._2.map(data => (area_map.getOrElse(data._2, data._2),data._3)).toSeq.sortBy(_._2).toMap
        Map("cardbin" -> cardbin, "bank_desc" -> bankinfo,"area_desc" -> areainfo,"count" -> info)
      }).sortBy(_.get("cardbin").get.toString)
      .saveToEs(index_type,Map(
        "es.index.auto.create" -> "true",
        "es.mapping.id" -> "cardbin",
        "es.mapping.exclude" -> "cardbin"
      ))

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
