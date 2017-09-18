package data.spark.batch.memo1parse.app

import data.spark.batch.memo1parse.util.raltutil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/9/16.
  * 清算数据中第十二个字段为发卡行代码不用再查卡bin表了
  */
object unionResMemo1 {
  def main(args: Array[String]): Unit = {
    val Array(filelist, configfile, unionres) = args
    val sparkConf = new SparkConf().setAppName("unionResMemo1")
    val sparkContext = new SparkContext(sparkConf)
    //读取配置文件并获取到所需要的关联表的路径
    val config = Source.fromFile(configfile).mkString
    val raltpath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("raltpath").toString
    val ppbankdspath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ppbankdspath").toString
    val ralt_choose_itempath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ralt_choose_itempath").toString

    val files = Source.fromFile(filelist).getLines.toArray
      .map(line => {
        val Array(inputfile, outputfile) = line.split("\\s+")
        outputfile
      })
    val ralt = raltutil.getSearcher(raltpath, ppbankdspath, ralt_choose_itempath)
    all_rdd(files.map(file => {
      old_rdd(file, sparkContext)
    })).map(data => {
        val cardno = data.get.getOrElse("卡号", "")
        if (cardno.length != 0) {
          val raltinfo = ralt.search(cardno)
          if (raltinfo != None) {
            s"${raltinfo.get.bank_name}#$cardno#${data.get.getOrElse("姓名", "")}#${data.get.getOrElse("证件", "")}#${data.get.getOrElse("手机", "")}#${data.get.getOrElse("CVN", "")}#${raltinfo.get.card_type}"
          } else {
            s"#$cardno#${data.get.getOrElse("姓名", "")}#${data.get.getOrElse("证件", "")}#${data.get.getOrElse("手机", "")}#${data.get.getOrElse("CVN", "")}#"
          }
        } else {
          s"##${data.get.getOrElse("姓名", "")}#${data.get.getOrElse("证件", "")}#${data.get.getOrElse("手机", "")}#${data.get.getOrElse("CVN", "")}#"
        }
      }).sortBy(_.split("#")(2))
        .saveAsTextFile(unionres)
  }

  def old_rdd(file: String, sc: SparkContext): RDD[Option[Map[String, String]]] = {
    sc.objectFile[Option[Map[String, String]]](file)
  }

  def all_rdd(rdds: Array[RDD[Option[Map[String, String]]]]): RDD[Option[Map[String, String]]] = {
    rdds.reduce(_ ++ _)
      .distinct()
  }
}
