package data.spark.batch.memo1parse.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/9/16.
  * 清算数据中第十二个字段为发卡行代码不用再查卡bin表了
  * 结果形式：发卡行#卡号#姓名#证件#手机#CVN#卡类型
  */
object unionResMemo1 {
  def main(args: Array[String]): Unit = {
    val Array(filelist, configfile, unionres) = args
    val sparkConf = new SparkConf().setAppName("unionResMemo1")
    val sparkContext = new SparkContext(sparkConf)
    //读取配置文件并获取到所需要的关联表的路径
    val config = Source.fromFile(configfile).mkString
    val ppbankdspath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ppbankdspath").toString

    val files = Source.fromFile(filelist).getLines.toArray
      .map(line => {
        val Array(inputfile, outputfile) = line.split("\\s+")
        outputfile
      })
    val ppbankdsmap = sparkContext.textFile(ppbankdspath).map(_.split("衚"))
      .map(data => {
        val alt_bank = data(0)
        val bankname = data(1)
        (alt_bank, bankname)
      }).collect().toMap

    all_rdd(files.map(file => {
      old_rdd(file, sparkContext)
    })).map(data => {
      val cardno = data.get.getOrElse("卡号", "")
      val bankno = data.get.getOrElse("发卡行", "")
      val cardtype = data.get.getOrElse("卡类型", "") match {
        case "credit" => "信用卡"
        case "debit" => "借记卡"
        case _ => None
      }
      if (cardno.length != 0) {
        val bankname = ppbankdsmap.getOrElse(bankno, "")
        s"$bankname#$cardno#${data.get.getOrElse("姓名", "")}#${data.get.getOrElse("证件", "")}#${data.get.getOrElse("手机", "")}#${data.get.getOrElse("CVN", "")}#$cardtype"
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
