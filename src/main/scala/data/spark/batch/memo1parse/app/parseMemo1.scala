package data.spark.batch.memo1parse.app

import data.spark.batch.memo1parse.util.ybs_sett_data
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by ranzechen on 2017/9/15.
  */
object parseMemo1 extends App {
  val sparkConf = new SparkConf().setAppName("parseMemo1")
  val sparkContext = new SparkContext(sparkConf)
  Source.fromFile(args(0)).getLines.foreach(line => {
    val Array(inputfile, outputpath) = line.split("\\s+")
    sparkContext.textFile(inputfile)
      .map(line => ybs_sett_data.parse(line))
      .filter(_.tfmemo1.length != 0)
      .map(data => {
        data.tfmccode match {
          //发卡行#卡号#姓名#证件#手机#CVN#卡类型
          case "020052" => Some({
            val arr = data.tfmemo1.split("\\^")
            if (arr.length >= 17) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(7).trim, "证件" -> arr(6).trim, "手机" -> arr(16).trim, "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case "020148" => Some({
            val arr = data.tfmemo.split("\\^")
            if (arr.length >= 12) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(3).trim, "证件" -> arr(5).trim, "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case "000505" => Some({
            val arr = data.tfmemo1.split("\\+")
            if (arr.length >= 7) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(3).split(":")(1).trim, "证件" -> arr(2).split(":")(1).trim, "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case "020058" => Some({
            val arr = data.tfmemo1.split("\\^")
            if (arr.length >= 4) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(3).trim, "证件" -> arr(4).trim, "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case "020060" => Some({
            val arr = data.tfmemo1.split("\\^")
            if (arr(1).contains(":")) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(1).split(":")(1).trim, "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case "020068" => Some({
            val arr = data.tfmemo1.split("\\^")
            if (arr.length >= 3) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(1).trim, "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })

          case "020137" => Some({
            if (data.tfmemo.contains(":")) {
              val arr = data.tfmemo.split(":")
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(1).trim, "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case "020255" => Some({
            val arr = data.tfmemo.split("\\^")
            if (arr.length >= 8) {
              Map("发卡行" -> "", "卡号" -> data.tfcardno, "姓名" -> arr(1).trim, "证件" -> arr(3).trim, "手机" -> "", "CVN" -> "", "卡类型" -> "")
            } else {
              Map("发卡行" -> "", "卡号" -> "", "姓名" -> "", "证件" -> "", "手机" -> "", "CVN" -> "", "卡类型" -> "")
            }
          })
          case _ => None
        }
      }).filter(_ != None)
      .filter(_.get.get("姓名").get.trim.length != 0)
      .filter(_.get.get("姓名").get.trim.length < 5)
      .filter(!_.get.get("姓名").get.trim.contains("公司"))
      .saveAsObjectFile(outputpath)
  })
}
