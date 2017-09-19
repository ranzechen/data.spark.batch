package data.spark.batch.memo1parse.app

import data.spark.batch.memo1parse.util.raltutil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.io.Source
import scala.util.matching.Regex
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/9/19.
  * 卡 + 户
  */
object card_name{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("card_name")
    val sparkContext = new SparkContext(sparkConf)

    val config = Source.fromFile(args(0)).mkString
    val raltpath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("raltpath").toString
    val ralt_choose_itempath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ralt_choose_itempath").toString
    val ppbankdspath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ppbankdspath").toString

    val ralt = raltutil.getSearcher(raltpath, ralt_choose_itempath, ppbankdspath)
    sparkContext.textFile(args(1))
      .map(_.split("#"))
      .filter(_.length == 7)
      .filter(_(1).trim.length != 0)
      .map(arr => {
        val p_t = new Regex("([\\u4e00-\\u9fa5]+)")
        val cardno = arr(1)
        val license = arr(3)
        val phone = arr(4)
        val name = p_t.findAllIn(arr(2).trim).mkString
        val raltinfo = ralt.search(cardno)
        if(raltinfo != None){
          val altbank = raltinfo.get.the_card_segment
          val bankcode = raltinfo.get.bank_code
          val cardtype = raltinfo.get.card_type
          val cardlen = raltinfo.get.card_len
          s"[${String.format("%1$-12s", altbank)}|${String.format("%1$-8s", bankcode)}|${String.format("%1$-10s", cardtype)}|${String.format("%1$-12s", cardlen)}]:${String.format("%1$-20s", cardno)}#${name + (" " * (20 - name.length * 2))}#${String.format("%1$-15s", if(phone.length == 11) phone else "")}#${String.format("%1$-20s", license)}#${String.format("%1$-8s", bankcode)}#${String.format("%1$-10s", cardtype)}"
        }else{
          ""
        }
      })
      .filter(_.length != 0)
      .filter(_.split("#")(1).trim.length >= 2)
      .groupBy(_.split("\\|")(0).replaceAll("\\[", "").trim)
      .map(data => {
        val alt_bank = data._1
        val info = data._2.toArray.sortWith(_ > _).take(5)
        (alt_bank, info)
      })
      .sortBy(_._1)
      .map(data => {
        data._2.mkString("\n")
      })
      .coalesce(10, true)
      .saveAsTextFile(args(2))
  }
}
