package data.spark.batch.dataCleaning.memo1parse

import data.spark.batch.dataCleaning.raltutil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.io.Source
import scala.util.matching.Regex
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/9/19.
  * 读取的数据格式为:  发卡行#卡号#姓名#证件#手机#cvn#卡类型
  * 对unionResMemo1的结果集得到卡 + 户 + 证
  */
object card_name_license {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("card_name_license")
    val sparkContext = new SparkContext(sparkConf)

    val config = Source.fromFile(args(0)).mkString
    val raltpath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("raltpath").toString
    val ralt_choose_itempath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ralt_choose_itempath").toString
    val ppbankdspath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("ppbankdspath").toString

    val ralt = raltutil.getSearcher(raltpath, ralt_choose_itempath, ppbankdspath)

    val filtercardno = sparkContext.textFile(args(3))
      .map(_.split(":")(1).split("#")(0).trim)
      .collect().toSet

    sparkContext.textFile(args(1))
      .map(_.split("#"))
      .filter(_.length == 7) //过滤出按照#分割数组长度为7的数据
      .filter(_ (3).length != 0) //过滤出证件号长度不等于0的数据
      .filter(_ (1).length != 0) //过滤出卡号长度不等于0的数据
      .filter(15 <= _ (3).length) //过滤出证件号长度大于等于15位的数据
      .map(data => (data(1), (data(2), data(3)))) //拼接(卡号,(姓名,证件)) 从而去掉同一个卡号下有多个姓名和证件号-------------->考虑了两种，没有考虑同名的
      .filter(data => {!(filtercardno.contains(data._1))})
      .groupByKey()
      .map(data => {
        val carno = data._1
        val info = data._2.toSeq.distinct
        (carno, info)
      })
      .filter(_._2.size == 1) //根据卡号分组去掉list不等于1的
      .map(data => ((data._2.map(_._2).mkString, (data._1, data._2.map(_._1).mkString)))) //拼接(证件号,(卡号,姓名)) 从而去掉同一个证件号下同一个姓名有多张不同的卡
      .groupByKey()
      .map(data => {
        val license = data._1
        val info = data._2.toSeq.distinct
        (license, info)
      }).filter(_._2.size == 1) //根据证件号分组去掉list不等于1的
      //转为tuple
      .map(data => {
      val cardno = data._2.map(_._1).mkString
      val name = data._2.map(_._2).mkString
      val license = data._1
      (cardno, name, license)
    })
      .map(arr => {
        val p_t = new Regex("([\\u4e00-\\u9fa5]+)")
        val cardno = arr._1
        val license = arr._3
        val name = p_t.findAllIn(arr._2.trim).mkString
        val raltinfo = ralt.search(cardno).get
        if (raltinfo != None) {
          val altbank = raltinfo.the_card_segment
          val bankcode = raltinfo.bank_code
          val cardtype = raltinfo.card_type
          val cardlen = raltinfo.card_len
          s"[${String.format("%1$-12s", altbank)}|${String.format("%1$-8s", bankcode)}|${String.format("%1$-10s", cardtype)}|${String.format("%1$-12s", cardlen)}]:${String.format("%1$-20s", cardno)}#${name + (" " * (20 - name.length * 2))}#${String.format("%1$-15s", "")}#${String.format("%1$-20s", license)}#${String.format("%1$-8s", bankcode)}#${String.format("%1$-10s", cardtype)}"
        } else {
          ""
        }
      })
      .filter(_.length != 0)
      .filter(_.split("#")(1).trim.length >= 2)
      .distinct()
      .groupBy(_.split("\\|")(0).replaceAll("\\[", "").trim)
      .map(data => {
        val alt_bank = data._1
        val info = data._2.toArray.sortWith(_ > _).take(5)
        (alt_bank, info)
      })
      .sortBy(_._1, false)
      .map(data => {
        data._2.mkString("\n")
      })
      .coalesce(1, true)
      .saveAsTextFile(args(2))
  }
}
