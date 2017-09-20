package data.spark.batch.dataCleaning.memo1parse.util

/**
  * Created by ranzechen on 2017/9/16.
  * 1、映射RALT,RALT_CHOOSE_ITEM,PPBANKDS表等
  * 2、通过卡号获取卡bin信息等
  */

case class raltfield(
                      bank_code: String,
                      card_type: String,
                      bank_name: String,
                      the_card_segment: String,
                      issuing_institution: String,
                      issuing_object: String,
                      card_level: String,
                      partner: String,
                      currency: String,
                      medium: String,
                      functions: String,
                      special_func: String,
                      south_card: String,
                      card_len:String
                    )

class raltSearch(
                  ralt: Array[(Map[String, Int], Map[String, Array[String]])],
                  ppbankds: Map[String, Array[String]],
                  ralt_choose_item: Map[String, String]
                ) extends Serializable {
  def search(cardno: String): Option[raltfield] = {

    for (c <- ralt) {
      val begin = c._1("begin")
      val len = c._1("len")

      c._2.get(cardno.substring(begin - 1, begin + len - 1) + ":" + cardno.toString.length) match {
        case Some(cardinfo) =>
          def getfuncStr(no: Int): String = {
            val str = if (cardinfo(no) == null) "" else cardinfo(no).toString
            str.split(",").map(d => ralt_choose_item.getOrElse(d, "").trim).mkString("[", ",", "]")
          }

          return Some(raltfield(
            bank_code = cardinfo(1).toString,
            card_type = cardinfo(2).toString,//if (cardinfo(2).toString == "credit") "信用卡" else if ( == "debit") "借记卡" else cardinfo(2).toString,
            bank_name = if(ppbankds.getOrElse(cardinfo(1).toString,Array()).length != 0) ppbankds(cardinfo(1).toString)(1).toString else "",
            the_card_segment = cardinfo(0).toString,
            issuing_institution = getfuncStr(8),
            issuing_object = getfuncStr(9),
            card_level = getfuncStr(10),
            partner = getfuncStr(11),
            currency = getfuncStr(12),
            medium = getfuncStr(13),
            functions = getfuncStr(14),
            special_func = getfuncStr(15),
            south_card = if (cardinfo(16).toString == "1") "是" else "否",
            card_len = cardinfo(5)
          ))
        case None =>
      }
    }
    None
  }
}


object raltutil {

  def getSearcher(raltPath: String, ppbankdspath: String, ralt_choose_itempath: String): raltSearch = {
    val sc = org.apache.spark.SparkContext.getOrCreate()
    val ralt = sc.textFile(raltPath).map(_.split("衚"))
      .groupBy(d => Map("len" -> d(0).length, "begin" -> d(6).toInt))
      .collect()
      .sortWith(_._2.toArray.length > _._2.toArray.length)
      .map(begin => (begin._1, begin._2.map(d => (d(0) + ":" + d(5), d)).toMap))

    val ppbankds = sc.textFile(ppbankdspath).map(_.split("衚")).collect.map(d => (d(0), d)).toMap

    val ralt_choose_item = sc.textFile(ralt_choose_itempath).map(_.split("衚")).collect.map(d => (d(0), d(2))).toMap

    new raltSearch(ralt, ppbankds, ralt_choose_item)
  }
}
