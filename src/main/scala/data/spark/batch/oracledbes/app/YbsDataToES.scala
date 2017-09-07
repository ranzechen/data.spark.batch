package data.spark.batch.oracledbes.app

import java.text.SimpleDateFormat

import data.spark.batch.oracledbes.util.{YbsDataUtils, RedisService}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by ranzechen on 2017/7/14.
  */
object YbsDataToES {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("YbsDataToES")
    sparkConf.set("es.nodes", "168.33.222.67")//esip地址
    sparkConf.set("es.port", "9200")
    sparkConf.set("cluster.name", "elasticsearch")//es集群名称
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.textFile(args(0))
      .map(lines => {
      val splitFlag = "衚"//字段分隔符
      val strArr = lines.split(splitFlag)
      val yymmdd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val yymmddhhmmss: SimpleDateFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      Map(
        "tftxcode" -> strArr(0),
        "tftermid" -> strArr(1),
        "tfteacct" -> strArr(2),
        "tfinvno" -> strArr(3),
        "tfbnussn" -> strArr(4),
        "tfbnossn" -> strArr(5),
        "tfodcode" -> strArr(6),
        "tfcustno" -> strArr(7),
        "tfintype" -> strArr(8),
        "tfmccode" -> strArr(9),
        "tftmccode" -> strArr(10),
        "tfbncode" -> strArr(11),
        "tftxmony" -> (if (YbsDataUtils.isNotEmpty(strArr(12))) strArr(12).toDouble else 0),
        "tfcharge" -> (if (YbsDataUtils.isNotEmpty(strArr(13))) strArr(13).toDouble else 0),
        "tfrlmony" -> (if (YbsDataUtils.isNotEmpty(strArr(14))) strArr(14).toDouble else 0),
        "tftxfnco" -> strArr(15),
        "encrypt_payer_card_no" -> YbsDataUtils.encrypt("1234567848615840", strArr(16)),
        "tfcardno" -> (
          if(strArr(16).length < 2){
            println("tftxcode"+strArr(0))
            strArr(16)
          } else if (strArr(16).length > 2 && strArr(16).length < 10) {
            strArr(16).substring(0, 2) + "*****" + strArr(16).substring(strArr(16).length() - 2, strArr(16).length())
          } else {
            strArr(16).substring(0, 6) + "*****" + strArr(16).substring(strArr(16).length() - 4, strArr(16).length())
          }
          ),
        "tfmainat" -> (
          if(strArr(17).length < 2){
            strArr(17)
          }else if (strArr(17).length > 2 && strArr(17).length < 10) {
            strArr(17).substring(0, 2) + "*****" + strArr(17).substring(strArr(17).length() - 2, strArr(17).length())
          } else {
            strArr(17).substring(0, 6) + "*****" + strArr(17).substring(strArr(17).length() - 4, strArr(17).length())
          }),
        "tfreqtno" -> strArr(18),
        "tfbackno" -> strArr(19),
        "tfsdtype" -> strArr(20),
        "tfacctdt" -> (if (YbsDataUtils.isNotEmpty(strArr(21))) yymmdd.parse(strArr(21).trim()).getTime else 0l),
        "tfauthco" -> strArr(22),
        "tfcovrat" -> (if (YbsDataUtils.isNotEmpty(strArr(23))) strArr(23).toDouble else 0),
        "tfcurren" -> strArr(24),
        "tfacmony" -> (if (YbsDataUtils.isNotEmpty(strArr(25))) strArr(25).toDouble else 0),
        "tftermno" -> strArr(26),
        "tflocstu" -> strArr(27),
        "tfbankstu" -> strArr(28),
        "tfmerstu" -> strArr(29),
        "tfbfcode" -> strArr(30),
        "tfdate" -> (if (YbsDataUtils.isNotEmpty(strArr(31))) yymmddhhmmss.parse(strArr(31).trim).getTime else 0l),
        "tftxdeta" -> strArr(32),
        "tftranno" -> (
          if(strArr(33).length< 2){
            strArr(33)
          } else if (strArr(33).length > 2 && strArr(33).length < 10) {
            strArr(33).substring(0, 2) + "*****" + strArr(33).substring(strArr(33).length() - 2, strArr(33).length())
          } else {
            strArr(33).substring(0, 6) + "*****" + strArr(33).substring(strArr(33).length() - 4, strArr(33).length())
          }),
        "tfacqinsid" -> strArr(34),
        "tffwdinsid" -> strArr(35),
        "tfrcvinsid" -> strArr(36),
        "sett_flag" -> strArr(37),
        "mer_name" -> strArr(38),
        "mer_type" -> strArr(39),
        "mer_comm" -> strArr(40),
        "tran_code" -> strArr(41),
        "hs_comm" -> (if (YbsDataUtils.isNotEmpty(strArr(42))) strArr(42).toDouble else 0),
        "hs_comm1" -> (if (YbsDataUtils.isNotEmpty(strArr(43))) strArr(43).toDouble else 0),
        "remark" -> strArr(44),
        "card_type" -> strArr(45),
        "cr_qs_no" -> strArr(46),
        "cr_in_date" -> strArr(47),
        "cr_date" -> strArr(48),
        "cr_reason" -> strArr(49),
        "should_arrive_date" -> strArr(50),
        "should_arrive_mony" -> (if (YbsDataUtils.isNotEmpty(strArr(51))) strArr(51).toDouble else 0),
        "sett_type" -> strArr(52),
        "huabo_date" -> strArr(53),
        "huabo_mony" -> (if (YbsDataUtils.isNotEmpty(strArr(54))) strArr(54).toDouble else 0),
        "moveout_date_n" -> strArr(55),
        "tftxfee" -> (if (YbsDataUtils.isNotEmpty(strArr(56))) strArr(56).toDouble else 0),
        "tfmemo" -> YbsDataUtils.encrypt("1234567848615840", strArr(57)).replaceAll("\r\n", "").replaceAll("\r", "").replaceAll("\n", ""),
        "tfmemo1" -> YbsDataUtils.encrypt("1234567848615840", strArr(58)).replaceAll("\r\n", "").replaceAll("\r", "").replaceAll("\n", ""),
        "tfrate" -> (if (YbsDataUtils.isNotEmpty(strArr(59))) strArr(59).toDouble else 0),
        "tfperdata" -> strArr(60),
        "tfdebitfee" -> (if (YbsDataUtils.isNotEmpty(strArr(61))) strArr(61).toDouble else 0),
        "tfmccode2" -> strArr(62),
        "tfextmccode2" -> strArr(63),
        "sett_level" -> strArr(64),
        "sett_bank" -> strArr(65),
        "fee_type" -> strArr(66),
        "sett_date" -> strArr(67),
        "money_come" -> strArr(68),
        "allocate_flag" -> strArr(69),
        "bank_getfee_remark" -> (if (YbsDataUtils.isNotEmpty(strArr(70))) strArr(70) else "0"),
        "bank_getfee" -> (if (YbsDataUtils.isNotEmpty(strArr(71))) strArr(71).toDouble else 0),
        "yl_getfee_remark" -> (if (YbsDataUtils.isNotEmpty(strArr(72))) strArr(72) else "0"),
        "yl_getfee" -> (if (YbsDataUtils.isNotEmpty(strArr(73))) strArr(73).toDouble else 0),
        "ybs_getfee" -> (if (YbsDataUtils.isNotEmpty(strArr(74))) strArr(74).toDouble else 0),
        "installment" -> strArr(75),
        "tfmemo_2bank" -> YbsDataUtils.encrypt("1234567848615840", strArr(76)).replaceAll("\r\n", "").replaceAll("\r", "").replaceAll("\n", ""),
        "tfmercdt" -> strArr(77),
        "ylagentid" -> strArr(78),
        "ylsendid" -> strArr(79),
        "ylrcvsid" -> strArr(80),
        "txchannel" -> strArr(81),
        "tfic_tc" -> strArr(82),
        "tfic_info" -> strArr(83),
        "tfcdtype" -> strArr(84),
        "pinblock_flag" -> strArr(85),
        "fwdtracenum" -> strArr(86),
        "fwdrefernum" -> strArr(87),
        "transpoints" -> strArr(88),
        "txfwdcode" -> strArr(89),
        "mertxcode" -> strArr(90),
        "ptr_from " -> strArr(91),
        "tfsettstu" -> strArr(92),
        "tfdistcode" -> strArr(93),
        "f_60_3_1" -> strArr(94),
        "f_60_3_2" -> strArr(95),
        "f_60_3_5" -> strArr(96),
        "f_60_3_8" -> strArr(97),
        "mer_price" -> strArr(98),
        "f_60_3_9" -> strArr(99),
        "f_60_3_10" -> strArr(100),
        "card_type_flag" -> strArr(101),
        "f22" -> strArr(102),
        "tfiac" -> (if (YbsDataUtils.isNotEmpty(strArr(103))) strArr(103).toDouble else 0),
        "alfee" -> strArr(104),
        "requetssn" -> strArr(105),
        "yunshanfu_flag" -> strArr(106),
        "yunshanfu_method" -> strArr(107),
        "data_from" -> strArr(108),
        "comefrom" -> "sett",
        "ybs_term_id" -> (if (YbsDataUtils.isNotEmpty(strArr(1))) strArr(1).substring(0, 8) else ""),
        "esIndex" -> ("ybs_sett_" + strArr(21).replaceAll("-", "").substring(0, 6)),
        "esType" -> strArr(21).replaceAll("-", "").substring(0, 8),
        "esId" -> (strArr(0) + "_" + strArr(9)),
        "term_district_name" -> (if (YbsDataUtils.isNotEmpty(strArr(93))) RedisService.getAreaInfo(strArr(93)) else ""),
        "payer_card_bankname" -> (if (YbsDataUtils.isNotEmpty(strArr(11))) RedisService.getBankInfo(strArr(11)) else ""),
        "bill_name" -> (if (YbsDataUtils.isNotEmpty(strArr(9))) RedisService.getMerInfo(strArr(9)) else ""),
        "sett_ins_name" -> (if (YbsDataUtils.isNotEmpty(strArr(35))) RedisService.getSettInfo(strArr(35)) else ""),
        "department" -> (if (YbsDataUtils.isNotEmpty(strArr(9))) RedisService.getDepartInfo(strArr(9)) else "")
      )
    }).saveToEs("{esIndex}/{esType}", Map(
        "es.index.auto.create" -> "true",
        "es.mapping.id" -> "esId",
        "es.mapping.exclude" -> "esId,esType,esIndex"
      ))
  }
}
