package data.spark.batch.messageparse.app

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by ranzechen on 2017/8/17.
  * 解析一般流水报文文件
  * usage:{args(0)=hdfspath args(1)=estype args(2)=机构号 args(3)=输入文件名称 args(4)=品牌费文件路径 args(5)=配置文件路径}
  */
object parseMessageAcomApp {

  def main(args: Array[String]): Unit = {
    val Array(inputpath, esType, jigouhao, input_file_name, alfeepath, configfile) = args
    //读取配置文件并获取到所需要的关联表的路径
    val config = Source.fromFile(configfile).mkString
    val trancodepath = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("trancodepath").toString
    val es_ip = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("parseMessage_ES_IP").toString
    val es_port = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("parseMessage_ES_PORT").toString
    val es_cluster_name = JSON.parseFull(config).asInstanceOf[Option[Map[String, Any]]].get("parseMessage_ES_CLUSTER_NAME").toString

    val sparkConf = new SparkConf().setAppName("parseMessageAcomApp")
    sparkConf.set("es.nodes", es_ip) //100.1.1.34,100.1.1.40,100.1.1.42
    sparkConf.set("es.port", es_port)
    sparkConf.set("cluster.name", es_cluster_name)
    //es-spark
    val sparkContext = new SparkContext(sparkConf)
    //读tran_code表
    val trancodeMap = sparkContext.textFile(trancodepath)
      .map(_.split("衚"))
      .map(arr => {
        val key = s"${arr(2)}_${arr(8)}_${arr(5)}"
        val valueArr = Array(arr(0), arr(1), arr(7))
        (key, valueArr)
      }).collect().toMap
    val pattern = "[0-9]".r
    //添加品牌费字段并根据id转为map获取
    val alfeeMap = sparkContext.textFile(alfeepath)
      .map(line => {
        val key = s"${line.substring(41, 52).trim}_${line.substring(116, 122).trim}_${line.substring(123, 133).trim}_${line.substring(134, 153).trim}_${line.substring(293, 305).trim}_${line.substring(264, 279).trim}"
        val value = pattern.findAllIn(line.substring(319, 331).trim).mkString.toDouble / 100
        (key, value)
      }).collect().toMap

    sparkContext.textFile(inputpath)
      .map(line => {
        val id = s"${line.substring(0, 11).trim}_${line.substring(24, 30).trim}_${line.substring(31, 41).trim}_${line.substring(42, 61).trim}_${line.substring(62, 74).trim}_${line.substring(127, 142).trim}"
        val trancodekey = s"${line.substring(101, 105).trim}_${line.substring(106, 112).trim.substring(0, 2)}_${line.substring(156, 158).trim}"
        val remark = if(trancodeMap.getOrElse(trancodekey, Array()).length !=0 ) trancodeMap.get(trancodekey).get(2).toDouble else 0
        if (line.length == 299) {
          Map(
            "acq_inst_id_code" -> line.substring(0, 11).trim,
            "fwd_inst_id_code" -> line.substring(12, 23).trim,
            "sys_trace_audit_num" -> line.substring(24, 30).trim,
            "transmsn_date_time" -> line.substring(31, 41).trim,
            "primary_acct_num" -> line.substring(42, 61).trim,
            "amt_trans" -> line.substring(62, 74).trim.toDouble / 100 * remark,
            "f95" -> line.substring(75, 87).trim.toDouble / 100,
            "amt_trans_fee" -> line.substring(88, 100).trim.toDouble / 100,
            "message_type" -> line.substring(101, 105).trim,
            "processing_code" -> line.substring(106, 112).trim,
            "mchnt_type" -> line.substring(113, 117).trim,
            "card_accptr_termnl_id" -> line.substring(118, 126).trim,
            "card_accptr_id" -> line.substring(127, 142).trim,
            "retrivl_ref_num" -> line.substring(143, 155).trim,
            "pos_cond_code" -> line.substring(156, 158).trim,
            "authr_id_resp" -> line.substring(159, 165).trim,
            "rcvg_inst_id_code" -> line.substring(166, 177).trim,
            "f90_2" -> line.substring(178, 184).trim,
            "resp_code" -> line.substring(185, 187).trim,
            "pos_entry_mode_code" -> line.substring(188, 191).trim,
            "recycle_fee" -> pattern.findAllIn(line.substring(192, 204).trim).mkString.toDouble / 100,
            "pay_fee" -> pattern.findAllIn(line.substring(205, 217).trim).mkString.toDouble / 100,
            "transfer_sett_fee" -> pattern.findAllIn(line.substring(218, 230).trim).mkString.toDouble / 100,
            "single_double_trans_marks" -> line.substring(231, 232).trim,
            "card_seq_id" -> line.substring(233, 236).trim,
            "f60_2_2" -> line.substring(237, 238).trim,
            "f60_2_3" -> line.substring(239, 240).trim,
            "f90_3" -> line.substring(241, 251).trim,
            "card_institution_code" -> line.substring(252, 263).trim,
            "trans_area_code" -> line.substring(264, 265).trim,
            "f60_2_5" -> line.substring(266, 268).trim,
            "f60_2_8" -> line.substring(269, 271).trim,
            "installment_pay_fee" -> pattern.findAllIn(line.substring(272, 284).trim).mkString.toDouble / 100,
            "authorization_flag" -> line.substring(285, 286).trim,
            "special_billing_type" -> line.substring(286, 288).trim,
            "special_billing_level" -> line.substring(288, 289).trim,
            "trans_start_way" -> line.substring(289, 290).trim,
            "account_sett_type" -> line.substring(290, 292).trim,
            "not_price_mername" -> line.substring(292, 293).trim,
            "card_account_level" -> line.substring(293, 294).trim,
            "card_products" -> line.substring(294, 296).trim,
            "standard_card" -> line.substring(296, 297).trim,
            "input_cardno" -> "",
            "installment_pay_times" -> "",
            "order_num" -> "",
            "pay_way" -> "",
            "account_level" -> "",
            "counter_check" -> "",
            "data_source" -> s"${jigouhao}_${input_file_name}",
            "alfee" -> alfeeMap.getOrElse(id, 0),
            "type_name" -> (if(trancodeMap.getOrElse(trancodekey, Array()).length !=0 ) trancodeMap.get(trancodekey).get(0) else ""),
            "tran_code" -> (if(trancodeMap.getOrElse(trancodekey, Array()).length !=0 ) trancodeMap.get(trancodekey).get(1) else ""),
            "id" -> id
          )
        } else if (line.length == 500) {
          Map(
            "acq_inst_id_code" -> line.substring(0, 11).trim,
            "fwd_inst_id_code" -> line.substring(12, 23).trim,
            "sys_trace_audit_num" -> line.substring(24, 30).trim,
            "transmsn_date_time" -> line.substring(31, 41).trim,
            "primary_acct_num" -> line.substring(42, 61).trim,
            "amt_trans" -> line.substring(62, 74).trim.toDouble / 100 * remark,
            "f95" -> line.substring(75, 87).trim.toDouble / 100,
            "amt_trans_fee" -> line.substring(88, 100).trim.toDouble / 100,
            "message_type" -> line.substring(101, 105).trim,
            "processing_code" -> line.substring(106, 112).trim,
            "mchnt_type" -> line.substring(113, 117).trim,
            "card_accptr_termnl_id" -> line.substring(118, 126).trim,
            "card_accptr_id" -> line.substring(127, 142).trim,
            "retrivl_ref_num" -> line.substring(143, 155).trim,
            "pos_cond_code" -> line.substring(156, 158).trim,
            "authr_id_resp" -> line.substring(159, 165).trim,
            "rcvg_inst_id_code" -> line.substring(166, 177).trim,
            "f90_2" -> line.substring(178, 184).trim,
            "resp_code" -> line.substring(185, 187).trim,
            "pos_entry_mode_code" -> line.substring(188, 191).trim,
            "recycle_fee" -> pattern.findAllIn(line.substring(192, 204).trim).mkString.toDouble / 100,
            "pay_fee" -> pattern.findAllIn(line.substring(205, 217).trim).mkString.toDouble / 100,
            "transfer_sett_fee" -> pattern.findAllIn(line.substring(218, 230).trim).mkString.toDouble / 100,
            "single_double_trans_marks" -> line.substring(231, 232).trim,
            "card_seq_id" -> line.substring(233, 236).trim,
            "f60_2_2" -> line.substring(237, 238).trim,
            "f60_2_3" -> line.substring(239, 240).trim,
            "f90_3" -> line.substring(241, 251).trim,
            "card_institution_code" -> line.substring(252, 263).trim,
            "trans_area_code" -> line.substring(264, 265).trim,
            "f60_2_5" -> line.substring(266, 268).trim,
            "f60_2_8" -> line.substring(269, 271).trim,
            "installment_pay_fee" -> pattern.findAllIn(line.substring(272, 284).trim).mkString.toDouble / 100,
            "authorization_flag" -> line.substring(285, 286).trim,
            "special_billing_type" -> line.substring(286, 288).trim,
            "special_billing_level" -> line.substring(288, 289).trim,
            "trans_start_way" -> line.substring(289, 290).trim,
            "account_sett_type" -> line.substring(290, 292).trim,
            "not_price_mername" -> line.substring(292, 293).trim,
            "card_account_level" -> line.substring(293, 294).trim,
            "card_products" -> line.substring(294, 296).trim,
            "standard_card" -> line.substring(296, 297).trim,
            "input_cardno" -> line.substring(300, 319).trim,
            "installment_pay_times" -> line.substring(320, 322).trim,
            "order_num" -> line.substring(323, 363).trim,
            "pay_way" -> line.substring(364, 368).trim,
            "account_level" -> line.substring(455, 456).trim,
            "counter_check" -> line.substring(457, 458).trim,
            "data_source" -> s"${args(2)}_${args(3)}",
            "alfee" -> alfeeMap.getOrElse(id, 0),
            "type_name" -> (if(trancodeMap.getOrElse(trancodekey, Array()).length !=0 ) trancodeMap.get(trancodekey).get(0) else ""),
            "tran_code" -> (if(trancodeMap.getOrElse(trancodekey, Array()).length !=0 ) trancodeMap.get(trancodekey).get(1) else ""),
            "id" -> id
          )
        } else {
          (">>>>>Exception", (line.length, line))
        }
      }).saveToEs(s"acom_${esType.substring(0,6)}/${esType}",Map(
      "es.index.auto.create" -> "true",
      "es.mapping.id" -> "id",
      "es.mapping.exclude" -> "id"
    ))
  }
}
