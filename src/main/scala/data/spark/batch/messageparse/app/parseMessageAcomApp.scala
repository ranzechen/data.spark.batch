package data.spark.batch.messageparse.app

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
/**
  * Created by ranzechen on 2017/8/17.
  * 解析一般流水报文文件
  */
object parseMessageAcomApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("parseMessageAcomApp")
    sparkConf.set("es.nodes", "100.1.1.42,100.1.1.40,100.1.1.34")//
    sparkConf.set("es.port", "9200")
    sparkConf.set("cluster.name", "es-spark")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.textFile(args(0))
      .map(line => {
        val pattern = "[0-9]".r
        if (line.length == 299) {
          Map(
            "acq_inst_id_code" -> line.substring(0, 11).trim,
            "fwd_inst_id_code" -> line.substring(12, 23).trim,
            "sys_trace_audit_num" -> line.substring(24, 30).trim,
            "transmsn_date_time" -> line.substring(31, 41).trim,
            "primary_acct_num" -> line.substring(42, 61).trim,
            "amt_trans" -> line.substring(62, 74).trim.toDouble,
            "f95" -> line.substring(75, 87).trim.toDouble,
            "amt_trans_fee" -> line.substring(88, 100).trim.toDouble,
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
            "recycle_fee" -> pattern.findAllIn(line.substring(192, 204).trim).mkString.toDouble,
            "pay_fee" -> pattern.findAllIn(line.substring(205, 217).trim).mkString.toDouble,
            "transfer_sett_fee" -> pattern.findAllIn(line.substring(218, 230).trim).mkString.toDouble,
            "single_double_trans_marks" -> line.substring(231, 232).trim,
            "card_seq_id" -> line.substring(233, 236).trim,
            "f60_2_2" -> line.substring(237, 238).trim,
            "f60_2_3" -> line.substring(239, 240).trim,
            "f90_3" -> line.substring(241, 251).trim,
            "card_institution_code" -> line.substring(252, 263).trim,
            "trans_area_code" -> line.substring(264, 265).trim,
            "f60_2_5" -> line.substring(266, 268).trim,
            "f60_2_8" -> line.substring(269, 271).trim,
            "installment_pay_fee" -> pattern.findAllIn(line.substring(272, 284).trim).mkString.toDouble,
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
            "id" -> (line.substring(0, 11).trim+"_"+line.substring(24, 30).trim+"_"+line.substring(31, 41).trim+"_"+line.substring(42, 61).trim+"_"+line.substring(62, 74).trim+"_"+line.substring(127, 142).trim)
          )
        } else if (line.length == 500) {
          Map(
            "acq_inst_id_code" -> line.substring(0, 11).trim,
            "fwd_inst_id_code" -> line.substring(12, 23).trim,
            "sys_trace_audit_num" -> line.substring(24, 30).trim,
            "transmsn_date_time" -> line.substring(31, 41).trim,
            "primary_acct_num" -> line.substring(42, 61).trim,
            "amt_trans" -> line.substring(62, 74).trim.toDouble,
            "f95" -> line.substring(75, 87).trim.toDouble,
            "amt_trans_fee" -> line.substring(88, 100).trim.toDouble,
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
            "recycle_fee" -> pattern.findAllIn(line.substring(192, 204).trim).mkString.toDouble,
            "pay_fee" -> pattern.findAllIn(line.substring(205, 217).trim).mkString.toDouble,
            "transfer_sett_fee" -> pattern.findAllIn(line.substring(218, 230).trim).mkString.toDouble,
            "single_double_trans_marks" -> line.substring(231, 232).trim,
            "card_seq_id" -> line.substring(233, 236).trim,
            "f60_2_2" -> line.substring(237, 238).trim,
            "f60_2_3" -> line.substring(239, 240).trim,
            "f90_3" -> line.substring(241, 251).trim,
            "card_institution_code" -> line.substring(252, 263).trim,
            "trans_area_code" -> line.substring(264, 265).trim,
            "f60_2_5" -> line.substring(266, 268).trim,
            "f60_2_8" -> line.substring(269, 271).trim,
            "installment_pay_fee" -> pattern.findAllIn(line.substring(272, 284).trim).mkString.toDouble,
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
            "id" -> (line.substring(0, 11).trim+"_"+line.substring(24, 30).trim+"_"+line.substring(31, 41).trim+"_"+line.substring(42, 61).trim+"_"+line.substring(62, 74).trim+"_"+line.substring(127, 142).trim)
          )
        }else{
          (">>>>>Exception", (line.length, line))
        }
      }).saveToEs({"acom_"+args(1).substring(0,6)}+"/"+{args(1)},Map(
      "es.index.auto.create" -> "true",
      "es.mapping.id" -> "id",
      "es.mapping.exclude" -> "id"
    ))
  }
}
