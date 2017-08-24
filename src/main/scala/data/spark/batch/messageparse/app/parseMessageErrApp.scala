package data.spark.batch.messageparse.app

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
/**
  * Created by ranzechen on 2017/8/23.
  * 解析差错流水报文文件并插入到es中
  */
object parseMessageErrApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("parseMessageErrApp")
    sparkConf.set("es.nodes", "100.1.1.42,100.1.1.40,100.1.1.34")//
    sparkConf.set("es.port", "9200")
    sparkConf.set("cluster.name", "es-spark")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.hadoopFile(args(0),classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
      .map(line => {
      val gap = 545 - line.length
      //计算汉字向前缩进的字节数
      val pattern = "[0-9]".r
      if (line.length == 354) {
        Map(
          "right_err_flag" -> line.substring(0, 3).trim,
          "acq_inst_id_code" -> line.substring(4, 15).trim,
          "fwd_inst_id_code" -> line.substring(16, 27).trim,
          "sys_trace_audit_num" -> line.substring(28, 34).trim,
          "transmsn_date_time" -> line.substring(35, 45).trim,
          "primary_acct_num" -> line.substring(46, 65).trim,
          "amt_trans" -> line.substring(66, 78).trim.toDouble,
          "message_type" -> line.substring(79, 83).trim,
          "processing_code" -> line.substring(84, 90).trim,
          "mchnt_type" -> line.substring(91, 95).trim,
          "card_accptr_termnl_id" -> line.substring(96, 104).trim,
          "retrivl_ref_num" -> line.substring(105, 117).trim,
          "pos_cond_code" -> line.substring(118, 120).trim,
          "authr_id_resp" -> line.substring(121, 127).trim,
          "rcvg_inst_id_code" -> line.substring(128, 139).trim,
          "bank_code" -> line.substring(140, 151).trim,
          "f90_2" -> line.substring(152, 158).trim,
          "resp_code" -> line.substring(159, 161).trim,
          "pos_entry_mode_code" -> line.substring(162, 165).trim,
          "recycle_fee" -> line.substring(166, 178).trim.toDouble,
          "pay_fee" -> line.substring(179, 191).trim.toDouble,
          "installment_pay_fee" -> pattern.findAllIn(line.substring(192, 204).trim).mkString.toDouble,
          "amt_trans_fee" -> pattern.findAllIn(line.substring(205, 217).trim).mkString.toDouble,
          "receive_fee" -> line.substring(218, 230).trim.toDouble,
          "cost_fee" -> line.substring(231, 243).trim.toDouble,
          "error_cause" -> line.substring(244, 248).trim,
          "rcvg_inst_id_code2" -> line.substring(249, 260).trim,
          "output_cardno" -> line.substring(261, 280).trim.trim,
          "rcvg_inst_id_code3" -> line.substring(281, 292).trim,
          "input_cardno" -> line.substring(293, 312).trim,
          "f90_3" -> line.substring(313, 323).trim,
          "card_seq_id" -> line.substring(324, 327).trim,
          "f60_2_2" -> line.substring(328, 329).trim,
          "f60_2_3" -> line.substring(330, 331).trim,
          "date_settlmt" -> line.substring(332, 336).trim,
          "amt_trans2" -> line.substring(337, 349).trim.toDouble,
          "trans_area_code" -> line.substring(350, 351).trim,
          "f60_2_8" -> line.substring(352, 354).trim,
          "card_accptr_id" -> "",
          "send_sett_institution" -> "",
          "rcvg_sett_institution" -> "",
          "into_sett_institution" -> "",
          "f60_2_5" -> "",
          "card_accptr_name_loc" -> "",
          "special_billing_type" -> "",
          "special_billing_level" -> "",
          "tac_flag" -> "",
          "card_mark_info" -> "",
          "err_original_code" -> "",
          "trans_start_way" -> "",
          "account_sett_type" -> "",
          "id" ->("aerr_"+line.substring(0, 3).trim+"_"+line.substring(4, 15).trim+"_"+line.substring(28, 34).trim+"_"+line.substring(35, 45).trim+"_"+line.substring(46, 65).trim+"_"+line.substring(66, 78).trim)
        )
      } else if (line.length > 354 && line.length < 545) {
        Map(
          "right_err_flag" -> line.substring(0, 3).trim,
          "acq_inst_id_code" -> line.substring(4, 15).trim,
          "fwd_inst_id_code" -> line.substring(16, 27).trim,
          "sys_trace_audit_num" -> line.substring(28, 34).trim,
          "transmsn_date_time" -> line.substring(35, 45).trim,
          "primary_acct_num" -> line.substring(46, 65).trim,
          "amt_trans" -> line.substring(66, 78).trim.toDouble,
          "message_type" -> line.substring(79, 83).trim,
          "processing_code" -> line.substring(84, 90).trim,
          "mchnt_type" -> line.substring(91, 95).trim,
          "card_accptr_termnl_id" -> line.substring(96, 104).trim,
          "retrivl_ref_num" -> line.substring(105, 117).trim,
          "pos_cond_code" -> line.substring(118, 120).trim,
          "authr_id_resp" -> line.substring(121, 127).trim,
          "rcvg_inst_id_code" -> line.substring(128, 139).trim,
          "bank_code" -> line.substring(140, 151).trim,
          "f90_2" -> line.substring(152, 158).trim,
          "resp_code" -> line.substring(159, 161).trim,
          "pos_entry_mode_code" -> line.substring(162, 165).trim,
          "recycle_fee" -> line.substring(166, 178).trim.toDouble,
          "pay_fee" -> line.substring(179, 191).trim.toDouble,
          "installment_pay_fee" -> pattern.findAllIn(line.substring(192, 204).trim).mkString.toDouble,
          "amt_trans_fee" -> pattern.findAllIn(line.substring(205, 217).trim).mkString.toDouble,
          "receive_fee" -> line.substring(218, 230).trim.toDouble,
          "cost_fee" -> line.substring(231, 243).trim.toDouble,
          "error_cause" -> line.substring(244, 248).trim,
          "rcvg_inst_id_code2" -> line.substring(249, 260).trim,
          "output_cardno" -> line.substring(261, 280).trim,
          "rcvg_inst_id_code3" -> line.substring(281, 292).trim,
          "input_cardno" -> line.substring(293, 312).trim,
          "f90_3" -> line.substring(313, 323).trim,
          "card_seq_id" -> line.substring(324, 327).trim,
          "f60_2_2" -> line.substring(328, 329).trim,
          "f60_2_3" -> line.substring(330, 331).trim,
          "date_settlmt" -> line.substring(332, 336).trim,
          "amt_trans2" -> line.substring(337, 349).trim.toDouble,
          "trans_area_code" -> line.substring(350, 351).trim,
          "f60_2_8" -> line.substring(352, 354).trim,
          "card_accptr_id" -> line.substring(355, 370).trim,
          "send_sett_institution" -> line.substring(371, 382).trim,
          "rcvg_sett_institution" -> line.substring(383, 394).trim,
          "into_sett_institution" -> line.substring(395, 406).trim,
          "f60_2_5" -> line.substring(407, 409).trim,
          //由于410-450字节存在汉字，所有从410以后计算gap并统一向前缩进相应的gap
          "card_accptr_name_loc" -> line.substring(410, 450 - gap).trim,
          "special_billing_type" -> line.substring(451 - gap, 453 - gap).trim,
          "special_billing_level" -> line.substring(454 - gap, 455 - gap).trim,
          "tac_flag" -> line.substring(456 - gap, 464 - gap).trim,
          "card_mark_info" -> line.substring(465 - gap, 489 - gap).trim,
          "err_original_code" -> line.substring(490 - gap, 493 - gap).trim,
          "trans_start_way" -> line.substring(494 - gap, 495 - gap).trim,
          "account_sett_type" -> line.substring(496 - gap, 498 - gap).trim,
          "id" ->("aerrn_"+line.substring(0, 3).trim+"_"+line.substring(4, 15).trim+"_"+line.substring(28, 34).trim+"_"+line.substring(35, 45).trim+"_"+line.substring(46, 65).trim+"_"+line.substring(66, 78).trim+"_"+line.substring(355, 370).trim)
        )
      } else {
        (">>>>>Exception", (line.length, line))
      }
    }).saveToEs({"err_"+args(1).substring(0,6)}+"/"+{args(1)},Map(
      "es.index.auto.create" -> "true",
      "es.mapping.id" -> "id",
      "es.mapping.exclude" -> "id"
    ))

  }
  /*//通过封装后的方法读取GBK文件,并讲每一行数据以字符串格式返回(RDD[String])
  def transfer(sc:SparkContext,path:String):RDD[String]={
    sc.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }*/
}
