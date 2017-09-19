package data.spark.batch.memo1parse.util

/**
  * Created by ranzechen on 2017/9/15.
  * 映射YBS_SETT_DATA清算表数据
  */
case class ybs_sett_data_field(
                                tfmccode: String,
                                tfmemo1: String,
                                tfmemo: String,
                                tfcardno:String,
                                tfbncode:String,
                                card_type:String
                              )


object ybs_sett_data {
  def parse(line: String): ybs_sett_data_field = {
    val arr = line.split("衚")
    ybs_sett_data_field(
      tfmccode = arr(9).trim,
      tfmemo1 = arr(58).trim,
      tfmemo = arr(57).trim,
      tfcardno = arr(16).trim,
      tfbncode = arr(11).trim,
      card_type = arr(45).trim
    )
  }
}
