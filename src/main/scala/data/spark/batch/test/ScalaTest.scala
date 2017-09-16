package data.spark.batch.test



import data.spark.batch.memo1parse.util.raltutil

import scala.annotation.switch
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by ranzechen on 2017/9/7.
  */
object ScalaTest extends App {
  val s = "            0 02 03 1 0 5013             48615840    00095840    63020000    63020000    48615840    0200 000000 00 353642 0807103457 5201088008185598                                                              000000000000000000000084861584000800095840 03910001 861440350130391 000647001726 000000126600              D00000000025                            0                                                                                                    "
  println(s.length)
  println(s"${s.substring(41,52).trim}_${s.substring(116,122).trim}_${s.substring(123,133).trim}_${s.substring(134,153).trim}_${s.substring(293,305).trim}_${s.substring(264,279).trim}")
  println("---------------------")
  println(s.substring(0,11))
  println(s.substring(12,13))
  println(s.substring(14,16))
  println(s.substring(17,19))
  println(s.substring(20,21))
  println(s.substring(22,23))
  println(s.substring(24,28))
  println(s.substring(29,40))
  println(s.substring(41,52))
  println(s.substring(53,64))
  println(s.substring(65,76))
  println(s.substring(77,88))
  println(s.substring(89,100))
  println(s.substring(101,105))
  println(s.substring(106,112))
  println(s.substring(113,115))
  println(s.substring(116,122))
  println(s.substring(123,133))
  println(s.substring(134,153))
  println(s.substring(154,182))
  println(s.substring(183,211))
  println(s.substring(212,254))
  println(s.substring(255,263))
  println(s.substring(264,279))
  println(s.substring(280,292))
  println(s.substring(293,305))
  println(s.substring(306,318))
  println(s.substring(319,331))
  println(s.substring(332,344))
  println(s.substring(345,358))
  println(s.substring(359,360))
  println(s.substring(361,460))
  System.exit(0)
  /*val sparkConf = new SparkConf().setAppName("ScalaTest")
  val sc = new SparkContext(sparkConf)
  val s = raltutil.getSearcher("C:\\Users\\dell\\Desktop\\表数据\\RALT","C:\\Users\\dell\\Desktop\\表数据\\PPBANKDS","C:\\Users\\dell\\Desktop\\表数据\\RALT_CHOOSE_ITEM")
    .search("622202300600523066")
  println(s)
  println(s.get.bank_name)
  println(s.get.functions)
  println(s.get.card_type)*/
  val name = "ranzechen6666"
  val age = 23
  //从第一位开始取出多少个字符,并将第一个字符转为大写
  println(name.take(9).capitalize)
  //从第一位开始删掉多少个字符
  println(name.drop(9))
  //针对字符串的s插值法
  println(s"${name.substring(0, 9)} is $age")
  //自定义插值法----为什么用%s和%d : {%c->字符,%d->十进制数字,%e->指数浮点数,%f->浮点数,%i->整数(十进制),%o->八进制,%s->字符串,%%->打印一个百分号,\%->打印一个百分号}
  println("%s is %d".format(name.substring(0, 9), age))
  //针对数值的f插值法
  println(f"${name.substring(0, 9)} is $age%.2f ")
  //字符串中的字符大小写转换 注意:6那里的必须是单引号因为代表的是字符,只有字符串采用双引号
  println(s"${
    for {x <- name if x != '6'} yield {
      x.toUpper
    }
  } 等价于 ${name.filter(_ != '6').map(_.toUpper)}")
  println((1 to 10 by 2).toList)
  //生成1到10个数,步进为2返回值为range并转为List
  val res = (age: @switch) match {
    case 23 => "the age is right"
    case _ => "the age is error"
  }
  println(res)
}
