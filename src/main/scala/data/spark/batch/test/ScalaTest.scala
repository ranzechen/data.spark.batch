package data.spark.batch.test


import data.spark.batch.dataCleaning.raltutil

import scala.annotation.switch
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by ranzechen on 2017/9/7.
  */
object ScalaTest extends App {
  val s = "[421317      |600403|debit     |16          ]:4213173000730440    #王研                #      #                    #600403  #debit     "
  println("瑾                  ".trim.length)
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
