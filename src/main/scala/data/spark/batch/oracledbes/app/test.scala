package data.spark.batch.oracledbes.app

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.jedis.exceptions.JedisException

/**
  * Created by ranzechen on 2017/7/21.
  */
object test {
  private var jedisPool:JedisPool = null

  try {
    val config:GenericObjectPoolConfig = new GenericObjectPoolConfig()
    config.setMaxIdle(300)
    config.setMaxTotal(60000)
    config.setTestOnBorrow(true)
    jedisPool = new JedisPool(config, "168.33.222.97", 6379)//100.1.1.32//
  } catch {
    case t:Throwable => t.printStackTrace()
  }

  def getResource(): Jedis ={
    var jedis:Jedis  = null
    try {
      jedis = jedisPool.getResource()
    } catch {
      case e: JedisException => returnBrokenResource(jedis)
    }
    jedis
  }

  def returnBrokenResource(jedis:Jedis ) {
    if (jedis != null) {
      jedisPool.returnBrokenResource(jedis)
    }
  }
  def returnResource(jedis:Jedis) {
    if (jedis != null) {
      jedisPool.returnResource(jedis)
    }
  }

  // area是string类型
  def getAreaInfo(areaCode: String): String = {
    val jedis:Jedis = getResource()
    val areaInfo = jedis.get(areaCode)
    returnResource(jedis)
    if (areaInfo == null) "" else areaInfo
  }
  def main(args: Array[String]): Unit = {
    println(getAreaInfo("zhangsha").length)
    println(getAreaInfo("zhangshan").length)
    if (getAreaInfo("zhangsha").length == 0) println("kong") else  println(getAreaInfo("zhangshan"))
  }
}
