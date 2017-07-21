package data.spark.batch.oracledbes.util


import redis.clients.jedis.Jedis

/**
  * Created by ranzechen on 2017/7/17.
  */
object RedisService{
  /**
    * 提起地区码
    * @param areaCode
    * @return
    */
  def getAreaInfo(areaCode: String): String = {
    val jedis:Jedis = RedisClient.pool.getResource
    val areaInfo = jedis.get("essearch.cache.areainfo." + areaCode)
    RedisClient.pool.returnResource(jedis)
    if (areaInfo == null) "" else areaInfo
  }

  /**
    * 提取银行名称
    * @param bankCode
    * @return
    */
  def getBankInfo(bankCode: String): String = {
    val jedis:Jedis = RedisClient.pool.getResource
    val bankInfo = jedis.hget("essearch.cache.bankinfo." + bankCode, "bankname")
    RedisClient.pool.returnResource(jedis)
    if (bankInfo == null) "" else bankInfo
  }

  /**
    * 提取商户名称
    * @param merCode
    * @return
    */
  def getMerInfo(merCode: String): String = {
    val jedis:Jedis = RedisClient.pool.getResource
    val merInfo = jedis.hget("essearch.cache.merinfo." + merCode, "mercname")
    RedisClient.pool.returnResource(jedis)
    if (merInfo == null) "" else merInfo
  }

  /**
    * 提取部门名称
    * @param departCode
    * @return
    */
  def getDepartInfo(departCode: String): String = {
    val jedis:Jedis = RedisClient.pool.getResource
    val departInfo: String = jedis.hget("essearch.cache.settinfo." + departCode, "department")
    RedisClient.pool.returnResource(jedis)
    if (departInfo == null) "" else departInfo
  }

  /**
    * 提取转发机构号
    * @param settStr
    * @return
    */
  def getSettInfo(settStr: String): String = {
    val jedis:Jedis = RedisClient.pool.getResource
    val settInfo = jedis.hget("essearch.cache.settinfo." + settStr, "typename")
    RedisClient.pool.returnResource(jedis)
    if (settInfo == null) "" else settInfo
  }
}
