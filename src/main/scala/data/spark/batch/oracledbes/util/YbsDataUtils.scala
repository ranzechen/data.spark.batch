package data.spark.batch.oracledbes.util

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import org.apache.commons.lang3.StringUtils

/**
  * Created by ranzechen on 2017/7/21.
  */
object YbsDataUtils {
  /**
    * @param strKey
    * @param strIn
    * @return
    */
  def encrypt(strKey: String, strIn: String) = {
    val skeySpec = getKey(strKey)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    val iv = new IvParameterSpec("0000000000000000".getBytes())
    cipher.init(1, skeySpec, iv)
    val encrypted = cipher.doFinal(strIn.getBytes())
    byte2hex(encrypted)
  }

  /**
    * AES加密 byte2hex,getKey,encrypt
    *
    * @param b
    * @return
    */
  def byte2hex(b: Array[Byte]): String = {
    var hs = ""
    var stmp = ""
    for (i <- 0 until b.length) {
      stmp = Integer.toHexString((b(i) & 0xFF))
      if (stmp.length == 1) {
        hs = hs + "0" + stmp
      } else {
        hs = hs + stmp
      }
    }
    hs.toUpperCase()
  }

  def getKey(strKey: String): SecretKeySpec = {
    val arrBTmp: Array[Byte] = strKey.getBytes()
    val arrB: Array[Byte] = new Array[Byte](16)
    for (i <- 0 until (if (arrB.length < arrBTmp.length) arrB.length else arrBTmp.length)) {
      arrB(i) = arrBTmp(i)
    }
    val skeySpec: SecretKeySpec = new SecretKeySpec(arrB, "AES")
    skeySpec
  }

  /**
    * @param str
    * @return true or false
    */
  def isNotEmpty(str: String): Boolean = {
    if((StringUtils.isNotEmpty(str)) && (!"".trim().equals(str)) && (!"null".trim().equals(str)))
      true
    else
      false
  }
}
