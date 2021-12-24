package com.optum.csp

import java.io.FileInputStream
import java.util.Properties

object CustomFunctions {
  def readProperties (input: String, enviParam: String): String = {
    try {
      //Logger.log.info(s"Reading Properties File with values input : $input and  envFile : $enviParam ")
      val prop = new Properties()
      val path = new FileInputStream("/mapr"+enviParam)
      prop.load(path)
      val value = prop.getProperty(s"$input")
      Logger.log.info(s" Value for the $input : $value ")
      // println( value )
      value
    } catch {
      case e: Exception => {
        Logger.log.info(" Exception at readProperties definition : " + e.getMessage)
        Logger.log.error("Error occured : " + e.getStackTrace.mkString("\n"))
      }
        throw e
    }
  }
}
