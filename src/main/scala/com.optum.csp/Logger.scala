package com.optum.csp

import org.apache.log4j.Level

/**

  */
object Logger {
  @transient lazy val log: org.apache.log4j.Logger = org.apache.log4j.LogManager.getLogger(getClass.getName)
  log.setLevel(Level.ALL)
}