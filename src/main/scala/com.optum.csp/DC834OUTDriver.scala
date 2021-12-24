package com.optum.csp

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DC834OUTDriver{
  def main(args: Array[String]): Unit = {
    val vendorName = args(0)
    Logger.log.info(s"===> vendorName is $vendorName <===")
    var stateId = args(1)
    Logger.log.info(s"===> stateId is $stateId <===")
    val extractionMode = args(2)
    Logger.log.info(s"===> extractionMode is $extractionMode <===")
    val env = args(3)
    Logger.log.info(s"===> env is $env <===")
    val envFile = args(4).replace("/mapr", "")
    Logger.log.info(s"===> envFile is $envFile <===")

    val vendorPropFile = envFile.replace("environment", s"$vendorName").replace("/mapr", "")
    Logger.log.info(s"===> vendorPropFile is $vendorPropFile <===")
    Logger.log.info(s"===> Application Name is $vendorName <===")
    Logger.log.info(s"===> Started executing the job <===")
    Logger.log.info(s"===> Reading the vendor properties from /mapr/$vendorPropFile <===")

    val rootDir = CustomFunctions.readProperties( s"rootDir" ,vendorPropFile)

    val PDBC_TYPE = CustomFunctions.readProperties( s"PDBC_TYPE" ,vendorPropFile)
    Logger.log.info(s"===>PDBC_TYPE is $PDBC_TYPE <===")

    val PDBC_PFX = CustomFunctions.readProperties( s"PDBC_PFX" ,vendorPropFile)
    Logger.log.info(s"===>PDBC_PFX is $PDBC_PFX <===")

    val HDFS_VOLUME_PATH = CustomFunctions.readProperties( s"HDFS_VOLUME_PATH" ,envFile)
    Logger.log.info(s"===>HDFS_VOLUME_PATH is $HDFS_VOLUME_PATH <===")

    val HBASEDC834OUTELIGTABLENAME = HDFS_VOLUME_PATH + CustomFunctions.readProperties( s"HBASEELIGTABLENAME" ,vendorPropFile)
    Logger.log.info(s"===>HBASE ELIG TABLE NAME is $HBASEDC834OUTELIGTABLENAME <===")

    val HIVEDC834OUTELIGTABLENAME = CustomFunctions.readProperties( s"HIVEELIGTABLENAME" ,vendorPropFile)
    Logger.log.info(s"===>HIVE ELIG TABLE NAME is $HIVEDC834OUTELIGTABLENAME <===")

    Logger.log.info(s"===> Reading the environment properties from /mapr/$envFile <===")
    val CSP_HOME = CustomFunctions.readProperties( s"CSP_HOME" ,envFile).replace("/mapr", "")
    Logger.log.info(s"===>CSP_HOME is $CSP_HOME <===")

    val jsonDir = CSP_HOME + CustomFunctions.readProperties( s"jsonDir" ,envFile).replace("/mapr", "")
    Logger.log.info(s"===>jsonDir is $jsonDir <===")

    val commonmoduleStagingDir = CSP_HOME + CustomFunctions.readProperties( s"commonmoduleStagingDir" ,envFile).replace("/mapr","")
    Logger.log.info(s"===>commonmoduleStagingDir is $commonmoduleStagingDir <===")

    val HiveSchema = CustomFunctions.readProperties( s"HiveSchema" ,envFile)
    Logger.log.info(s"===>HiveSchema is $HiveSchema <===")

    val commonmodulelayoutsDir = CSP_HOME + CustomFunctions.readProperties( s"commonmodulelayoutsDir" ,envFile).replace("/mapr","")
    Logger.log.info(s"===>commonmodulelayoutsDir is $commonmodulelayoutsDir <===")

    val finaloutputDir = CSP_HOME + CustomFunctions.readProperties( s"finaloutputDir" ,envFile).replace("/mapr","")
    Logger.log.info(s"===>finaloutputDir is $finaloutputDir <===")

    val HBaseZooKeeperQuorum = CustomFunctions.readProperties( s"HBaseZooKeeperQuorum" ,envFile).replace("/mapr","")
    Logger.log.info(s"===>HBaseZooKeeperQuorum is $HBaseZooKeeperQuorum <===")

    val HBaseZooKeeperClientPort = CustomFunctions.readProperties( s"HBaseZooKeeperClientPort" ,envFile).replace("/mapr","")
    Logger.log.info(s"===>HBaseZooKeeperClientPort is $HBaseZooKeeperClientPort <===")

    Logger.log.info(s"===> Launcing the  Spark Session <===")
    val appName = vendorName + "_" + stateId + "_" + extractionMode + "_" + env + "_EXTRACT"
    Logger.log.info(s"===> Setting the app Name for Spark Session to $appName <===")
    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    Logger.log.info(s"===> Spark Session is established : $spark <===")
    val conf = new SparkConf().setAppName(appName).setMaster("yarn")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sqlContext = spark.sqlContext

    def getCurrentDateTime(dateTimeFormat: String): String = {
      val dateFormat = new SimpleDateFormat(dateTimeFormat)
      val cal = Calendar.getInstance()
      dateFormat.format(cal.getTime)
    }
    def getCurrentTsFormat: String = getCurrentDateTime("yyyyMMddHHmmss").substring(0, 14)

    val ExtractionStartTime = getCurrentTsFormat

    var BRC_NAME=""
    if(stateId=="ALL")
    {
      BRC_NAME = CustomFunctions.readProperties( s"ALL_BRC_NAME" ,vendorPropFile)
      Logger.log.info(s"===>BRC_NAME is $BRC_NAME <===")
    }
    if(stateId != "ALL")
    {
      BRC_NAME = CustomFunctions.readProperties( s"INCLUSION_BRC_NAME" ,vendorPropFile)
      Logger.log.info(s"===>BRC_NAME is $BRC_NAME <===")
    }
    val BrclTable = CustomFunctions.readProperties( s"BRCL" ,envFile)

    Logger.log.info(s"===> DC834OUT Extraction Started at $ExtractionStartTime <===")
    val ExtractionStartTimeMillis = System.currentTimeMillis()
    try {
      DC834OUTExtract.setappName(appName)
      DC834OUTExtract.createDC834OUTExtract(extractionMode, commonmoduleStagingDir, vendorName, stateId, HiveSchema, commonmodulelayoutsDir, finaloutputDir,HBASEDC834OUTELIGTABLENAME, HIVEDC834OUTELIGTABLENAME, HBaseZooKeeperQuorum, HBaseZooKeeperClientPort, envFile)
    } catch {
      case e: Exception => {
        Logger.log.info(" Exception at generating DC834OUT Extract : " + e.getMessage)
        Logger.log.error("Error occured : " + e.getStackTrace.mkString("\n"))
      }
        throw e
    }
    val ExtractionEndTime = getCurrentTsFormat
    Logger.log.info(s"===> DC834OUT Extraction Ended at $ExtractionEndTime <===")
    val ExtractionEndTimeMillis = System.currentTimeMillis()
    val DC834OUTExtractDuration=(ExtractionEndTimeMillis - ExtractionStartTimeMillis) / 60000
    Logger.log.info(s"===> DC834OUT Extraction Duration is $DC834OUTExtractDuration <===")
  }
}