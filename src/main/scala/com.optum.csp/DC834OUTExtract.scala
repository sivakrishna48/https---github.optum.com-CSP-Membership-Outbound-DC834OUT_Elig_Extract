package com.optum.csp


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/* This Class is to generate the DC834OUT vendor Specific exttact by applying business logic on top of Common Module output */
object DC834OUTExtract {
  var appName = ""

  def setappName(appName1: String) = {
    appName = appName1
  }

  val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
  val conf = new SparkConf().setAppName(appName).setMaster("yarn")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sqlContext = spark.sqlContext

  import sqlContext.implicits._



  def createDC834OUTExtract(extractionMode: String, CommonModuleFilesDir: String, vendorName: String, stateId: String, HiveSchema: String, commonmodulelayoutsDir: String, vendorOutputFileDir: String, HBASEDC834OUTELIGTABLENAME: String, HIVEDC834OUTELIGTABLENAME: String, HBaseZooKeeperQuorum: String, HBaseZooKeeperClientPort: String, envFile: String): Unit = {
    var membersdf: org.apache.spark.sql.DataFrame = spark.emptyDataFrame;
    var DC834OUTELIGDF: org.apache.spark.sql.DataFrame = spark.emptyDataFrame;

    var PRPR = CustomFunctions.readProperties(s"PRPR", envFile)
    var PRRG = CustomFunctions.readProperties(s"PRRG", envFile)
    /* assigining demo, elig and cob layouts from the common module layouts directory */
    var demolayoutfile = commonmodulelayoutsDir.replace("/mapr", "") + "/DEMOLAYOUT/"
    var eliglayoutfile = commonmodulelayoutsDir.replace("/mapr", "") + "/ELIGLAYOUT/"
    var coblayoutfile = commonmodulelayoutsDir.replace("/mapr", "") + "/COBLAYOUT/"

    /* converting the layouts that are assigned in previous steps into schema */
    var demoschema = spark.sqlContext.read.format("csv").option("delimiter", "|").option("header", "true").load(demolayoutfile).schema
    var eligschema = spark.sqlContext.read.format("csv").option("delimiter", "|").option("header", "true").load(eliglayoutfile).schema
    var cobschema = spark.sqlContext.read.format("csv").option("delimiter", "|").option("header", "true").load(coblayoutfile).schema

    /* mapping the schema to corresponding output file of common module */

    Logger.log.info(s"===> Reading the $vendorName $stateId  DEMO File from $CommonModuleFilesDir <===")
    var demoDF = spark.sqlContext.read.format("csv").option("delimiter", "|").schema(demoschema).load(CommonModuleFilesDir + "/" + vendorName + "/" + extractionMode + "/" + stateId + "/DEMOFILE")

    Logger.log.info(s"===> Reading the $vendorName $stateId  ELIG File from $CommonModuleFilesDir <===")
    var eligDF = spark.sqlContext.read.format("csv").option("delimiter", "|").schema(eligschema).load(CommonModuleFilesDir + "/" + vendorName + "/" + extractionMode + "/" + stateId + "/ELIGFILE")

    Logger.log.info(s"===> Reading the $vendorName $stateId  COB File from $CommonModuleFilesDir <===")
    var cobDF = spark.sqlContext.read.format("csv").option("delimiter", "|").schema(cobschema).load(CommonModuleFilesDir + "/" + vendorName + "/" + extractionMode + "/" + stateId + "/COBFILE")


    Logger.log.info(s"===> Creating Temp Table for DEMO File<===")
    demoDF.createOrReplaceTempView("DEMO_DC834OUT_TABLE_VIEW")

    Logger.log.info(s"===> Creating Temp Table for eligrecordsDF<===")
    eligDF.createOrReplaceTempView("ELIG_DC834OUT_TABLE_VIEW")

    Logger.log.info(s"===> Lodaing Demo details into data frame<===")
    val finaldemoDF = spark.sql("SELECT CM_DEMO_SBSB_ID AS SBSB_ID," +
      "1 AS REC_NUM," +
      "'17531231' as EFF_DT ," +
      " CONCAT_WS('',RPAD (COALESCE (TRIM (DEMO.RECORD_TYPE), ' '), 4, ' ')," +
      " RPAD (COALESCE(TRIM(CM_DEMO_SBSB_ID), ' '), 9, ' ')," +
      " RPAD (COALESCE (TRIM (CM_DEMO_GRGR_ID), ' '), 8, ' ')," +
      " RPAD (COALESCE (TRIM (CM_MEME_MEDCD_NO), ' '), 20, ' ')," +
      " RPAD (COALESCE (TRIM(CM_MEME_HICN), ' '), 12, ' ')," +
      " RPAD (COALESCE (TRIM (CM_MEME_RECORD_NO), ' '), 11, ' ')," +
      " RPAD (COALESCE (TRIM (CM_MEME_FAM_LINK_ID), ' '), 12, ' ')," +
      " RPAD (COALESCE (TRIM(CM_MEME_LAST_NAME), ' '), 35, ' ')," +
      " RPAD (COALESCE (TRIM (CM_MEME_FIRST_NAME), ' '),15, ' ')," +
      " RPAD (COALESCE (TRIM (CM_MEME_MID_INIT), ' '), 1, ' ')," +
      " RPAD (COALESCE (TRIM(CM_MEME_TITLE), ' '), 10, ' ')," +
      " case when TRIM (CM_MEME_SSN) = '' or TRIM (CM_MEME_SSN) is null then '000000000' else RPAD (COALESCE (TRIM(CM_MEME_SSN), ' '), 9, ' ') end," +
      " case when TRIM(CM_SBAD_PHONE_HOME) = '' OR TRIM(CM_SBAD_PHONE_HOME) is null then '0000000000' else RPAD (COALESCE (TRIM(CM_SBAD_PHONE_HOME), ' '), 20, ' ') end," +
      " RPAD (COALESCE (TRIM(CM_MEME_WRK_PHONE), ' '), 20, ' ')," +
      " RPAD (COALESCE (TRIM(CM_MEME_CELL_PHONE), ' '), 20, ' ')," +
      " RPAD (COALESCE (TRIM (CM_SBAD_ADDR1_HOME), ' '), 40, ' ')," +
      " RPAD (COALESCE(TRIM(CM_SBAD_ADDR2_HOME), ' '), 40, ' ')," +
      " RPAD (COALESCE (TRIM (CM_SBAD_CITY_HOME),' '), 19, ' ')," +
      " RPAD (COALESCE (TRIM (CM_SBAD_STATE_HOME), ' '), 2, ' ')," +
      " RPAD(COALESCE (TRIM(CM_SBAD_ZIP_HOME), ' '), 11, ' ')," +
      " RPAD (COALESCE (TRIM(CM_MEME_BIRTH_DT), ' '), 8,' ')," +
      " RPAD (COALESCE (TRIM (CM_MEME_SEX), ' '), 1, ' ')," +
      " RPAD (COALESCE (TRIM(CM_MEME_MCTR_LANG), ' '), 4, ' ')," +
      " RPAD (COALESCE (TRIM(CM_DEMO_MECD_MDCT), ' '), 20,' ')," +
      " case when TRIM (CM_ELIG_IND)='Y' then  RPAD (COALESCE (TRIM(CM_ELIG_IND), ' '), 1, ' ') else space(1) end," +
      " case when TRIM(CM_MEME_QUALIFY_DT) = '17530101' then space(8) else RPAD (COALESCE (TRIM(CM_MEME_QUALIFY_DT), ' '), 8, ' ') end," +
      " RPAD (COALESCE (TRIM(CM_SBAD_ADDR1_MAIL), ' '), 40, ' ')," +
      " RPAD (COALESCE (TRIM(CM_SBAD_ADDR2_MAIL), ' '),40, ' ')," +
      " RPAD (COALESCE (TRIM(CM_SBAD_CITY_MAIL), ' '),19, ' ')," +
      " RPAD (COALESCE (TRIM(CM_SBAD_STATE_MAIL), ' '), 2,' ')," +
      " RPAD (COALESCE(TRIM (CM_SBAD_ZIP_MAIL), ' '), 11,' ')," +
      " case when TRIM(CM_MEME_HIST_LINK_ID) = '' then space(12) else RPAD (COALESCE (TRIM(CM_MEME_HIST_LINK_ID), ' '), 12, ' ') end," +
      " case when TRIM(CM_SBSB_RETIRE_DT) = '17530101' OR TRIM(CM_SBSB_RETIRE_DT) = '19530101' OR TRIM(CM_SBSB_RETIRE_DT) = '' OR TRIM(CM_SBSB_RETIRE_DT) is null then '00010101' else RPAD (COALESCE (TRIM(CM_SBSB_RETIRE_DT), ' '), 8, ' ') end," +
      " RPAD (COALESCE(TRIM (CM_MEME_MARITAL_STATUS), ' '), 1,' ')," +
      " RPAD (COALESCE(TRIM (CM_SBSB_MCTR_VIP), ' '), 1,' ')," +
      " RPAD (COALESCE(TRIM(CM_ELIG_EFF_DT), ' '), 8, ' ')," +
      " RPAD (COALESCE(TRIM(CM_ELIG_TERM_DT), ' '), 8, ' ')," +
      " RPAD (COALESCE (TRIM (CM_MECD_MCTR_AIDC), ' '), 4, ' ')," +
      " space(486)) AS REC_TYPE, SUBSTR(CM_DEMO_GRGR_ID,1,2) AS STATE_ID,CM_DEMO_MEME_CK,CM_DEMO_GRGR_CK,CM_ELIG_EFF_DT AS CM_DEMO_EFF_DT,CM_ELIG_TERM_DT AS CM_DEMO_TERM_DT " +
      "FROM DEMO_DC834OUT_TABLE_VIEW DEMO " +
      "JOIN ELIG_DC834OUT_TABLE_VIEW ELIG " +
 //     "(select CM_ELIG_GRGR_CK,CM_ELIG_MEME_CK,CM_MECD_MCTR_AIDC,MAX(CM_ELIG_EFF_DT) as CM_ELIG_EFF_DT_MAX,MAX(CM_ELIG_TERM_DT) as CM_ELIG_TERM_DT_MAX " +
//      "FROM ELIG_DC834OUT_TABLE_VIEW ELIG WHERE CM_MEPE_ELIG_IND = 'Y' and " +
      " ON CM_MEPE_ELIG_IND = 'Y'" +
      " AND from_unixtime(unix_timestamp(CM_ELIG_EFF_DT, 'yyyyMMdd'), 'yyyy-MM-dd') <= from_unixtime(unix_timestamp(current_date, 'yyyyMMdd'), 'yyyy-MM-dd')" +
      " AND from_unixtime(unix_timestamp(CM_ELIG_TERM_DT, 'yyyyMMdd'), 'yyyy-MM-dd') >= from_unixtime(unix_timestamp(current_date, 'yyyyMMdd'), 'yyyy-MM-dd')" +
      " AND DEMO.CM_DEMO_MEME_CK=ELIG.CM_ELIG_MEME_CK" +
      " AND DEMO.CM_DEMO_GRGR_CK=ELIG.CM_ELIG_GRGR_CK")

     finaldemoDF.createOrReplaceTempView("DEMO_DC834OUTFINAL_TABLE_VIEW")

//    Logger.log.info(s"===> pulling maint type code & reason code details into data frame<===")
//    val demoDF_mtc = spark.sql("SELECT DEMO.*,case when dc834out.MEME_CK is null then '021' " +
//      "when dc834out.MEME_CK is not null and from_unixtime(unix_timestamp(CM_ELIG_TERM_DT_MAX, 'yyyyMMdd'), 'yyyy-MM-dd') < from_unixtime(unix_timestamp(current_date, 'yyyyMMdd'), 'yyyy-MM-dd')  then '024' else '001' end " +
//      "from DEMO_DC834OUTELIG_TABLE_VIEW DEMO left join $HiveSchema.$HIVEDC834OUTELIGTABLENAME dc834out " +
//      "on DEMO.CM_DEMO_MEME_CK = dc834out.MEME_CK")
//
//    demoDF_mtc.createOrReplaceTempView("DEMO_MTC_DC834OUT_TABLE_VIEW")
//
//    val demoDF_mrcd = spark.sql("SELECT DEMO.*,MEPE.MEPE_MCTR_RSN " +
//      "from DEMO_MTC_DC834OUT_TABLE_VIEW DEMO left join $HiveSchema.$MEPE MEPE on DEMO.CM_DEMO_MEME_CK = MEPE.MEME_CK ")
//
//    demoDF_mrcd.createOrReplaceTempView("DEMO_MCDID_DC834OUT_TABLE_VIEW")

//    val finaldemoDF = spark.sql("SELECT CM_DEMO_SBSB_ID AS SBSB_ID," +
//      "1 AS REC_NUM," +
//      "'17531231' as EFF_DT ," +
//      " CONCAT_WS('',REC_TYP," +
//      " RPAD (COALESCE(TRIM(elig_maint_typ_cd), ' '), 3, ' ')," +
//      " RPAD (COALESCE (TRIM (MEPE_MCTR_RSN), ' '), 4, ' ')," +
//      " space(490)) AS REC_TYPE,STATE_ID " +
//      "from DEMO_DC834OUTELIG_TABLE_VIEW")

    Logger.log.info(s"===> Loading ELIGIBILITY details into data frame<===")
    val eligDemoDF = spark.sql(s"SELECT CM_ELIG_SBSB_ID AS SBSB_ID" +
      s",2 AS REC_NUM" +
      s",CM_ELIG_EFF_DT AS EFF_DT" +
      s",CONCAT_WS('',RPAD (COALESCE (TRIM (ELIG.RECORD_TYPE), ' '), 4, ' ')" +
      s", RPAD (COALESCE(TRIM(CM_DEMO_EFF_DT), ' '), 8, ' ')" +
      s", RPAD (COALESCE(TRIM(CM_DEMO_TERM_DT), ' '), 8, ' ')" +
      s", RPAD (COALESCE (TRIM (CM_CSPI_ID), ' '), 8, ' ')" +
      s", RPAD (COALESCE (TRIM(CM_SGSG_ID), ' '), 4, ' ')" +
      s", RPAD (COALESCE (TRIM (CM_GRGR_ID), ' '), 8, ' ')" +
      s", RPAD(COALESCE (TRIM(CM_CSCS_ID), ' '), 4, ' ')" +
      s", RPAD (COALESCE (TRIM (CM_PDPD_ID), ' '), 8, ' ')" +
      s", case when TRIM (CM_PRPR_ENTITY) is null then space(35) when TRIM (CM_PRPR_ENTITY)='P' then RPAD(COALESCE (TRIM (CM_PRCP_LAST_NAME), ' '), 35, ' ') else RPAD(COALESCE (TRIM (CM_PRPR_NAME), ' '), 35, ' ') end" +
      s", case when TRIM (CM_PRPR_ENTITY) is null then space(15) when TRIM (CM_PRPR_ENTITY)='P' then  RPAD (COALESCE (TRIM(CM_PRCP_FIRST_NAME), ' '), 15, ' ') else space(15) end" +
      s", RPAD (COALESCE (TRIM (CM_PRCP_MID_INIT), ' '), 1,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRPR_ID), ' '), 12, ' ')" +
      s", RPAD (COALESCE (TRIM(CM_PRPR_NPI), ' '), 10, ' ')" +
      s", RPAD (COALESCE (TRIM (CM_PCP_TIN), ' '), 9, ' ')" +
      s", case when TRIM (CM_PRPR_ENTITY) is null then space(1) when TRIM (CM_PRPR_ENTITY)='P' then '1' else '2' end " +
      s", space(37)" +
      s", RPAD (COALESCE (TRIM (CM_MECD_MCTR_AIDC), ' '), 4, ' ')" +
      s", RPAD (COALESCE (TRIM (CM_ELIG_MECD_MDCT),' '),20,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRAD_ADDR1),' '),40,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRAD_ADDR2),' '),40,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRAD_CITY),' '),19,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRAD_STATE),' '),2,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRAD_ZIP),' '),11,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRAD_CTRY_CD),' '),4,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PRPR_CLINIC_NAME),' '),55,' ')" +
      s", RPAD (COALESCE (TRIM (CM_PCP_CLINIC_TIN),' '),9,' ')" +
      s",space(7)) AS REC_TYP,  SUBSTR(CM_GRGR_ID,1,2) AS STATE_ID, CM_ELIG_MEME_CK, CM_PRPR_ID " +
      s"FROM ELIG_DC834OUT_TABLE_VIEW ELIG " +
      s"JOIN DEMO_DC834OUTFINAL_TABLE_VIEW DEMO " +
      s"ON ELIG.CM_ELIG_MEME_CK=DEMO.CM_DEMO_MEME_CK " +
      s"AND ELIG.CM_ELIG_GRGR_CK=DEMO.CM_DEMO_GRGR_CK " +
      s"AND ELIG.CM_ELIG_EFF_DT=DEMO.CM_DEMO_EFF_DT " +
      s"AND ELIG.CM_ELIG_TERM_DT=DEMO.CM_DEMO_TERM_DT")

    eligDemoDF.createOrReplaceTempView("ELIG_DC834OUTDEMO_TABLE_VIEW")

    eligDemoDF.printSchema()
    Logger.log.info(s"===> pulling PCP medicaid id into data frame<===")

    val prrgdf = sqlContext.sql(s"select PRCR_ID, PRRG_MCTR_TYPE, max(PRRG_SEQ_NO) AS PRRG_SEQ_NO_MAX " +
      s"from $HiveSchema.$PRRG where TRIM(PRRG_MCTR_TYPE) = 'MD' " +
      s"group by PRCR_ID,PRRG_MCTR_TYPE")

    prrgdf.createOrReplaceTempView("PRRG_TABLE_VIEW")

    val eligDF_prrg = sqlContext.sql(s"SELECT ELIG.*," +
      s"PRPR.PRCR_ID,case when TRIM(PRRG.PRRG_MCTR_TYPE) = 'MD' then TRIM (PRRG.PRRG_ID) else space(15) end AS elig_pcp_mdcd_id " +
      s"from ELIG_DC834OUTDEMO_TABLE_VIEW ELIG left outer join $HiveSchema.$PRPR PRPR " +
      s"on elig.CM_PRPR_ID = PRPR.PRPR_ID " +
      s"left outer join " +
      s"  (select PRRG.PRCR_ID,PRRG.PRRG_MCTR_TYPE,PRRG.PRRG_ID " +
      s"  from $HiveSchema.$PRRG PRRG " +
      s"   inner join PRRG_TABLE_VIEW PRRGV " +
      s"   on PRRG.PRCR_ID = PRRGV.PRCR_ID and PRRG.PRRG_MCTR_TYPE = PRRGV.PRRG_MCTR_TYPE and PRRG.PRRG_SEQ_NO = PRRGV.PRRG_SEQ_NO_MAX) PRRG " +
      s"on PRRG.PRCR_ID = PRPR.PRCR_ID ")

//    val eligDF_prrg = spark.sql("SELECT ELIG.*,PRPR.PRCR_ID, PRRG_ID " +
//      "from ELIG_DC834OUTDEMO_TABLE_VIEW ELIG left outer join $HiveSchema.$PRPR PRPR on elig.CM_PRPR_ID = PRPR.PRPR_ID left outer join $HiveSchema.CMC_PRRG_REG PRRG " +
//      "on PRRG.PRCR_ID = PRPR.PRCR_ID")

    eligDF_prrg.createOrReplaceTempView("ELIG_PRRG_DC834OUT_TABLE_VIEW")

    eligDF_prrg.printSchema()
    val finaleligDF = spark.sql("SELECT SBSB_ID," +
      "2 AS REC_NUM," +
      "EFF_DT," +
      " CONCAT_WS('',REC_TYP," +
      //      " space(15)," +
      " RPAD (COALESCE(TRIM(elig_pcp_mdcd_id), ' '), 15, ' ')," +
//      " space(15)," +
      " space(602)) AS REC_TYPE,STATE_ID " +
//      "from ELIG_DC834OUTDEMO_TABLE_VIEW")
      "from ELIG_PRRG_DC834OUT_TABLE_VIEW")

    //      Logger.log.info(s"===> Loading COB details into data frame<===")
//    val cobDF1 = spark.sql("select distinct COB.* from COB_DC834OUT_TABLE_VIEW COB INNER JOIN DEMO_DC834OUT_TABLE_VIEW DEMO ON COB.CM_COB_SBSB_ID = DEMO.CM_DEMO_SBSB_ID")
//    cobDF1.createOrReplaceTempView("COB_DC834OUT_TABLE_VIEW")
//
//    val finalcobDF = spark.sql("select SBSB_ID,REC_NUM,EFF_DT ,REC_TYPE,STATE_ID " +
//      "from (SELECT COB.CM_COB_SBSB_ID AS SBSB_ID" +
//      ",3 AS REC_NUM" +
//      ", COB.CM_COB_EFF_DT AS EFF_DT" +
//      ", SUBSTR(COB.CM_COB_GRGR_ID,1,2) AS STATE_ID" +
//      ", row_number() OVER (PARTITION BY COB.CM_COB_SBSB_ID ORDER BY COB.CM_COB_EFF_DT DESC) AS rank" +
//      ", CONCAT_WS('',RPAD (COALESCE (TRIM (COB.RECORD_TYPE), ' '), 4, ' ')" +
//      ", RPAD (COALESCE (TRIM(COB.CM_CARRIER_ID), ' '), 9, ' ')" +
//      ", RPAD (COALESCE (TRIM (COB.CM_COB_INSUR_TYPE), ' '),1, ' ')" +
//      ", RPAD (COALESCE (TRIM (COB.CM_COB_POLICY_ID), ' '), 25, ' ')" +
//      ", RPAD (COALESCE (TRIM(COB.CM_COB_EFF_DT), ' '), 8, ' ')" +
//      ", RPAD (COALESCE (TRIM (COB.CM_COB_TERM_DT), ' '), 8, ' ')" +
//      ",RPAD (COALESCE (TRIM (COB.CM_MCRE_NAME), ' '), 50, ' ')" +
//      ", RPAD (COALESCE (TRIM(COB.CM_MCRE_ADDR1), ' '), 40, ' ')" +
//      ", RPAD (COALESCE (TRIM (COB.CM_MCRE_ADDR2), ' '), 40, ' ')" +
//      ", RPAD (COALESCE(TRIM (COB.CM_MCRE_CITY), ' '), 19, ' ')" +
//      ", RPAD (COALESCE (TRIM(COB.CM_MCRE_STATE), ' '), 2, ' ')" +
//      ", RPAD (COALESCE (TRIM (COB.CM_MCRE_ZIP), ' '), 11, ' ')" +
//      ", RPAD (COALESCE (TRIM(COB.CM_MCRE_PHONE), ' '), 20, ' ')" +
//      ", space(763)) AS REC_TYPE " +
//      "FROM COB_DC834OUT_TABLE_VIEW COB) STEP where step.rank <=5")

    Logger.log.info(s"===> MERGING DEMO & ELIG & COB Records <===")
//    val finalDC834OUTDF = finaldemoDF.select('rec_type, 'SBSB_ID, 'REC_NUM, 'EFF_DT,'STATE_ID).union(finaleligDF.select('rec_type, 'SBSB_ID, 'REC_NUM, 'EFF_DT,'STATE_ID)).union(finalcobDF.select('rec_type, 'SBSB_ID, 'REC_NUM, 'EFF_DT,'STATE_ID))
    val finalDC834OUTDF = finaldemoDF.select('rec_type, 'SBSB_ID, 'REC_NUM, 'EFF_DT,'STATE_ID).union(finaleligDF.select('rec_type, 'SBSB_ID, 'REC_NUM, 'EFF_DT,'STATE_ID))
    Logger.log.info(s"===> creating temporary view for final file <===")
    Logger.log.info(s"===> creating dataframe sorted order  <===")
    Logger.log.info(s"===> Creating Final File for DC834OUT type count:  ${ finalDC834OUTDF.count()} <===")
    //finalDC834OUTDF.createOrReplaceTempView("DC834OUTDATA_VIEW")
    //Logger.log.info(s"===> creating the dataframe finalDC834OUTFileDF by left joining with static_states_table <===")
    //val finalDC834OUTFileDF=sqlContext.sql("select case when trim(DC834OUT.STATE_ID) is not null then DC834OUT.rec_type else '' end as rec_type,DC834OUT.SBSB_ID,DC834OUT.REC_NUM,DC834OUT.EFF_DT,static_states.STATE_ID as STATE_ID from static_states_table static_states left outer join DC834OUTDATA_VIEW DC834OUT on static_states.STATE_ID=DC834OUT.STATE_ID")

    finalDC834OUTDF.orderBy($"SBSB_ID", $"REC_NUM", $"EFF_DT".asc).coalesce(1).select($"rec_type").write.mode("overwrite").format("text").save(vendorOutputFileDir.replace("/mapr", "") +"/"+ s"$vendorName/$extractionMode/$stateId")
    Logger.log.info(s"===> DC834OUT FINAL FILE CREATED <===")

    if (extractionMode == "CHANGE") {
      membersdf = demoDF.as('DEMO).join(eligDF.as('ELIG), $"DEMO.CM_DEMO_MEME_CK" === $"ELIG.CM_ELIG_MEME_CK", "INNER").as('DEMOELIG).join(cobDF.as('COB), $"DEMOELIG.CM_ELIG_MEME_CK" === $"COB.CM_COB_MEME_CK", "LEFT_OUTER").select($"DEMOELIG.*", $"COB.*", concat($"DEMOELIG.CM_ELIG_GRGR_ID", $"DEMOELIG.CM_CSPI_ID").alias("GRGR_ID_CSPI_ID"), concat($"DEMOELIG.CM_ELIG_GRGR_ID", $"DEMOELIG.CM_CSCS_ID", $"DEMOELIG.CM_SGSG_ID").alias("GRGR_ID_CSCS_ID_SGSG_ID"), concat($"DEMOELIG.CM_ELIG_GRGR_ID", $"DEMOELIG.CM_CSPI_ID", $"DEMOELIG.CM_CSCS_ID").alias("GRGR_ID_CSPI_ID_CSCS_ID"), concat($"DEMOELIG.CM_ELIG_GRGR_ID", $"DEMOELIG.CM_CSPI_ID", $"DEMOELIG.CM_SGSG_ID").alias("GRGR_ID_CSPI_ID_SGSG_ID"), concat($"DEMOELIG.CM_ELIG_GRGR_ID", $"DEMOELIG.CM_SGSG_ID").alias("GRGR_ID_SGSG_ID"), concat($"DEMOELIG.CM_ELIG_GRGR_ID", $"DEMOELIG.CM_CSCS_ID").alias("GRGR_ID_CSCS_ID"), $"DEMOELIG.CM_ELIG_SBSB_ID".alias("SBSB_ID"), $"DEMOELIG.CM_MEME_MEDCD_NO".alias("MEME_MEDCD_NO"))

      Logger.log.info(s"===> membersdf CREATED <===")

      DC834OUTELIGDF = sqlContext.sql(s"select * from $HiveSchema.$HIVEDC834OUTELIGTABLENAME")
      var DC834OUTELIGTABLENAME = HBASEDC834OUTELIGTABLENAME
      val Hbaseconf = HBaseConfiguration.create()
      Hbaseconf.set("hbase.zookeeper.quorum", s"$HBaseZooKeeperQuorum")
      Hbaseconf.set("hbase.zookeeper.property.clientPort", s"$HBaseZooKeeperClientPort")
      Hbaseconf.set(TableOutputFormat.OUTPUT_TABLE, DC834OUTELIGTABLENAME)
      val hbaseContext = new HBaseContext(spark.sparkContext, Hbaseconf)

      val jobConf = new Configuration(Hbaseconf)
      jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
      jobConf.set("mapreduce.job.output.value.class", classOf[LongWritable].getName)
      jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)

      def hbaseDeleteExisting(membersdf: DataFrame, DC834OUTELIGDF: DataFrame, DC834OUTELIGTABLENAME: String): Unit = {
        val deletionrowkeys = membersdf.as('df1).join(DC834OUTELIGDF.as('df2), $"df1.CM_DEMO_MEME_CK" === $"df2.MEME_CK", "left_outer").filter($"df2.MEME_CK".isNotNull).select(concat($"df2.MEME_CK", lit("_"), $"df2.MEPE_EFF_DT").alias("row_key")).filter($"df1.CM_ELIG_IND" === "Y")

        deletionrowkeys.createOrReplaceTempView("deletionrowkeysgenerationview")
        val deletionrowkeyslist = sqlContext.sql("select row_key from deletionrowkeysgenerationview").map(r => r.getString(0)).collect.toArray
        val deletionrowkeyslistBytes = spark.sparkContext.parallelize(deletionrowkeyslist.map(x => Bytes.toBytes(x)))
        hbaseContext.bulkDelete[Array[Byte]](deletionrowkeyslistBytes, TableName.valueOf(DC834OUTELIGTABLENAME), putRecord => new Delete(putRecord), 10)
      }

      def hbaseinsertrecords(membersdf: DataFrame, DC834OUTEELIGDF: DataFrame, DC834OUTELIGTABLENAME: String): Unit = {
        try{

          val insertionrowkeys_0=membersdf.as('df1).select ($"df1.CM_ELIG_IND",
            when($"df1.CM_DEMO_MEME_CK".isNull or trim($"df1.CM_DEMO_MEME_CK")==="","").otherwise($"df1.CM_DEMO_MEME_CK").alias("CM_DEMO_MEME_CK"),
            when($"df1.CM_ELIG_EFF_DT".isNull or trim($"df1.CM_ELIG_EFF_DT")==="","").otherwise($"df1.CM_ELIG_EFF_DT").alias("CM_ELIG_EFF_DT"),
            when($"df1.CM_ELIG_TERM_DT".isNull or trim($"df1.CM_ELIG_TERM_DT")==="","").otherwise($"df1.CM_ELIG_TERM_DT").alias("CM_ELIG_TERM_DT"),
            when($"df1.CM_CSPI_ID".isNull or trim($"df1.CM_CSPI_ID")==="","").otherwise($"df1.CM_CSPI_ID").alias("CM_CSPI_ID"),
            when($"df1.CM_SGSG_ID".isNull or trim($"df1.CM_SGSG_ID")==="","").otherwise($"df1.CM_SGSG_ID").alias("CM_SGSG_ID"),
            when($"df1.CM_CSCS_ID".isNull or trim($"df1.CM_CSCS_ID")==="","").otherwise($"df1.CM_CSCS_ID").alias("CM_CSCS_ID"),
            when($"df1.CM_MEPE_FI".isNull or trim($"df1.CM_MEPE_FI")==="","").otherwise($"df1.CM_MEPE_FI").alias("CM_MEPE_FI"),
            when($"df1.CM_PDPD_ID".isNull or trim($"df1.CM_PDPD_ID")==="","").otherwise($"df1.CM_PDPD_ID").alias("CM_PDPD_ID")
          )

          val insertionrowkeys = insertionrowkeys_0.as('df1).select(concat_ws("",$"df1.CM_DEMO_MEME_CK", lit("_"), from_unixtime(unix_timestamp($"df1.CM_ELIG_EFF_DT", "yyyyMMdd"), "yyyy-MM-dd")).alias("row_key"), $"df1.CM_DEMO_MEME_CK", from_unixtime(unix_timestamp($"df1.CM_ELIG_EFF_DT", "yyyyMMdd"), "yyyy-MM-dd").alias("CM_ELIG_EFF_DT"), from_unixtime(unix_timestamp($"df1.CM_ELIG_TERM_DT", "yyyyMMdd"), "yyyy-MM-dd").alias("CM_ELIG_TERM_DT"), $"df1.CM_CSPI_ID", $"df1.CM_SGSG_ID", $"df1.CM_CSCS_ID", $"df1.CM_MEPE_FI", $"df1.CM_PDPD_ID").filter($"df1.CM_ELIG_IND" === "Y")

          val hbasePuts = insertionrowkeys.rdd.map((row: Row) => {
            val put = new Put(Bytes.toBytes(row.getString(0)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("MEME_CK"), Bytes.toBytes(row.getString(1)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("MEPE_EFF_DT"), Bytes.toBytes(row.getString(2)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("MEPE_TERM_DT"), Bytes.toBytes(row.getString(3)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("CSPI_ID"), Bytes.toBytes(row.getString(4)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("SGSG_ID"), Bytes.toBytes(row.getString(5)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("CSCS_ID"), Bytes.toBytes(row.getString(6)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("MEPE_FI"), Bytes.toBytes(row.getString(7)))
            put.addColumn(Bytes.toBytes("EC"), Bytes.toBytes("PDPD_ID"), Bytes.toBytes(row.getString(8)))
            (new ImmutableBytesWritable(), put)
          })
          hbasePuts.saveAsNewAPIHadoopDataset(jobConf)
        }catch {
          case e: Exception => {
            Logger.log.info(" Exception at HBase Elig Table Insertion : " + e.getMessage)
            Logger.log.error("Error occured : " + e.getStackTrace.mkString("\n"))
          }
            throw e
        }
      }

      Logger.log.info(s"===> Hbase delete started <===")
      // Logger.log.info(s"===> MERGING TIMELINES Print Schema: ${membersdf.printSchema()} <===")
      hbaseDeleteExisting(membersdf, DC834OUTELIGDF, DC834OUTELIGTABLENAME)
      Logger.log.info(s"===> Hbase deleted  <===")
      Logger.log.info(s"===> Hbase insert started  <===")
      hbaseinsertrecords(membersdf, DC834OUTELIGDF, DC834OUTELIGTABLENAME)
      Logger.log.info(s"===> Hbase insert complted   <===")
    }
  }

}