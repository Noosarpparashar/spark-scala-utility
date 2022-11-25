package scala
import org.apache.spark
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

///home/prasoon/Downloads/dsw1/dsw/component
object asw extends App {
  val conf = new SparkConf()
    .setAppName("ReadParquet")
    .set("spark.driver.bindAddress", "127.0.0.1")
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "8g")


  val spark = SparkSession.builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  val file_location = "/FileStore/tables"
  val  file_type = "parquet"
  val infer_schema = "false"
  val first_row_is_header = "true"
  val delimiter = ","

  val quoteDF = spark.read.parquet("/home/prasoon/Downloads/dsw1/dsw/quote").limit(300000)
  val componentDF = spark.read.parquet("/home/prasoon/Downloads/dsw1/dsw/component").limit(800000)
    .withColumnRenamed("CTRYCODE","COUNTRY_CODE")
    .withColumn("QUOTE_ID",trim(col("QUOTE_ID")))
    .withColumn("COUNTRY_CODE",trim(col("COUNTRY_CODE")))
  val headerDF = spark.read.parquet("/home/prasoon/Downloads/dsw1/dsw/header").limit(400000)
    .withColumn("QUOTEID",trim(col("QUOTEID")))
    .withColumn("COUNTRY_CODE",trim(col("COUNTRY_CODE")))

  var newquoteDF = quoteDF
  for(col <- quoteDF.columns){
    newquoteDF = newquoteDF.withColumnRenamed(col,col.concat("_QUOTE"))
  }
  var newcomponentDF = componentDF
  for(col <- componentDF.columns){
    newcomponentDF = newcomponentDF.withColumnRenamed(col,col.concat("_COMPONENT"))
  }
  var newheaderDF = headerDF
  for(col <- headerDF.columns){
    newheaderDF = newheaderDF.withColumnRenamed(col,col.concat("_HEADER"))
  }
  //println(newquoteDF.columns.mkString(","))
  //  println(newcomponentDF.columns.mkString(","))
  //  println(newheaderDF.columns.mkString(","))
  val df =newquoteDF.join(newcomponentDF,newquoteDF("COUNTRY_CODE_QUOTE")===newcomponentDF("COUNTRY_CODE_COMPONENT") &&  newquoteDF("QUOTE_ID_QUOTE")===newcomponentDF("QUOTE_ID_COMPONENT"))
    .join(newheaderDF,newquoteDF("COUNTRY_CODE_QUOTE")===newheaderDF("COUNTRY_CODE_HEADER") &&  newquoteDF("QUOTE_ID_QUOTE")===newheaderDF("QUOTEID_HEADER"), "leftouter")
    .filter((col("QUOTE_CUSTOMER_CODE_HEADER")==='C' || col("QUOTE_CUSTOMER_CODE_HEADER").isNull)
      && (col("LIST_PRICE_LOCAL_COMPONENT")>0 )
      && (col("APPROVED_PRICE_LOCAL_COMPONENT") >= 0)
      && (col("TRANSFER_PRICE_LOCAL_COMPONENT") >= 0)
      && (col("QUANTITY_COMPONENT") > 0)
      //  && (col("TYPE_COMPONENT") ==="7120")
      && (col("UPGRADE_FLAG_COMPONENT") !="3" || col("UPGRADE_FLAG_COMPONENT") !="C" )
      && (( col("CREATOR_TYPE_QUOTE") === 'I' &&
      (col("TRANSACTION_STATUS_QUOTE").isin ('2','3') ||
        ( col("APPROVAL_CODE_QUOTE").isin( 'A', 'E' ) &&
          !col("TRANSACTION_STATUS_QUOTE").isin('4','5','6','7','9')) ||
        (col("APPROVAL_CODE_QUOTE")==='C' || col("COPRA_APPROVED_INDICATOR_QUOTE")==='Y')
        )) || ( col("CREATOR_TYPE_QUOTE")=== 'B' &&
      col("BP_QUOTE_STATUS_QUOTE").isin("27","30","31")))
      && (col("CATEGORY_COMPONENT") ==="S")
      && (col("SUB_CATEGORY_COMPONENT") ==="P" || col("SUB_CATEGORY_COMPONENT") ==="O")
      && !(col("LEGACY_QUOTE_INDICATOR_QUOTE") ==="N" && upper(col("USER_DESCRIPTION_QUOTE")).like("%TEST%"))


    )
  df.show()

  df.groupBy("COMPONENT_ID_COMPONENT","BRAND_CODE_COMPONENT","TYPE_COMPONENT","MODEL_COMPONENT","CATEGORY_COMPONENT","UPGRADE_FLAG_COMPONENT", "LIST_PRICE_LOCAL_COMPONENT",     "COUNTRY_CODE_QUOTE","QUOTE_ID_QUOTE","CHANNELIDCODE_QUOTE","APPROVAL_CODE_QUOTE","COPRA_APPROVED_INDICATOR_QUOTE","BP_QUOTE_STATUS_QUOTE","BP_REASON_CODE_QUOTE","CREATOR_TYPE_QUOTE",
    "TRANSCREATEDATE_SKEY_date_HEADER","TRANSSIGNEDDATE_SKEY_date_HEADER","APPROVALDATETIME_SKEY_date_HEADER","TRANSQUOTEDDATE_SKEY_date_HEADER","CMR_CUSTOMER_NUMBER_HEADER", "COVERAGETYPE_HEADER" ,"COVERAGEID_HEADER" , "COVERAGENAME_HEADER" , "ISUCODE_HEADER" , "REGIONSTATE_HEADER" , "ZIPCODE_HEADER"
  )
    .agg(
      min("APPROVAL_CODE_COMPONENT").as("APPROVALCODE"),
      min("TRANSACTION_STATUS_QUOTE").as("TRANSACTIONSTATUS"),
      min("CURRENCYCODE_QUOTE").as("CURRENCYCODE"),
      min("CURRENCY_CONVERSION_CODE_QUOTE").as("CONVERSIONCODE"),
      min("COST_CONVERSION_RATE_QUOTE").as("COSTCONVERSRATE"),
      min(lit("  ")).as("GEOCODE"),
      min(lit("     ")).as("GEODESC"),
      min(lit("  ")).as("REGIONCODE"),
      min(lit("  ")).as("REGIONDESC"),
      min(lit("  ")).as("COUNTRYDESC"),
      min(rtrim(col("COMPANYNAME1_HEADER")))as("CUSTOMER_NAME"),
      max("CITY_HEADER").as("CITY"),
      sum("QUANTITY_COMPONENT").as("QUANTITY"),
      sum(col("LIST_PRICE_LOCAL_COMPONENT")*col("QUANTITY_COMPONENT")).as("LIST_PRICE"),
      sum(col("REQUESTED_PRICE_LOCAL_COMPONENT")*col("QUANTITY_COMPONENT")).as("REQUESTED_PRICE"),
      sum(col("APPROVED_PRICE_LOCAL_COMPONENT")*col("QUANTITY_COMPONENT")).as("BID_REVENUE"),
      sum(col("TRANSFER_PRICE_LOCAL_COMPONENT")*col("QUANTITY_COMPONENT")).as("BMC"),
      sum(col("COMPONENT_COST_LOCAL_COMPONENT")*col("QUANTITY_COMPONENT")).as("TMC"),
      min(col("BELOW_DELEGATION_REASON_CODE_COMPONENT")).as("BELOWDELEGCODE")
    )
    .withColumn("IS0_2_CTRY_CODE",concat(substring(col("COUNTRY_CODE_QUOTE"), 1, 2),lit("")))
    .withColumn("TPRSS_BMDIV", when(col("BRAND_CODE_COMPONENT") === "9R","75")
      .otherwise(col("BRAND_CODE_COMPONENT")))
    .withColumn("TRANS_ID_PLUS",substring(concat(substring(col("COUNTRY_CODE_QUOTE"), 1, 2),
      rtrim(col("QUOTE_ID_QUOTE")),rtrim(col("COMPONENT_ID_COMPONENT")),substring(col("TPRSS_BMDIV"), 1, 2),
      rtrim(col("TYPE_COMPONENT")),rtrim(col("MODEL_COMPONENT")),rtrim(col("UPGRADE_FLAG_COMPONENT"))),1,35
    ))
    .withColumn("TRANS_ID",substring(concat(substring(col("COUNTRY_CODE_QUOTE"), 1, 2),rtrim(col("QUOTE_ID_QUOTE"))),1,19
    ) )

    .withColumn("PROD_TYPE",col("CATEGORY_COMPONENT"))
    .withColumn("PRODID",substring(concat(col("TYPE_COMPONENT"),col("MODEL_COMPONENT")),1,7))
    .withColumn("MACHINE_TYPE",col("TYPE_COMPONENT"))
    .withColumn("MACHINE_MODEL",col("MODEL_COMPONENT"))
    .withColumn("CHANNEL_ID", when(substring(col("CHANNELIDCODE_QUOTE"),1,1).isin(" ","0"),"U")
      .otherwise(concat(substring(col("CHANNELIDCODE_QUOTE"),1,1),lit(" "))))
    .withColumn("DIRECT_FL", when(substring(col("CHANNELIDCODE_QUOTE"),1,1).isin("A","B","C","D","E","N","O","P","Q","R","Y","z"),"Y")
      .otherwise("N"))
    .withColumn("TRANS_DT", when(((col("CREATOR_TYPE_QUOTE")==="B" && col("BP_QUOTE_STATUS_QUOTE")===27 && col("BP_REASON_CODE_QUOTE")==="273") ||
      (col("CREATOR_TYPE_QUOTE")==="I" &&(col("APPROVAL_CODE_QUOTE")==="C") || col("COPRA_APPROVED_INDICATOR_QUOTE")==="Y" )) &&
      col("APPROVALDATETIME_SKEY_date_HEADER") === "0001-01-01", col("TRANSCREATEDATE_SKEY_date_HEADER"))
      .otherwise(when(col("TRANSSIGNEDDATE_SKEY_date_HEADER") >  "0001-01-01", col("TRANSSIGNEDDATE_SKEY_date_HEADER"))
        .when(col("APPROVALDATETIME_SKEY_date_HEADER") >  "0001-01-01", col("APPROVALDATETIME_SKEY_date_HEADER"))
        .otherwise(col("TRANSQUOTEDDATE_SKEY_date_HEADER"))))

    .withColumn("CREATE_DATE",col("TRANSCREATEDATE_SKEY_date_HEADER"))
    .withColumn("QUOTE_DATE",col("TRANSQUOTEDDATE_SKEY_date_HEADER"))
    .withColumn("APPROVE_DATE",col("APPROVALDATETIME_SKEY_date_HEADER"))
    .withColumn("SIGNED_DATE",col("TRANSSIGNEDDATE_SKEY_date_HEADER"))
    .withColumn("CALYEAR", when(col("TRANSSIGNEDDATE_SKEY_date_HEADER") >"0001-01-01",col("TRANSSIGNEDDATE_SKEY_date_HEADER"))
      .when(col("APPROVALDATETIME_SKEY_date_HEADER") >"0001-01-01",col("APPROVALDATETIME_SKEY_date_HEADER"))
      .otherwise(col("TRANSQUOTEDDATE_SKEY_date_HEADER")))
    .withColumn("ROLLOUT_YEAR", when(col("TRANSSIGNEDDATE_SKEY_date_HEADER") >"0001-01-01",col("TRANSSIGNEDDATE_SKEY_date_HEADER"))
      .when(col("APPROVALDATETIME_SKEY_date_HEADER") >"0001-01-01",col("APPROVALDATETIME_SKEY_date_HEADER"))
      .otherwise(col("TRANSQUOTEDDATE_SKEY_date_HEADER")))
    .withColumn("CAL_QUARTER",when(((col("CREATOR_TYPE_QUOTE")==='B' && col("BP_QUOTE_STATUS_QUOTE")===27 && col("BP_REASON_CODE_QUOTE") ==="273") || (col("CREATOR_TYPE_QUOTE")==='I' && (col("APPROVAL_CODE_QUOTE")==='C'
      || col("COPRA_APPROVED_INDICATOR_QUOTE")==='Y'))) && col("APPROVALDATETIME_SKEY_date_HEADER")==="0001-01-01", col("TRANSCREATEDATE_SKEY_date_HEADER") )
      .otherwise(when(col("TRANSSIGNEDDATE_SKEY_date_HEADER") > "0001-01-01", col("TRANSSIGNEDDATE_SKEY_date_HEADER"))
        .when(col("APPROVALDATETIME_SKEY_date_HEADER") > "0001-01-01", col("APPROVALDATETIME_SKEY_date_HEADER"))
        .otherwise(col("TRANSQUOTEDDATE_SKEY_date_HEADER"))
      ))
    .withColumn("ROLLOUT_QUARTER",when(((col("CREATOR_TYPE_QUOTE")==='B' && col("BP_QUOTE_STATUS_QUOTE")===27 && col("BP_REASON_CODE_QUOTE") ==="273") || (col("CREATOR_TYPE_QUOTE")==='I' && (col("APPROVAL_CODE_QUOTE")==='C'
      || col("COPRA_APPROVED_INDICATOR_QUOTE")==='Y'))) && col("APPROVALDATETIME_SKEY_date_HEADER")==="0001-01-01", col("TRANSCREATEDATE_SKEY_date_HEADER") )
      .otherwise(when(col("TRANSSIGNEDDATE_SKEY_date_HEADER") > "0001-01-01", col("TRANSSIGNEDDATE_SKEY_date_HEADER"))
        .when(col("APPROVALDATETIME_SKEY_date_HEADER") > "0001-01-01", col("APPROVALDATETIME_SKEY_date_HEADER"))
        .otherwise(col("TRANSQUOTEDDATE_SKEY_date_HEADER"))
      ))
    .withColumn("CUSTOMER_NUM",col("CMR_CUSTOMER_NUMBER_HEADER"))
    .withColumn("COVERAGE_TYPE",col("COVERAGETYPE_HEADER"))
    .withColumn("COVERAGE_ID", col("COVERAGEID_HEADER"))
    .withColumn("COVERAGE_NAME", col("COVERAGENAME_HEADER"))
    .withColumn("ISU", substring(col("ISUCODE_HEADER"),1,3))
    .withColumn("REGIONSTATE", when(col("COUNTRY_CODE_QUOTE").isin("US","CA"),rtrim(col("REGIONSTATE_HEADER")) ).otherwise(""))
    .withColumn("ZIPCODE", when(col("COUNTRY_CODE_QUOTE").isin("US","CA"),rtrim(col("ZIPCODE_HEADER")) ).otherwise(""))
    .withColumn("LIST_PRICE",when(col("LIST_PRICE_LOCAL_COMPONENT")===999999999,0).otherwise("LIST_PRICE"))
    .withColumn("DISCOUNT", when(col("LIST_PRICE_LOCAL_COMPONENT")===999999999,col("BID_REVENUE")).otherwise(col("BID_REVENUE")-col("LIST_PRICE")))
    .withColumn("BMC_GP",col("BID_REVENUE")-col("BMC"))
    .withColumn("TMC_GP",col("BID_REVENUE")-col("TMC"))
    .withColumn("MES_FL",when(col("UPGRADE_FLAG_COMPONENT")==='4', 'M').when(col("UPGRADE_FLAG_COMPONENT")==='5','U').otherwise('N'))
    .withColumn("BM_RECORD_FL", lit('N'))
    .withColumn("DELEG_FL", lit('P'))
    .select("BELOWDELEGCODE","APPROVALCODE", "TRANSACTIONSTATUS", "CURRENCYCODE", "CONVERSIONCODE", "COSTCONVERSRATE",
      "GEOCODE", "GEODESC", "REGIONCODE", "REGIONDESC", "IS0_2_CTRY_CODE", "COUNTRYDESC", "TRANS_ID_PLUS",
      "TRANS_ID", "TPRSS_BMDIV", "PROD_TYPE", "PRODID", "MACHINE_TYPE", "MACHINE_MODEL", "CHANNEL_ID", "DIRECT_FL",
      "TRANS_DT", "CREATE_DATE", "QUOTE_DATE", "APPROVE_DATE", "SIGNED_DATE", "CALYEAR", "CAL_QUARTER", "ROLLOUT_YEAR",
      "ROLLOUT_QUARTER", "CUSTOMER_NUM", "CUSTOMER_NAME", "COVERAGE_TYPE", "COVERAGE_ID", "COVERAGE_NAME",
      "ISU", "CITY", "REGIONSTATE", "ZIPCODE",  "QUANTITY", "LIST_PRICE", "REQUESTED_PRICE",
      "DISCOUNT", "BID_REVENUE", "BMC", "TMC", "BMC_GP", "TMC_GP", "MES_FL", "BM_RECORD_FL", "DELEG_FL"
    )
    .show()



}
