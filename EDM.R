source(paste(getwd(), 'util', 'util_data_access_MSSQL.R', sep = '/'))

get_producer_info <- function(env, producercd = '') {
  sqlText <-
    sprintf(
      "SELECT * FROM CMS_PRODUCERCHANNEL_M WHERE PRODUCERCD='%s' OR PRODUCERSOURCECD='%s'",
      producercd,
      producercd
    )
  queryResults <- execute_sql(sqlText, env)
  queryResults
}

get_kpi_mapping_to_channel <- function(env, channelcd) {
  sqlText <-
    "SELECT * FROM CMS_PRODUCTIONUNIT_DEF WHERE PRODNUNITCD IN (
  SELECT RULECD
  FROM CMS_RULE_MAPPING
  WHERE CHANNELCD = '%s'
  )
  "
  queryResults <- execute_sql(sprintf(sqlText, channelcd), env)
  queryResults
}

get_all_agencies <- function(env) {
  sqlText <-
    "SELECT * FROM CMS_PRODUCERCHANNEL_M WHERE PRODUCERTYPECD='AGENCY'"
  queryResults <- execute_sql(sqlText, env)
  queryResults
}

get_incentive <- function(env, benefitdt) {
  sqlText <- "
  SELECT
  CPM.PRODUCERSOURCECD,
  CB.BENEFITRESULTSEQ,
  CBPR.BENEFITPAYRESULTSEQ,
  CB.BENEFITCD,
  CBD.BENEFITNAME1,
  CB.DESIGNATIONCD,
  CB.BENEFITEVALDT,
  CB.BENEFITAMT,
  CB.PRODUCTIONMONTH,
  CB.PRODUCTIONYEAR,
  CBD.NEXTCALCULATIONDT,
  CB.ADHOCFL
  FROM CMS_BENEFITRESULT CB INNER JOIN CMS_PRODUCERCHANNEL_M CPM ON CB.PRODUCERCD = CPM.PRODUCERCD
  INNER JOIN CMS_BENEFIT_DEF CBD ON CBD.BENEFITCD = CB.BENEFITCD
  LEFT JOIN CMS_BENEFITPAY_RESULT CBPR ON CB.BENEFITRESULTSEQ = CBPR.BENEFITRESULTSEQ
  WHERE CB.BENEFITDT = '%s'
  "
  queryResults <- execute_sql(sprintf(sqlText, benefitdt), env)
  queryResults
}

get_incentive_kpis <- function(env, benefitdt) {
  sqlText <- "
  SELECT
  CB.PRODUCERCD,
  CB.BENEFITCD,
  CBK.KPIBASISRESULTSEQ,
  CBK.BENEFITRESULTSEQ,
  CBK.KPICD,
  CBK.GROUPCD,
  CBK.KPIPERIODFROM,
  CBK.KPIPERIODTO,
  CBK.KPIVALUE
  FROM CMS_BENEFITRESULT_KPI CBK INNER JOIN CMS_BENEFITRESULT CB ON CBK.BENEFITRESULTSEQ = CB.BENEFITRESULTSEQ
  WHERE CB.BENEFITDT = '%s' AND CBK.KPIVALUE <> 0
  "
  queryResults <- execute_sql(sprintf(sqlText, benefitdt), env)
  queryResults
}
