library(dplyr)
library(RSQLite)
library(xlsx)
library (lubridate)
library(zoo)
library(tibble)
library(readxl)
library(tidyr)
library(openxlsx)
library(RODBC)
library(RDCOMClient)
options(java.parameters = "-Xmx5120m") 

insert_or_replaceall <-
  function(datadf, tbname, dbfile = 'KPI_PRODUCTION/main_database.db') {
    # create a new table and then insert new data to it
    # or delete all data from the table and then insert new data to it
    my_database <- src_sqlite(dbfile, create = TRUE)
    
    if (!(tbname %in% dbListTables(my_database$con))) {
      copy_to(my_database,
              datadf,
              name = tbname,
              temporary = FALSE)
    } else {
      results <-
        dbSendQuery(my_database$con, sprintf("delete FROM %s", tbname))
      dbClearResult(results)
      db_insert_into(con = my_database$con,
                     table = tbname,
                     values = datadf)
    }
  }

insert_or_replace <-
  function(df,
           tbname,
           dbfile = 'KPI_PRODUCTION/main_database.db') {
    if (nrow(df)){
      my_database <- src_sqlite(dbfile, create = TRUE)
      sql = "insert or replace into %s (%s) values ('%s');"
      for (i in 1:nrow(df)) {
        results <- dbSendQuery(my_database$con, 
                               sprintf(sql, 
                                       tbname, 
                                       paste(names(df), collapse = ","),
                                       paste(gsub("'", "''", df[i,]), collapse="','")
                               )
                               )
        dbClearResult(results)
      }
    }
  }

insert_or_replace_bulk <-
  function(df,
           tbname,
           dbfile = 'KPI_PRODUCTION/main_database.db',
           bulksize = 100) {
    if (nrow(df)){
      my_database <- src_sqlite(dbfile, create = TRUE)
      sql = "insert or replace into %s (%s) values %s;"
      
      i = 1
      if (nrow(df) < bulksize) {
        bulksize = nrow(df)
      }
      for (j in seq(bulksize,
                    nrow(df),
                    bulksize)) {
        values = paste (sprintf(
          "('%s')",
          apply(df[i:j,], 1,
                function(row) {
                  # print(gsub("^\\s+|\\s+$", "", row))
                  paste(gsub("'", "''", row), collapse = "','")
                  # # trim leading and trailing whitespace (left trim, right trim)
                  # paste(gsub("'", "''", gsub("^\\s+|\\s+$", "", row)), collapse = "','")
                })
        ),
        collapse = ",")
        
        result = tryCatch({
          results <- dbSendQuery(my_database$con,
                                 sprintf(sql,
                                         tbname,
                                         paste(names(df), collapse = ","),
                                         values
                                 )
          )
          dbClearResult(results)
        }, warning = function(w) {
          # warning-handler-code
        }, error = function(e) {
          # error-handler-code
          print(result)
          # print(sprintf(sql, 
          #               tbname, 
          #               paste(names(df), collapse = ","),
          #               values
          # ))
        }, finally = {
          # cleanup-code
        })
        
        
       
        
        i = j
      }
      
      if (i<nrow(df)) {
        values = paste (sprintf(
          "('%s')",
          apply(df[i:nrow(df),], 1,
                function(row) {
                  paste(gsub("'", "''", row), collapse = "','")
                  # paste(row, collapse = "','")
                })
        ),
        collapse = ",")
        
        result = tryCatch({
          results <- dbSendQuery(my_database$con, 
                                 sprintf(sql, 
                                         tbname, 
                                         paste(names(df), collapse = ","),
                                         values
                                 )
          )
          dbClearResult(results)
        }, warning = function(w) {
          # warning-handler-code
        }, error = function(e) {
          # error-handler-code
          message ("INSERT ERROR")
          message(e)
          print(sprintf(sql,
                        tbname,
                        paste(names(df), collapse = ","),
                        values
          ))
        }, finally = {
          # cleanup-code
        })
        
       
        
      }
    }
  }
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# ACTIVE ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



convert_data_to_statkpis_format <- function(kpi_month, kpi_name, kpi_grp, lev, viewby){
  if (nrow(kpi_month)) {
    kpi_month = dplyr::ungroup(kpi_month) 
    names(kpi_month) <- c('NAME', 'BUSINESSDATE', 'VALUE')
    kpi_month <- dplyr::mutate(kpi_month, KPI=kpi_name)
    kpi_month <- dplyr::mutate(kpi_month, LEVEL=lev)
    kpi_month <- dplyr::mutate(kpi_month, KPIGROUP=kpi_grp)
    kpi_month <- dplyr::mutate(kpi_month, VIEWBY=viewby)
  }
  kpi_month
}

rookies_agents <- function(agents) {
  Rookie_in_month <<- dplyr::select(dplyr::filter(agents, MDIFF==0),-c(REGIONCD,TERRITORY))
  Rookie_last_month <<- dplyr::select(dplyr::filter(agents, MDIFF==1),-c(REGIONCD,TERRITORY))
  Rookie_2_3_months <<- dplyr::select(dplyr::filter(agents, (MDIFF==2) | (MDIFF==3)),-c(REGIONCD,TERRITORY)) 
  Rookie_4_6_months <<- dplyr::select(dplyr::filter(agents, MDIFF %in% (4:6)),-c(REGIONCD,TERRITORY)) 
  Rookie_7_12_months <<- dplyr::select(dplyr::filter(agents, MDIFF %in% (7:12)),-c(REGIONCD,TERRITORY))
  Rookie_13_and_more_months <<- dplyr::select(dplyr::filter(agents, MDIFF >= 13),-c(REGIONCD,TERRITORY))
}

extract_kpi <- function(kpi, kpigroup, businessdates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = "SELECT NAME AS TERRITORY, 
  BUSINESSDATE AS BUSSINESSDATE, 
  VALUE AS `%s`
  FROM STAT_KPIS 
  WHERE KPI='%s' AND KPIGROUP='%s' AND BUSINESSDATE IN ('%s')"
  result <- sprintf(sql, kpi, kpi, kpigroup, paste(businessdates, collapse = "','")) %>% 
    dbSendQuery(my_database$con, .)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data
}

get_active_sa <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT AGENT_CODE, REGIONCD, B.TERRITORY, BUSSINESSDATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE BUSSINESSDATE IN ('%s') 
    AND ACTIVE='Yes'
    AND SERVICING_AGENT='Yes' and ACTIVESP='Yes'", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_active_excluded_SA <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A.ACTIVE='Yes'
    AND A.ACTIVESP='Yes'
    AND A1.AGENT_STATUS IN ('Active','Suspended')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    ", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}
get_active <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.AGENT_DESIGNATION 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A.ACTIVE='Yes'
    AND A.ACTIVESP='Yes'
    AND A1.AGENT_STATUS IN ('Active','Suspended')
    ", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_ape_sa <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT  B.TERRITORY,
        AGCODE,
        APE,
        substr(RK.BUSSINESSDATE,1,4) ||'-'|| substr(RK.BUSSINESSDATE,5,2) ||'-'|| substr(RK.BUSSINESSDATE,7,2) as BUSSINESSDATE
      FROM RAWDATA_KPITotal RK
      JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON RK.REGIONCD = B.REGION_CODE
      WHERE RK.BUSSINESSDATE LIKE '%s'
      AND RK.AGCODE IN (
        SELECT AGENT_CODE
        FROM RAWDATA_MANPOWER_ACTIVERATIO A
        WHERE A.BUSSINESSDATE='%s'
        AND A.SERVICING_AGENT='Yes'
      )"
  
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%"), d))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data
  })
  )
  
  dbDisconnect(my_database$con)
  APE
}

get_ape_rookie_in_month_exl_mdrt <- function(last_day_of_months, m=c(0:0), dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT  B.TERRITORY AS TERRITORY,
  AGCODE,
  APE
  FROM RAWDATA_KPITotal RK
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON RK.REGIONCD = B.REGION_CODE
  WHERE RK.BUSSINESSDATE LIKE '%s' AND APE != 0
  "
  
  SQL1 <- "
    SELECT A.AGENT_CODE, A.JOINING_DATE, A.BUSSINESSDATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    WHERE A.BUSSINESSDATE in ('%s') 
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_CODE NOT IN (SELECT AgCode FROM RAWDATA_MDRT WHERE EffDateFrom<= A.BUSSINESSDATE AND A.BUSSINESSDATE<=EffDateTo)
  "
  result <- dbSendQuery(my_database$con, sprintf(SQL1, paste(last_day_of_months, collapse = "','")))
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  rookie <- 
    data %>%
    convert_string_to_date('JOINING_DATE', FORMAT_BUSSINESSDATE) %>%
    convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%
    # calculate different months between joining date and business date
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12)))
  
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    rookie_in_month <- rookie %>% filter(BUSSINESSDATE==d, MDIFF %in% m)
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data %>% 
      merge(x=.,y=rookie_in_month, by.x='AGCODE', by.y='AGENT_CODE')
  })
  )
  
  dbDisconnect(my_database$con)
  APE 
}

get_ape_rookie_in_month_exl_genlion <- function(last_day_of_months, m=c(0:0), dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT  B.TERRITORY AS TERRITORY,
  AGCODE,
  APE
  FROM RAWDATA_KPITotal RK
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON RK.REGIONCD = B.REGION_CODE
  WHERE RK.BUSSINESSDATE LIKE '%s' AND APE != 0
  "
  
  SQL1 <- "
    SELECT A.AGENT_CODE, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  WHERE A.BUSSINESSDATE in ('%s') 
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  "
  result <- dbSendQuery(my_database$con, sprintf(SQL1, paste(last_day_of_months, collapse = "','")))
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  rookie <- 
    data %>%
    convert_string_to_date('JOINING_DATE', FORMAT_BUSSINESSDATE) %>%
    convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%
    # calculate different months between joining date and business date
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12)))
  
  actGenlion = get_active_genlion(last_day_of_months, dbfile)
  
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    rookie_in_month <- rookie %>% filter(BUSSINESSDATE==d, MDIFF %in% m) 
    rookie_in_month <- rookie_in_month[!rookie_in_month$AGENT_CODE %in% as.vector(dplyr::filter(actGenlion, BUSSINESSDATE==d)$AGENT_CODE),]
    
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data %>% 
      merge(x=.,y=rookie_in_month, by.x='AGCODE', by.y='AGENT_CODE')
    # return agents who is not gen lion club member  
    # data[!data$AGCODE %in% as.vector(dplyr::filter(actGenlion, BUSSINESSDATE==d)$AGENT_CODE),]
  })
  )
  
  dbDisconnect(my_database$con)
  APE 
}

get_ape <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT
  AGCODE,
  APE
  FROM RAWDATA_KPITotal RK
  WHERE RK.BUSSINESSDATE LIKE '%s' AND APE != 0
  "
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE %>% dplyr::group_by(AGCODE,BUSSINESSDATE) %>%  summarise(APE = sum(APE))
}

get_ape_sum_by_office <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT OFFICE, SUM(APE) AS APE FROM RAWDATA_KPITotal WHERE BUSSINESSDATE LIKE '%s' GROUP BY OFFICE"
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE 
}

get_ape_sum_by_office_designation <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT OFFICE, AGCODEDESIGNATION, SUM(APE) AS APE FROM RAWDATA_KPITotal WHERE BUSSINESSDATE LIKE '%s' GROUP BY OFFICE, AGCODEDESIGNATION"
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE 
}

get_rider_ape_sum_by_office <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- "
  SELECT OFFICE, SUM(APE) AS APE FROM (
    SELECT OFFICE, APE, PRODUCTCODE FROM RAWDATA_KPITotal 
    WHERE BUSSINESSDATE LIKE '%s' 
  ) KPI
  JOIN RAWDATA_Product PR ON KPI.PRODUCTCODE = PR.PRUDUCTCODE
  WHERE BASEPRODUCT='No'
  GROUP BY OFFICE
  "
  # apply-like function that returns a data frame
  APE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE 
}

get_main_case_sum_by_office <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- "
  SELECT OFFICE, SUM(`CASE`) AS CASECOUNT FROM (
    SELECT OFFICE, `CASE`, PRODUCTCODE FROM RAWDATA_KPITotal 
    WHERE BUSSINESSDATE LIKE '%s' 
  ) KPI
  JOIN RAWDATA_Product PR ON KPI.PRODUCTCODE = PR.PRUDUCTCODE
  WHERE BASEPRODUCT='Yes'
  GROUP BY OFFICE
  "
  # apply-like function that returns a data frame
  CASE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  CASE 
}

get_rider_case_sum_by_office <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- "
  SELECT OFFICE, SUM(`CASE`) AS CASECOUNT FROM (
    SELECT OFFICE, `CASE`, PRODUCTCODE FROM RAWDATA_KPITotal 
    WHERE BUSSINESSDATE LIKE '%s' 
  ) KPI
  JOIN RAWDATA_Product PR ON KPI.PRODUCTCODE = PR.PRUDUCTCODE
  WHERE BASEPRODUCT='No'
  GROUP BY OFFICE
  "
  # apply-like function that returns a data frame
  CASE <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  CASE 
}

get_ryp <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile)
  SQL <- 
    "SELECT
  AGCODE,
  RYP
  FROM RAWDATA_KPITotal RK
  WHERE RK.BUSSINESSDATE LIKE '%s' AND RYP != 0
  "
  # apply-like function that returns a data frame
  returndf <- do.call(rbind,lapply(last_day_of_months, function(d) {
    result <- dbSendQuery(my_database$con, sprintf(SQL, strftime(d,"%Y%m%")))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data = mutate(data, BUSSINESSDATE=strftime(d,"%Y-%m-%d"))
    data
  })
  )
  dbDisconnect(my_database$con)
  returndf %>% dplyr::group_by(AGCODE,BUSSINESSDATE) %>%  summarise(RYP = sum(RYP))
}

get_active_genlion <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, 'YES' AS GENLION 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
  WHERE A.BUSSINESSDATE='%s'
  AND A.ACTIVE='Yes'
  AND A.ACTIVESP='Yes'
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_CODE IN (
  SELECT AGENTCD FROM RAWDATA_Genlion_Report WHERE FINALDATE='%s' AND (XEPHANG IS NOT NULL AND XEPHANG != '')
  )
  "
  
  # apply-like function that returns a data frame
  Active_GenLion <- do.call(rbind,lapply(last_day_of_months, function(d) {
    # last date in previous quarter
    ldpq = zoo::as.Date(zoo::as.yearqtr(d)) - 1
    result <- dbSendQuery(my_database$con, sprintf(SQL, d, ldpq))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data
  })
  )
  dbDisconnect(my_database$con)
  Active_GenLion
}

get_active_MDRT <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, 'YES' AS MDRT 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A.ACTIVE='Yes'
    AND A.ACTIVESP='Yes'
    AND A1.AGENT_STATUS IN ('Active','Suspended')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_CODE IN (SELECT AgCode FROM RAWDATA_MDRT WHERE EffDateFrom<= A.BUSSINESSDATE AND A.BUSSINESSDATE<=EffDateTo)", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  Active_MDRT <- data
  dbDisconnect(my_database$con)
  Active_MDRT
}

# Manpower ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


get_Manpower_MDRT <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, 'YES' AS MDRT 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_CODE IN (SELECT AgCode FROM RAWDATA_MDRT WHERE EffDateFrom<= A.BUSSINESSDATE AND A.BUSSINESSDATE<=EffDateTo)", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  MDRT <- data
  dbDisconnect(my_database$con)
  MDRT
}

get_Manpower_genlion <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, 'YES' AS GENLION 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
  WHERE A.BUSSINESSDATE='%s'
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_CODE IN (
  SELECT AGENTCD FROM RAWDATA_Genlion_Report WHERE FINALDATE='%s' AND (XEPHANG IS NOT NULL AND XEPHANG != '')
  )
  "
  # apply-like function that returns a data frame
  Active_GenLion <- do.call(rbind,lapply(last_day_of_months, function(d) {
    # last date in previous quarter
    ldpq = zoo::as.Date(zoo::as.yearqtr(d)) - 1
    result <- dbSendQuery(my_database$con, sprintf(SQL, d, ldpq))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data
  })
  )
  dbDisconnect(my_database$con)
  Active_GenLion
}

get_Manpower_excluded_SA <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    ", 
    paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}
  
get_Manpower_sa <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.BUSSINESSDATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A.SERVICING_AGENT='Yes' 
    AND A1.AGENT_STATUS IN ('Active','Suspended')
    ", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.SUPERVISOR_CODE, A.SUPERVISOR_CODE_DESIGNATION, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE, A.AGENT_DESIGNATION  
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended', '%s')
    ", 
    paste(last_day_of_months, collapse = "','"),  ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

# this funtion is not following to business rule from c.Diem (it follows to anh Hung's rule)
get_Manpower_AG <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  AG <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_DESIGNATION='AG'
  ", paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, AG)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower_AG_v1.1 <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  AG <- sprintf(
    "SELECT distinct A1.AGENTCD as AGENT_CODE, A1.REGIONCD, B.TERRITORY, A1.JOINING_DATE, A1.BUSSINESSDATE, A1.REINSTATEMENT_DATE 
  FROM GVL_AGENTLIST A1 
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A1.REGIONCD = B.REGION_CODE
  WHERE A1.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
  AND A1.AGENT_DESIGNATION='AG'
  ", paste(last_day_of_months, collapse = "','"), 'Terminated')
  result <- dbSendQuery(my_database$con, AG)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower_US <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_DESIGNATION='US'
    ", paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower_UM <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_DESIGNATION='UM'
    AND (A1.STAFFCD NOT IN ('SUM') OR A1.STAFFCD IS NULL)
    ", paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower_SUM <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_DESIGNATION='UM'
    AND A1.STAFFCD IN ('SUM')
    ", paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower_BM <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_DESIGNATION='BM'
    AND (A1.STAFFCD NOT IN ('SBM') OR A1.STAFFCD IS NULL)
    ", paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

get_Manpower_SBM <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.REINSTATEMENT_DATE 
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
    AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
    AND A.AGENT_DESIGNATION='BM'
    AND A1.STAFFCD IN ('SBM')
    ", paste(last_day_of_months, collapse = "','"), ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}


get_new_recruited_ag_in_month <- function(last_day_of_month, dbfile = 'KPI_PRODUCTION/main_database.db'){
  new_recruited_ag <- get_Manpower_AG_v1.1(last_day_of_month, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, FORMAT_BUSSINESSDATE))) %>% 
    # calculate different months between joining date and business date
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    dplyr::filter(MDIFF == 0) # new recuit
  #   group_by(BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% 
  #   summarise('new_recruited_ag_in_month' = n())
  # new_recruited_ag$new_recruited_ag_in_month[1]
  new_recruited_ag
}

get_Rookie_Metric_by_Recruited_month <- function(last_day_of_month, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(last_day_of_month, "Date")) {
    message ("last_day_of_month should be a Date type")
    stop()
  }
  
  y <- lubridate::year(last_day_of_month)
  m <- lubridate::month(last_day_of_month)
 
  m0 <- get_last_day_of_m0(y, m)
  m1 <- get_last_day_of_m1(y, m)
  m2 <- get_last_day_of_m2(y, m)
  
  new_recruited_ag_m0 <- get_new_recruited_ag_in_month(m0)
  new_recruited_ag_m1 <- get_new_recruited_ag_in_month(m1)
  new_recruited_ag_m2 <- get_new_recruited_ag_in_month(m2)
  
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT 
    A.AGENT_CODE, A.SUPERVISOR_CODE, 
    A.SUPERVISOR_CODE_DESIGNATION, 
    A.REGIONCD, B.TERRITORY, 
    A.JOINING_DATE, A.BUSSINESSDATE, 
    A.REINSTATEMENT_DATE, A.AGENT_DESIGNATION,
    A.ACTIVE,
    A.ACTIVESP
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended', 'Terminated')
    ", 
    last_day_of_month )
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  data <- data %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, FORMAT_BUSSINESSDATE))) %>% 
    # calculate different months between joining date and business date
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    dplyr::mutate(AGING=BUSSINESSDATE-JOINING_DATE)
  
  # 
  # lay danh sach tuyen dung o mot month nao do trong qua khu
  # join voi danh sach dang active o thoi diem dang xet
  # de thay duoc rang so tuyen dung trong qua khu
  # con active bao nhieu tai thoi diem xet
  # 
  active_d1_d30_ag <- data %>%   
    # dplyr::filter(AGENT_DESIGNATION=='AG') %>% 
    # dplyr::filter(MDIFF==0) %>%
    # dplyr::filter(AGING %in% c(0:30)) %>% 
    dplyr::filter(ACTIVESP == 'Yes') %>% 
    merge(x=., y=select(new_recruited_ag_m0, AGENT_CODE), by.x=c('AGENT_CODE'), by.y=c('AGENT_CODE')) %>% 
    group_by(BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    summarise('active_d1_d30_ag' = n()) 
    
  active_d31_d60_ag <- data %>%   
    # dplyr::filter(AGENT_DESIGNATION=='AG') %>% 
    # dplyr::filter(MDIFF==1) %>%
    # dplyr::filter(AGING %in% c(31:60)) %>% 
    dplyr::filter(ACTIVESP == 'Yes') %>% 
    merge(x=., y=select(new_recruited_ag_m1, AGENT_CODE), by.x=c('AGENT_CODE'), by.y=c('AGENT_CODE')) %>% 
    group_by(BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    summarise('active_d31_d60_ag' = n())
  
  active_d61_d90_ag <- data %>%   
    # dplyr::filter(AGENT_DESIGNATION=='AG') %>% 
    # dplyr::filter(MDIFF==2) %>%
    # dplyr::filter(AGING %in% c(61:90)) %>% 
    dplyr::filter(ACTIVESP == 'Yes') %>% 
    merge(x=., y=select(new_recruited_ag_m2, AGENT_CODE), by.x=c('AGENT_CODE'), by.y=c('AGENT_CODE')) %>% 
    group_by(BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    summarise('active_d61_d90_ag' = n())
  
  active_d1_d30_ag <<- active_d1_d30_ag %>% 
    dplyr::mutate(recruited_ag_m0=nrow(new_recruited_ag_m0)) %>% 
    dplyr::mutate(`%actv d1-30`= active_d1_d30_ag/recruited_ag_m0)
  
  active_d31_d60_ag <<- active_d31_d60_ag %>% 
    dplyr::mutate(recruited_ag_m1=nrow(new_recruited_ag_m1)) %>% 
    dplyr::mutate(`%actv d31-60`= active_d31_d60_ag/recruited_ag_m1)
  
  active_d61_d90_ag <<- active_d61_d90_ag %>% 
    dplyr::mutate(recruited_ag_m2=nrow(new_recruited_ag_m2)) %>% 
    dplyr::mutate(`%actv d61-90`= active_d61_d90_ag/recruited_ag_m2)
  
  dbDisconnect(my_database$con)
}

get_kpitotal_policies_first_issued <- function(yyyy, mm, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # Tra ve cac records cua kpi total 
  # ghi nhan kpi cho cac hop dong lan dau phat hanh
  # các hợp đồng bán sản phẩm chính, không tính raider
  # hợp đồng có ip từ 8tr trở lên
  my_database <- src_sqlite(dbfile, create = TRUE)
  # qui dinh txncode=t642 va business date = issued date (hoissdte) la hop dong lan dau phat hanh
  SQL <- sprintf(
    "
    SELECT * FROM RAWDATA_KPITotal T1
    JOIN RAWDATA_Product T2 ON T1.PRODUCTCODE=T2.PRUDUCTCODE
    WHERE BUSSINESSDATE LIKE '%s%s%s' 
    AND TXNCODE='T642' 
    AND IP>=8000000 
    AND BUSSINESSDATE=HOISSDTE
    AND UPPER(T2.BASEPRODUCT)='YES'
    ", yyyy, mm, '%')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y%m%d'))) %>% 
    dplyr::mutate(HOISSDTE=as.Date(strptime(HOISSDTE, '%Y%m%d'))) %>% 
    dplyr::select(AGCODE, HOISSDTE, CASE)
}

get_Rookie_Metric_by_Recruited_month_v2 <- function(last_day_of_month, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(last_day_of_month, "Date")) {
    message ("last_day_of_month should be a Date type")
    stop()
  }
  ###
  # last_day_of_month=as.Date('2017-07-31')
  # dbfile = 'KPI_PRODUCTION/main_database.db'
  ###
  y <- lubridate::year(last_day_of_month)
  m <- lubridate::month(last_day_of_month)
  
  m0 <- get_last_day_of_m0(y, m)
  m1 <- get_last_day_of_m1(y, m)
  m2 <- get_last_day_of_m2(y, m)
  m3 <- get_last_day_of_m3(y, m)
  m4 <- get_last_day_of_m4(y, m)
  m5 <- get_last_day_of_m5(y, m)
  m6 <- get_last_day_of_m6(y, m)
  
  new_recruited_ag_m0 <- get_new_recruited_ag_in_month(m0) 
  new_recruited_ag_m1 <- get_new_recruited_ag_in_month(m1) 
  new_recruited_ag_m2 <- get_new_recruited_ag_in_month(m2) 
  new_recruited_ag_m3 <- get_new_recruited_ag_in_month(m3) 
  new_recruited_ag_m4 <- get_new_recruited_ag_in_month(m4) 
  new_recruited_ag_m5 <- get_new_recruited_ag_in_month(m5) 
  new_recruited_ag_m6 <- get_new_recruited_ag_in_month(m6) 
  new_recruited_ag_m0_m6 <- rbind(
    new_recruited_ag_m0,
    new_recruited_ag_m1,
    new_recruited_ag_m2,
    new_recruited_ag_m3,
    new_recruited_ag_m4,
    new_recruited_ag_m5,
    new_recruited_ag_m6
  )
  
  recruite_by_month <-  new_recruited_ag_m0_m6 %>% 
    dplyr::group_by(recruit_month=strftime(JOINING_DATE, '%Y%m')) %>% 
    dplyr::summarise(new_recruited_agents=n())
  
  # các hđ phát hành lần đầu trong tháng m0 đến m6 (ip > 8tr, không tính raider) 
  kpitotal_case <- get_kpitotal_policies_first_issued('%', '%')
    
  # kpitotal_case <- rbind(
  #   get_kpitotal_policies_first_issued(strftime(m0,'%Y'), strftime(m0,'%m')),
  #   get_kpitotal_policies_first_issued(strftime(m1,'%Y'), strftime(m1,'%m')),
  #   get_kpitotal_policies_first_issued(strftime(m2,'%Y'), strftime(m2,'%m')),
  #   get_kpitotal_policies_first_issued(strftime(m3,'%Y'), strftime(m3,'%m')),
  #   get_kpitotal_policies_first_issued(strftime(m4,'%Y'), strftime(m4,'%m')),
  #   get_kpitotal_policies_first_issued(strftime(m5,'%Y'), strftime(m5,'%m')),
  #   get_kpitotal_policies_first_issued(strftime(m6,'%Y'), strftime(m6,'%m'))
  # )
  
  
  # recruite_by_month <- data %>% 
  #   dplyr::group_by(recruit_month = strftime(JOINING_DATE, '%Y%m')) %>% 
  #   summarise(new_recruited_agents = n())
    
  casecount_summary <- new_recruited_ag_m0_m6 %>% 
    merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
    # tư vấn có hợp đồng vào ngày thứ x sau ngày joined in date 
    dplyr::mutate(HAS_POLICY_ISSUED_IN_DAYS=HOISSDTE-JOINING_DATE) %>% 
    dplyr::mutate(has_policy_d1_d30_ag=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:30), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d31_d60_ag=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(31:60), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d61_d90_ag=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(61:90), CASE, 0)) %>% 
    dplyr::group_by(recruit_month = strftime(JOINING_DATE, '%Y%m'), AGENT_CODE) %>% 
    # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
    dplyr::summarise(
      has_policy_d1_d30_ag= sum( has_policy_d1_d30_ag),
      has_policy_d31_d60_ag=sum(has_policy_d31_d60_ag),
      has_policy_d61_d90_ag=sum(has_policy_d61_d90_ag)
      ) %>% 
    dplyr::ungroup(.) %>% 
    # tư vấn được tính là active trong d1-d30 nếu có ít nhất 1hđ trong d1-d30
    dplyr::mutate(active_d1_d30_ag = ifelse(has_policy_d1_d30_ag >= 1, 1, 0)) %>% 
    # tư vấn được tính là active trong d31-d60 nếu có ít nhất 1hđ trong d31-d60
    dplyr::mutate(active_d31_d60_ag = ifelse(has_policy_d31_d60_ag >= 1, 1, 0)) %>% 
    # tư vấn được tính là active trong d61-d90 nếu có ít nhất 1hđ trong d61-d90
    dplyr::mutate(active_d61_d90_ag = ifelse(has_policy_d61_d90_ag >= 1, 1, 0)) %>% 
    dplyr::group_by(recruit_month) %>% 
    # tính xem tổng cộng bao nhiêu tư vấn active trong d1-d30, d31-d60, d61-d90
    dplyr::summarise(
      active_d1_d30_ag=sum( active_d1_d30_ag),
      active_d31_d60_ag=sum(active_d31_d60_ag),
      active_d61_d90_ag=sum(active_d61_d90_ag)
    )
  
  
  rookie_mertric <- merge(
    x=recruite_by_month,
    y=casecount_summary,
    by.x = c('recruit_month'),
    by.y = c('recruit_month'),
    all.x = T
  ) %>% 
    dplyr::mutate(`%actv d1-30`= active_d1_d30_ag/new_recruited_agents) %>% 
    dplyr::mutate(`%actv d31-60`= active_d31_d60_ag/new_recruited_agents) %>% 
    dplyr::mutate(`%actv d61-90`= active_d61_d90_ag/new_recruited_agents)
  rookie_mertric
}


# MOVEMENT ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
promotion_movement_SUM_SBM <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = as.Date('2017-07-31')
  if(!inherits(business_dates, "Date")) {
    message ("the param should be a Date type")
    stop()
  }
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT AGENTCD, NEW_DESIGNATION, EFFECTIVE_FROM FROM RAWDATA_Promotion_Demotion_SBM_SUM
    WHERE 
    EFFECTIVE_FROM LIKE '%s'
    AND (
      (OLD_DESIGNATION = 'UM' AND NEW_DESIGNATION = 'SUM')
        OR 
      (OLD_DESIGNATION = 'BM' AND NEW_DESIGNATION = 'SBM')
    )
    ", 
    strftime(business_dates, '%Y-%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  
  get_Manpower(business_dates, dbfile) %>% 
    dplyr::select(AGENT_CODE, TERRITORY) %>% 
    merge(x=., y=data, by.x='AGENT_CODE', by.y='AGENTCD')
}
demotion_movement_SUM_SBM <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = as.Date('2017-07-31')
  if(!inherits(business_dates, "Date")) {
    message ("the param should be a Date type")
    stop()
  }
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT AGENTCD, NEW_DESIGNATION, EFFECTIVE_FROM FROM RAWDATA_Promotion_Demotion_SBM_SUM
    WHERE 
    EFFECTIVE_FROM LIKE '%s'
    AND (
      (OLD_DESIGNATION = 'SUM' AND NEW_DESIGNATION = 'UM')
        OR 
      (OLD_DESIGNATION = 'SBM' AND NEW_DESIGNATION = 'BM')
    )
    ", 
    strftime(business_dates, '%Y-%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  
  get_Manpower(business_dates, dbfile) %>% 
    dplyr::select(AGENT_CODE, TERRITORY) %>% 
    merge(x=., y=data, by.x='AGENT_CODE', by.y='AGENTCD')
}

promotion_movement_US_UM_BM <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = as.Date('2017-07-31')
  if(!inherits(business_dates, "Date")) {
    message ("the param should be a Date type")
    stop()
  }
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.NEW_AGENT_DESIGNATION, B.TERRITORY, A.EFFECTIVE_FROM 
    FROM RAWDATA_AGENTMOVEMENT A
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.OLD_REGION_CODE = B.REGION_CODE 
    WHERE 
    EFFECTIVE_FROM LIKE '%s'
    AND UPPER(MOVEMENT_TYPE) = 'PROMOTION'
    AND OLD_AGENT_DESIGNATION IN ('AG','US','UM')
    AND NEW_AGENT_DESIGNATION IN ('US', 'UM', 'BM')
    AND MOVEMENT_REASON NOT IN ('Tuy?n d?ng tr?c ti?p', 'Tuyển dụng trực tiếp')
    ", 
    strftime(business_dates, '%Y-%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}
demotion_movement_US_UM_BM <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = as.Date('2017-07-31')
  if(!inherits(business_dates, "Date")) {
    message ("the param should be a Date type")
    stop()
  }
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.NEW_AGENT_DESIGNATION, B.TERRITORY, A.EFFECTIVE_FROM 
    FROM RAWDATA_AGENTMOVEMENT A
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.OLD_REGION_CODE = B.REGION_CODE 
    WHERE 
    EFFECTIVE_FROM LIKE '%s'
    AND UPPER(MOVEMENT_TYPE) = 'DEMOTION'
    AND OLD_AGENT_DESIGNATION IN ('US', 'UM', 'BM')
    AND NEW_AGENT_DESIGNATION IN ('AG','US','UM')
    ", 
    strftime(business_dates, '%Y-%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

promoted_AL <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db'){
  p1=promotion_movement_SUM_SBM(business_dates, dbfile) %>% 
    dplyr::group_by(territory = TERRITORY) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) 
  
  p2=promotion_movement_US_UM_BM(business_dates, dbfile)%>% 
    dplyr::group_by(territory = TERRITORY) %>% 
    dplyr::summarise(value=n())%>% 
    dplyr::ungroup(.) 
  
  rbind(p1,p2) %>% 
    dplyr::group_by(territory) %>% 
    dplyr::summarise(value=sum(value))%>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(time_view = strftime(business_dates, '%Y%m')) %>% 
    dplyr::mutate(kpi='promoted_AL') 
}
demoted_AL <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db'){
  p1=demotion_movement_SUM_SBM(business_dates, dbfile) %>% 
    dplyr::group_by(territory = TERRITORY) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) 
  
  p2=demotion_movement_US_UM_BM(business_dates, dbfile)%>% 
    dplyr::group_by(territory = TERRITORY) %>% 
    dplyr::summarise(value=n())%>% 
    dplyr::ungroup(.) 
  
  rbind(p1,p2) %>% 
    dplyr::group_by(territory) %>% 
    dplyr::summarise(value=sum(value))%>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(time_view = strftime(business_dates, '%Y%m')) %>% 
    dplyr::mutate(kpi='demoted_AL') 
}
# RECRUIT -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  New recruited AL
new_recruited_AL <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = '2017-03-31'
  get_Manpower(business_dates, included_ter_ag = T, dbfile) %>% 
    # filter agent whose joining dates in the month of the business date
    dplyr::filter(
      substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
    ) %>% 
    dplyr::filter(AGENT_DESIGNATION %in% c('US','UM','BM')) %>%
    convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
    group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m')) %>% # group data by month
    summarise(`value` = n()) %>% 
    dplyr::ungroup(.)
}

#
# #active recruit leader: us, um, bm recruited atleast 1 agent in month----
# 
active_recruit_leader <- function(business_dates) {
  get_Manpower(business_dates, included_ter_ag = T) %>% 
  # filter agent whose joining dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  # chỉ đếm những leader nào tuyển AG mới, còn leader tuyển um, us... thì không đếm
  # dplyr::filter(AGENT_DESIGNATION=='AG') %>% 
  # tuy nhiên theo Châu Huỳnh thì trường hợp leader tuyển leader vẫn đếm luôn nên không lọc điều kiện này
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
  # tìm các leader có tuyển dụng trong tháng
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m'), SUPERVISOR_CODE) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.) %>% 
  dplyr::filter(value>=1) %>% # leader recruited at least 1 agent is counted as active recruit leader
  dplyr::filter(!is.na(SUPERVISOR_CODE) & SUPERVISOR_CODE != '') %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.)
}

total_recruit_mtd <- function(business_dates) {
  get_Manpower(business_dates, included_ter_ag = T) %>% 
    # filter agent whose joining dates in the month of the business date
    dplyr::filter(
      substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
    ) %>% 
    convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
    # tìm các leader có tuyển dụng trong tháng
    group_by(
      time_view='new_crecuted_inmonth',
      kpi=sprintf('agent_retention:recruited_in_%s', strftime(JOINING_DATE, '%Y%m'))
    ) %>% # group data by month
    summarise(`value` = n()) %>% 
    dplyr::ungroup(.)
}

agent_retention <- function(business_dates) {
  get_Manpower(business_dates, included_ter_ag = F) %>% 
    convert_string_to_date('JOINING_DATE', FORMAT_BUSSINESSDATE) %>% 
    convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
    # tìm các leader có tuyển dụng trong tháng
    group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), kpi=sprintf('agent_retention:recruited_in_%s', strftime(JOINING_DATE, '%Y%m'))) %>% # group data by month
    summarise(`value` = n()) %>% 
    dplyr::ungroup(.)
}

get_ExperienceLeaders <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT AGENT_CODE FROM RAWDATA_ExperienceLeaders
    ", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}


add_separated_col <- function(tem, pos, colnam) {
  tem1 = tem[,1:pos]
  tem1[,colnam] = ''
  tem = tem[, -c(1:pos)]
  cbind(tem1,tem)
}

compare_mom_qoq <- function(tem, fr_column, to_column, remove_compared = F) {
  # tem = North_results
  # fr_column =44
  # to_column=53
  # tem: data frame
  # fr_column: from column idx
  # to_column: to column idx
  # total_col = ncol(tem)
  tem1 = tem[,1:to_column] # create tem1 included the 1st to to_column columns
  tem = tem[,-c(1:to_column)] # remove columns from 1st to to_column from tem
  total_col = (to_column-fr_column+1)/2
  for (i in fr_column:(fr_column+total_col-1)) {
    col0 = colnames(tem1)[i]
    col1 = colnames(tem1)[i+total_col]
    tem1 = mutate(tem1, newcolname = tem1[,col1]/tem1[,col0]-1) 
    names(tem1)= gsub('newcolname', sprintf('MoM%s', col1), names(tem1))
  }
  if (remove_compared) {
    tem1 = tem1[,-c(fr_column:to_column)]
  }
  names(tem1) <- gsub('^MoMMoM', 'MoM', names(tem1))
  cbind(tem1,tem)
}
compare_yoy <- function(tem, fr_column, to_column, remove_compared = F) {
  # tem = North_results
  # fr_column =4
  # to_column=5
  # tem: data frame
  # fr_column: from column idx
  # to_column: to column idx
  # total_col = ncol(tem)
  tem1 = tem[,1:to_column] # create tem1 included the 1st to to_column columns
  tem = tem[,-c(1:to_column)] # remove columns from 1st to to_column from tem
  total_col = to_column
  for (i in fr_column:(total_col-1)) {
    col0 = colnames(tem1)[i]
    col1 = colnames(tem1)[i+1]
    tem1 = mutate(tem1, newcolname = tem1[,col1]/tem1[,col0]-1) 
    names(tem1)= gsub('newcolname', sprintf('YoY%s', ifelse(total_col-1-i,total_col-1-i,'')), names(tem1))
  }
  if (remove_compared) {
    tem1 = tem1[,-c(fr_column:to_column)]
  }
  cbind(tem1,tem)
}
convert_string_to_date <- function(df, colname, format) {
  df[,colname]=as.Date(strptime(df[,colname], format)) # parsing to date
  df
}

change_BUSSINESSDATE_Q <- function(df){
  df <- df %>% 
    dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
    dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%b-%y-%d'))) %>% # converts the bussiness date to date datatype
    dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
    dplyr::mutate(BUSSINESSDATE=paste(Q,strftime(BUSSINESSDATE,FORMAT_YEAR_y),sep="'")) # converts the bussiness date to YYYY-QQ format
  df  
}
change_BUSSINESSDATE_YTD <- function(df){
  df <- df %>% 
    dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
    dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%b-%y-%d'))) %>% # converts the bussiness date to date datatype
    dplyr::mutate(BUSSINESSDATE=paste("YTD", strftime(BUSSINESSDATE,FORMAT_YEAR_y), sep="'")) # converts the bussiness date to YYYY-QQ format
  df  
}


# excel -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# filter_data_for_mom <- function(kpidata_grouped_month) {
  # # filter data of the current year
  # month_of_year0 <- kpidata_grouped_month %>% 
  #   dplyr::filter(endsWith(BUSSINESSDATE, strftime(to, FORMAT_YEAR_y))) %>% # filter for the current year 
  #   # create new coresponding month column of the previous year
  #   dplyr::mutate(Y_1=gsub(strftime(to, FORMAT_YEAR_y), strftime(last_day_of_years[length(last_day_of_years)-1], FORMAT_YEAR_y), BUSSINESSDATE))
  # 
  # # filter data for the current year -1 
  # month_of_year1 <- kpidata_grouped_month %>% 
  #   # filter for the current year -1
  #   dplyr::filter(BUSSINESSDATE %in% month_of_year0$Y_1)
  # 
  # # row binding data of the current year and the the current year -1
  # month_of_year <- select(month_of_year0,-Y_1) %>% 
  #   rbind(.,month_of_year1) %>% # binding y0 and y1
  #   dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  #   dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  #   dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%b-%y-%d'))) %>% # converts the bussiness date to date datatype
  #   dplyr::mutate(BUSSINESSDATE=sprintf('MoM-%s',strftime(BUSSINESSDATE,'%b-%y'))) %>% # converts the bussiness date to YYYY-QQ format
  #   group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  #   dplyr::select(-D) 
  # month_of_year
# }

# filter_data_for_yoy <- function(kpidata_grouped_month) {
  # # filter data of the current year
  # yoy0 <- kpidata_grouped_month %>% 
  #   dplyr::filter(endsWith(BUSSINESSDATE, strftime(to, FORMAT_YEAR_y))) %>% # filter for the current year 
  #   dplyr::mutate(Y_1=gsub(strftime(to, FORMAT_YEAR_y), strftime(last_day_of_years[length(last_day_of_years)-1], FORMAT_YEAR_y), BUSSINESSDATE)) 
  # # filter data for the current year -1 
  # yoy1 <- kpidata_grouped_month %>% 
  #   dplyr::filter(BUSSINESSDATE %in% yoy0$Y_1) 
  # # row binding data of the current year and the the current year -1
  # yoy <- select(yoy0,-Y_1) %>% 
  #   rbind(.,yoy1) %>% # binding y0 and y1
  #   dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  #   dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  #   dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%b-%y-%d'))) %>% # converts the bussiness date to date datatype
  #   dplyr::mutate(BUSSINESSDATE=sprintf('YoY%s',strftime(BUSSINESSDATE,FORMAT_YEAR_y))) %>% # converts the bussiness date to YYYY-QQ format
  #   group_by(TERRITORY, BUSSINESSDATE) 
  # yoy
# }


find_cell_basedon_rowname_colname <- function(colname, rowname, selectedSheet, headerRowIndex, rowNameColIndex){
  #headerRowIndex: which row is the columns' names
  #rowNameColIndex: which col is the rows' names
  #colname: finding's column name
  #rowname: finding's row name
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  # get digits following by the dot
  colIndex = as.numeric(sub(".*\\.", "", names(columnNameValues[columnNameValues==colname])))  
  
  rows <- xlsx::getRows(selectedSheet)    
  rowNameCells <- xlsx::getCells(rows, colIndex = 1)
  rowNameValues <- lapply(rowNameCells, getCellValue) # extract the cell values
  
  # get digits before the dot
  rowIndex = as.numeric(sub("\\.[0-9]+$", "", names(rowNameValues[rowNameValues==rowname])))
  
  cells <- getCells(getRows(selectedSheet,rowIndex=rowIndex),colIndex = colIndex)
  cells
}

replace_cellvalue <- function(North_results, sheetname) {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook("KPI_PRODUCTION/output/GVL_Agency_reports_BASEDFILE.xlsx")
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[['North']]
  
  system.time(
    lapply(row.names(North_results), function(r) {
      lapply(colnames(North_results), function(c) {
        North_results[r, c]
        cells = find_cell_basedon_rowname_colname(colname = c,
                                                  rowname = r,
                                                  selectedSheet = selectedSheet,
                                                  headerRowIndex = 2,
                                                  rowNameColIndex = 1)
        lapply(names(cells), function(x) xlsx::setCellValue(cells[[x]], 333)) 
      })
    })
  )
  
  system.time( 
    for (c in colHeaders) {
      for (r in rowHeaders) {
        North_results[r, c]
        cells = find_cell_basedon_rowname_colname(colname = c,
                                                  rowname = r,
                                                  selectedSheet = selectedSheet,
                                                  headerRowIndex = 2,
                                                  rowNameColIndex = 1)
        lapply(names(cells), function(x) xlsx::setCellValue(cells[[x]], 100)) 
      }
    }
  )
  xlsx::saveWorkbook(wb, sprintf("KPI_PRODUCTION/output/GVL_Agency_reports_%s.xlsx", strftime(Sys.time(),'%y%m%d_%H%M%S')))
}

replace_cellvalue2 <- function(North_results, sheetname, rowNameColIndex = 1, headerRowIndex = 2, template="KPI_PRODUCTION/output/GVL_Agency_reports_BASEDFILE.xlsx") {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  rows <- xlsx::getRows(selectedSheet)    
  rowNameCells <- xlsx::getCells(rows, colIndex = rowNameColIndex)
  rowNameValues <- lapply(rowNameCells, getCellValue) # extract the cell values
  
  # remove prefix from colnames 
  # lapply(names(rowNameCells), function(x) {
  #   xlsx::setCellValue(rowNameCells[[x]], 
  #                      sub(".*\\:", "", xlsx::getCellValue(rowNameCells[[x]])))
  # })
  
  
  system.time(
    lapply(row.names(North_results), function(r) {
      lapply(colnames(North_results), function(c) {
        newVal = North_results[r, c]
        if (!(is.na(newVal))) {
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", names(columnNameValues[columnNameValues ==
                                                                          c])))
          # get digits before the dot
          rowIndex = as.numeric(sub("\\.[0-9]+$", "", names(rowNameValues[rowNameValues ==
                                                                            r])))
          cells <-
            getCells(getRows(selectedSheet, rowIndex = rowIndex), colIndex = colIndex)
          lapply(names(cells), function(x)
            xlsx::setCellValue(cells[[x]], newVal))
        }
      })
    })
  )
  # autosize column widths for all the columns in the sheet
  xlsx::autoSizeColumn(selectedSheet, colIndex=1:(1+ncol(North_results)))
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  # excelFile = sprintf("KPI_PRODUCTION/output/GVL_Agency_reports_%s.xlsx", strftime(Sys.time(),'%y%m%d'))
  excelFile = template
  xlsx::saveWorkbook(wb, excelFile)
  excelFile
}

replace_cellvalue2.1_set_cell_color <- function(df, sheetname, 
           rowNameColIndex, headerRowIndex, 
           template, result_file, color="white") {
    # each element in the df will be written to the excel sheet 
    # based on it colname and rowname
    wb = xlsx::loadWorkbook(template)
    sheets = xlsx::getSheets(wb)
    selectedSheet = sheets[[sheetname]]
    
    columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
    columnNameCells <- xlsx::getCells(columnNameRows)
    columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
    rows <- xlsx::getRows(selectedSheet)    
    rowNameCells <- xlsx::getCells(rows, colIndex = rowNameColIndex)
    rowNameValues <- lapply(rowNameCells, getCellValue) # extract the cell values
    
    for (r in names(rowNameValues)) {
      for (c in names(columnNameValues)) {
        rv=rowNameValues[[r]]
        cv=columnNameValues[[c]]
        
        i = match(rv, rownames(df))
        j = match(cv, colnames(df))
        
        if (!is.na(i) & !is.na(j)) {
          v = df[i, j]
          if (!is.na(v)){
            
            
            # get digits before the dot
            rowIndex = as.numeric(sub("\\.[0-9]+$", "", r))
            if (rowIndex != headerRowIndex) {
              # get digits following by the dot
              colIndex = as.numeric(sub(".*\\.", "", c))
              if (colIndex !=headerRowIndex){
                arow = getRows(selectedSheet, rowIndex = rowIndex)
                cells <- getCells(arow, colIndex = colIndex)
                if (is.null(cells)) {
                  xlsx::createCell(arow, colIndex = colIndex)
                  cells <- getCells(arow, colIndex = colIndex)
                  lapply(names(cells), function(x) { 
                    c = cells[[x]]
                    xlsx::setCellValue(c, v)
                    dataFormat =  xlsx::DataFormat('#,##0') 
                    cs = CellStyle.default(wb) + dataFormat + xlsx::Fill(foregroundColor=color, backgroundColor=color, pattern="SOLID_FOREGROUND")  
                    xlsx::setCellStyle(c, cs)
                  })
                } else {
                  lapply(names(cells), function(x) { 
                    c = cells[[x]]
                    xlsx::setCellValue(c, v)
                    # cách này replace luôn fomula trong cell bằng v
                    # c[["Value"]] <- v
                    dataFormat =  xlsx::DataFormat('#,##0') 
                    cs = CellStyle.default(wb) + dataFormat + xlsx::Fill(foregroundColor=color, backgroundColor=color, pattern="SOLID_FOREGROUND")  
                    xlsx::setCellStyle(c, cs)
                  })
                }
              }
            }
          }
        }
      }
    }
    # set vay
    # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
    # xlsx::setCellValue(cell11, "tao day ky")
    
    #Exporting data from R to Excel: formulas do not recalculate
    wb$setForceFormulaRecalculation(T)
    
    # autosize column widths for all the columns in the sheet
    xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
    # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
    xlsx::saveWorkbook(wb, result_file)
}

replace_cellvalue2.1 <- function(df, sheetname, 
                                 rowNameColIndex, headerRowIndex, 
                                 template, result_file) {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  rows <- xlsx::getRows(selectedSheet)    
  rowNameCells <- xlsx::getCells(rows, colIndex = rowNameColIndex)
  rowNameValues <- lapply(rowNameCells, getCellValue) # extract the cell values
  
  for (r in names(rowNameValues)) {
    for (c in names(columnNameValues)) {
      rv=rowNameValues[[r]]
      cv=columnNameValues[[c]]
      
      i = match(rv, rownames(df))
      j = match(cv, colnames(df))
      
      if (!is.na(i) & !is.na(j)) {
        v = df[i, j]
        if (!is.na(v)){
          
          
          # get digits before the dot
          rowIndex = as.numeric(sub("\\.[0-9]+$", "", r))
          if (rowIndex != headerRowIndex) {
            # get digits following by the dot
            colIndex = as.numeric(sub(".*\\.", "", c))
            if (colIndex !=headerRowIndex){
              arow = getRows(selectedSheet, rowIndex = rowIndex)
              cells <- getCells(arow, colIndex = colIndex)
              if (is.null(cells)) {
                xlsx::createCell(arow, colIndex = colIndex)
                cells <- getCells(arow, colIndex = colIndex)
                lapply(names(cells), function(x) { 
                  c = cells[[x]]
                  xlsx::setCellValue(c, v)
                })
              } else {
                lapply(names(cells), function(x) { 
                  c = cells[[x]]
                  xlsx::setCellValue(c, v)
                })
              }
            }
          }
        }
      }
    }
  }
  # set vay
  # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
  # xlsx::setCellValue(cell11, "tao day ky")
 
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  
  # autosize column widths for all the columns in the sheet
  xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
}

sheet_region_kpi <- function(df, workbook, sheetName, row_name_prefixes, offset=1) {
  sheet0 <- xlsx::createSheet(workbook, sheetName=sheetName) # create different sheets
  # add data frame to the sheets
  
  for (i in 1:length(row_name_prefixes)) {
    pre = row_name_prefixes[i]
    selected_row_indexes = grep(sprintf("^%s", pre), rownames(df))
    if (length(selected_row_indexes)) {
      temdt = as.data.frame(df[selected_row_indexes,])
      # remove prefix from colnames 
      # rownames(temdt) <- gsub(pre, "", rownames(temdt))
      startRow = ifelse(i==1,(selected_row_indexes[1] + offset),(selected_row_indexes[1]+i+(i*offset-1)))
      print(startRow)
      xlsx::addDataFrame(
        temdt,
        sheet0,
        row.names=T, startRow = startRow,
        colnamesStyle = TABLE_COLNAMES_STYLE,
        rownamesStyle = TABLE_ROWNAMES_STYLE
      )
      # Set cell stype for all the rows excluded row is headers
      rows  <- getRows(sheet0, rowIndex=(startRow+1):(startRow+1+nrow(temdt)))   
      cells <- xlsx::getCells(rows, colIndex = 2:(ncol(temdt)+1)) # get all the cells excluded the 1st col
      lapply(names(cells), function(ii) xlsx::setCellStyle(cells[[ii]], NUMERIC_STYLE))
      # format percentage for YoY, MoM columns
      cellsYoY <- xlsx::getCells(rows, colIndex = 1+grep("YoY", names(temdt)))
      lapply(names(cellsYoY), function (x) xlsx::setCellStyle(cellsYoY[[x]], PERCENTAGE_STYLE))
      cellsMoM <- xlsx::getCells(rows, colIndex = 1+grep("MoM", names(temdt)))
      lapply(names(cellsMoM), function (x) xlsx::setCellStyle(cellsMoM[[x]], PERCENTAGE_STYLE))
      
      # format cel A1 Overall KPI performance
      cell11 <- getCells(getRows(sheet0,rowIndex=startRow:(startRow+1)),colIndex = 1:2)[[1]]
      xlsx::setCellValue(cell11, gsub("[_]*[:]"," ", pre))
      xlsx::setCellStyle(cell11, TABLE_COLNAMES_STYLE_BLUE)
      
      # autosize column widths for all the columns in the sheet
      xlsx::autoSizeColumn(sheet0, colIndex=1:(1+ncol(df)))
      
      # replace separated columns' values by sep1
      header_row  <- getRows(sheet0, rowIndex=startRow)   # get the header row
      cellsSeps <- xlsx::getCells(header_row, colIndex = 1+grep("^sep", names(df)))
      lapply(names(cellsSeps), function (x) xlsx::setCellValue(cellsSeps[[x]], ''))
    }
  }
  
  # Create a freeze pane, fix first row and column
  xlsx::createFreezePane(sheet0, colSplit = 2, rowSplit = 2+offset)
  sheet0
}


export_report_data_to_excelfile <- function() {
  wb <-
    xlsx::createWorkbook(type = "xlsx")         # create blank workbook
  
  # Styles for the data table row/column names 
  NUMERIC_STYLE <-
    xlsx::CellStyle(wb, dataFormat =  xlsx::DataFormat('#,##0.0'))
  # + Fill(foregroundColor="lightblue", backgroundColor="lightblue", pattern="SOLID_FOREGROUND")
  # + Font(wb, isBold=TRUE)
  
  PERCENTAGE_STYLE <-
    xlsx::CellStyle(wb, dataFormat =  xlsx::DataFormat('#,##0.0%')) #+ Font(wb, isBold=TRUE)
  
  TABLE_ROWNAMES_STYLE <-
    xlsx::CellStyle(wb) + Font(wb, isBold = TRUE) +
    Alignment(wrapText = TRUE, horizontal = "ALIGN_RIGHT")
  
  TABLE_COLNAMES_STYLE <-
    xlsx::CellStyle(wb) + Font(wb, isBold = TRUE) +
    Alignment(wrapText = TRUE, horizontal = "ALIGN_RIGHT") +
    Border(
      color = "black",
      position = c("TOP", "BOTTOM"),
      pen = c("BORDER_THIN", "BORDER_THIN")
    )
  
  TABLE_COLNAMES_STYLE_BLUE <-
    xlsx::CellStyle(wb) + Font(wb, isBold = TRUE, color = 'blue') +
    # Alignment(wrapText=TRUE, horizontal="ALIGN_CENTER") +
    Alignment(wrapText = TRUE, horizontal = "ALIGN_RIGHT") +
    Border(
      color = "blue",
      position = c("TOP", "BOTTOM"),
      pen = c("BORDER_THIN", "BORDER_THIN")
    )
  
  # add sheets to the workbook
  sheet0 <-
    sheet_region_kpi(
      North_results,
      wb,
      "North",
      c(
        "Overall KPI performance:",
        "Ending MP:",
        "Recruitment:",
        "AL recruitment KPIs:",
        "APE:",
        "# Active:"
      )
    )
  sheet1 <-
    sheet_region_kpi(
      South_results,
      wb,
      "South",
      c(
        "Overall KPI performance:",
        "Ending MP:",
        "Recruitment:",
        "AL recruitment KPIs:",
        "APE:",
        "# Active:"
      )
    )
  
  
  # write the file with multiple sheets
  xlsx::saveWorkbook(wb,
                     sprintf(
                       "KPI_PRODUCTION/output/GVL_Agency_reports_%s.xlsx",
                       strftime(Sys.time(), '%y%m%d_%H%M%S')
                     ))
}

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
report_kpi_segmentation <- function(criteria='', dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = sprintf("select trim(kpi) as kpi, time_view, value from report_kpi_segmentation %s", criteria)
  result <- dbSendQuery(my_database$con, sql)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

report_rookie_metric_by_recruited_month <- function(criteria='', dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = sprintf("select trim(kpi) as kpi, time_view, value from report_rookie_metric_by_recruited_month %s", criteria)
  result <- dbSendQuery(my_database$con, sql)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}

report_agent_retention <- function(criteria='', dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = sprintf("select trim(kpi) as kpi, time_view, value from report_agent_retention %s", criteria)
  result <- dbSendQuery(my_database$con, sql)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  dbDisconnect(my_database$con)
  data
}
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# DATE UTIL ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


generate_last_day_of_month <- function(yyyy) {
  fr <- as.Date(sprintf('%s-01-01', yyyy))
  to <- as.Date(sprintf('%s-12-31', yyyy))
  # Generate a sequence of the fisrt day and last day of the month over 5 years ---------------------------------------------------------------------------------------------------------------------
  first_day_of_months <- seq(from=fr, to=to, by="1 month")
  last_day_of_months <- seq(from=seq(fr, length=2, by='1 month')[2], to=seq(to, length=2, by='1 month')[2], by="1 month") - 1
  last_day_of_months
}

get_last_day_of_m0 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=2, by='1 month')-1
  re[2]
}

get_last_day_of_m1 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=1, by='-1 month')-1
  re
}

get_last_day_of_m2 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=2, by='-1 month')-1
  re[2]
}

get_last_day_of_m3 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=3, by='-1 month')-1
  re[3]
}

get_last_day_of_m4 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=4, by='-1 month')-1
  re[4]
}

get_last_day_of_m5 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=5, by='-1 month')-1
  re[5]
}

get_last_day_of_m6 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=6, by='-1 month')-1
  re[6]
}

count_different_month_between_2days <- function(d1, d2) {
  length(seq(from=d1, to=d2, by="1 month"))-1
}

# import data -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import_agentlist_from_msaccess <- function(BUSSINESSDATE, accessdb="KPI_PRODUCTION/KPI_PRODUCTION.accdb", dbfile = 'KPI_PRODUCTION/main_database.db') {
  # ++++++++++++++++++++++++++++++++++++++++++++++++++
  # param:
  # BUSSINESSDATE: %Y%m%d
  # ++++++++++++++++++++++++++++++++++++++++++++++++++
  tablenames=c("AgentList")
  accessConn<-odbcConnectAccess2007(accessdb, readOnlyOptimize=T)
  my_database <- src_sqlite(dbfile, create = TRUE)
  all_tables <- subset(sqlTables(accessConn), 
                       TABLE_TYPE %in% c("TABLE", "VIEW"), 
                       TABLE_NAME) # get all tables' name from the data base
  all_tables <- dplyr::filter(all_tables, TABLE_NAME %in% ifelse(is.na(tablenames), all_tables$TABLE_NAME, tablenames))
  
  for (i in 1:nrow(all_tables)) {
    tablename <- (all_tables[i,'TABLE_NAME'])
    print(sprintf('Data copying for %s', tablename))
    # result <- sqlFetch(accessConn, tablename) # fetch data from tables
    # thay bang 2 dong ben duoi
    accessSQL = sprintf("SELECT * from [%s]", tablename)
    result <- RODBC::sqlQuery(accessConn, accessSQL)
    
    result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns
    
    fieldnames <- names(result) # get all field names in the result
    fieldnames <- gsub("^\\s+|\\s+$", "", fieldnames) # trim leading and trailing whitespace (left trim, right trim)
    fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
    names(result) <- toupper(fieldnames) # rename all fields in the result
    
    # add PARTFULLTIMECD field if there is not any
    if (!('PARTFULLTIMECD' %in% names(result))) {
      result <- result %>% dplyr::mutate(PARTFULLTIMECD='')
    }
    # add PARTFULLTIME_NAME field if there is not any
    if (!('PARTFULLTIME_NAME' %in% names(result))) {
      result <- result %>% dplyr::mutate(PARTFULLTIME_NAME='')
    }
    # add BANKCD field if there is not any
    if (!('BANKCD' %in% names(result))) {
      result <- result %>% dplyr::mutate(BANKCD='')
    }
    # add BANKBRANCHCD field if there is not any
    if (!('BANKBRANCHCD' %in% names(result))) {
      result <- result %>% dplyr::mutate(BANKBRANCHCD='')
    }
    
    # add business date field if there is not any
    if (!('BUSSINESSDATE' %in% names(result))) {
      result <- result %>% dplyr::mutate(BUSSINESSDATE=strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')),'%Y-%m-%d'))
    }
    
    tablename <- 'GVL_AGENTLIST'
    
    result <- dplyr::filter(result, !(is.na(AGENTCD)) & AGENTCD!='' ) 
    result <- dplyr::select(result,
                            BUSSINESSDATE,        AGENT_DESIGNATION,
                            AGENTCD,              STAFFCD,
                            APPLICATION_DATE,     AGENT_STATUS,
                            JOINING_DATE,         CLASSIFICATION,
                            APPROVED_DATE,        TERMINATION_DATE,
                            
                            REINSTATEMENT_DATE,   ZONECD,
                            CHANNELCD,            TEAMCD,
                            SUBCHANNELCD,         OFFICECD,
                            AGENCYCD,             BMCD,
                            REGIONCD,             BRANCHCD,
                            
                            UMCD,                 REPORTINGCD,
                            UNITCD,               UNITCD_OF_REPORTINGCD
                            
    )
    if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
      copy_to(my_database, result, name=tablename, temporary = FALSE) # save data to a new table
    } else { 
      results <- dbSendQuery(my_database$con, sprintf("delete FROM %s WHERE BUSSINESSDATE='%s'",
                                                      tablename,
                                                      strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')),'%Y-%m-%d')
      )) # clear data from the table
      dbClearResult(results) # clear result set
      db_insert_into(con = my_database$con, table = tablename, values = result) # insert data to the table
      # insert_or_replace_bulk(result, tablename, bulksize = 1000)
    }
  }
  close(accessConn) 
  dbDisconnect(my_database$con)
}

import_kpiproduction_msaccess <- function(tablenames=NA, accessdb="KPI_PRODUCTION/KPI_PRODUCTION.accdb", dbfile = 'KPI_PRODUCTION/main_database.db') {
  # accessConn<-odbcConnectAccess2007("d:/Data/DA_201707/KPI_PRODUCTION.accdb", readOnlyOptimize=T)
  accessConn<-odbcConnectAccess2007(accessdb, readOnlyOptimize=T)
  my_database <- src_sqlite(dbfile, create = TRUE)
  all_tables <- subset(sqlTables(accessConn), 
                       TABLE_TYPE %in% c("TABLE", "VIEW"), 
                       TABLE_NAME) # get all tables' name from the data base
  all_tables <- dplyr::filter(all_tables, TABLE_NAME %in% ifelse(is.na(tablenames), all_tables$TABLE_NAME, tablenames))
  
  for (i in 1:nrow(all_tables)) {
    tablename <- (all_tables[i,'TABLE_NAME'])
    print(sprintf('Data copying for %s', tablename))
    # result <- sqlFetch(accessConn, tablename) # fetch data from tables
    # thay bang 2 dong ben duoi
    accessSQL = sprintf("SELECT * from [%s]", tablename)
    result <- RODBC::sqlQuery(accessConn, accessSQL)
    
    result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns
    
    fieldnames <- names(result) # get all field names in the result
    fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
    names(result) <- toupper(fieldnames) # rename all fields in the result
    
    tablename <- gsub(' ', '', tablename) # replace all special characters from the table name 
    tablename <- gsub('&', '', tablename) # replace all special characters from the table name 
    tablename <- gsub('-', '', tablename) # replace all special characters from the table name
    tablename <- gsub('%', '', tablename) # replace all special characters from the table name
    tablename <- sprintf('RAWDATA_%s', tablename)
    if (!(tablename %in% dbListTables(my_database$con))) {
      copy_to(my_database, result, name=tablename, temporary = FALSE)
    } else {
      results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
      dbClearResult(results)
      db_insert_into(con = my_database$con, table = tablename, values = result)
    }
  }
  close(accessConn) 
  dbDisconnect(my_database$con)
}


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# RDCOMClient utils -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
RDCOMClient_replace_cellvalues <- function(df, excelfile, newexcelfile, sheetname, visible, rowNameColIndex, headerRowIndex) {
  ############################
  # excelfile="D:/DOM/DIEM/SP 18-22/1.0. GVL Agency Sales Plan_2018 2022_working file (July 2017)_Tung_updated.xlsx"
  # newexcelfile = sprintf("D:/DOM/DIEM/SP 18-22/1.0. GVL Agency Sales Plan_2018 2022_working file (July 2017)_Tung_updated_%s.xlsx", strftime(Sys.time(),'%y%m%d'))
  # sheetname="Agency North"
  # visible=T
  # rowNameColIndex=1
  # headerRowIndex=1
  ############################
  xlApp <- COMCreate("Excel.Application")
  xlApp$Quit() # close running Excel if any
  # wb <- xlApp[["Workbooks"]]$Open(excelfile)
  wb <- xlApp$Workbooks()$Open(excelfile)
  sheet <- wb$Worksheets(sheetname)
  sheet[["Select"]]
  used_range = sheet$UsedRange()
  
  # Make Excel workbook visible to user
  xlApp[["Visible"]] <- visible
  
  # change the value of a single cell
  rowcount = used_range$Rows()$Count()
  colcount = used_range$Columns()$Count()
  tryCatch({
    for (rowidx in seq(rowcount)) {
      # rowNameColIndex: colunm ind which contains rows' names
      rowidx_val = sheet$Cells(rowidx, rowNameColIndex)$Value()
      if (!is.null(rowidx_val)) {
        for (colidx in seq(colcount)) {
          colidx_val = sheet$Cells(headerRowIndex, colidx)$Value()
          if (!is.null(colidx_val)) {
            
            i = match(rowidx_val, rownames(df))
            j = match(colidx_val, colnames(df))
            
            if (!is.na(i) & !is.na(j)) {
              v = df[i, j]
              if (!is.na(v)){
                cell  <- sheet$Cells(rowidx,colidx)
                cell[["Value"]] <- v
              }
            }
          }
        }
      }
    }
  }, warning = function(w) {
    # warning-handler-code
  }, error = function(e) {
    # error-handler-code
    print(rowidx_val)
    print(colidx_val)
    # print(sprintf(sql, 
    #               tbname, 
    #               paste(names(df), collapse = ","),
    #               values
    # ))
  }, finally = {
    # cleanup-code
  })
  
  # change the value of a range
  # #################################################
  # range <- sheet$Range("A1:F1")
  # range[["Value"]] <- paste("Col",1:6,sep="-")
  # #################################################
  
  # Force to Overwrite An Excel File through RDCOMClient Package in R, 
  # Ghi đè file sẵn có nếu có không cần thông báo
  xlApp[["DisplayAlerts"]] <- FALSE
  
  # wb$Save()                  # save the workbook
  wb$SaveAS(newexcelfile)      # save as a new workbook
  xlApp[["DisplayAlerts"]] <- T # TRA LAI DEFAULT CONFIG
  xlApp$Quit()  
}

RDCOMClient_set_cellvalue <- function(RDCOMClient_workbook, str_sheetname, rowidx, colidx, value) {
  sheet <- RDCOMClient_workbook$Worksheets(str_sheetname)
  cell  <- sheet$Cells(rowidx,colidx)
  cell[["Value"]] <- value
}

RDCOMClient_get_row_values <- function(RDCOMClient_workbook, str_sheetname, int_rowidx) {
  sheet <- RDCOMClient_workbook$Worksheets(str_sheetname)
  used_range = sheet$UsedRange()
  unlist(used_range$Rows()$Item(int_rowidx)$Value()) 
}

RDCOMClient_get_col_values <- function(RDCOMClient_workbook, str_sheetname, int_colidx) {
  sheet <- RDCOMClient_workbook$Worksheets(str_sheetname)
  used_range = sheet$UsedRange()
  # the same of below: 
  # unlist(used_range[["Columns"]]$Item(int_colidx)$Value())  
  unlist(used_range$Columns()$Item(int_colidx)$Value()) 
}

RDCOMClient_row_count <- function(RDCOMClient_workbook, str_sheetname) {
  sheet <- RDCOMClient_workbook$Worksheets(str_sheetname)
  used_range = sheet$UsedRange()
  used_range$Rows()$Count()
}

RDCOMClient_col_count <- function(RDCOMClient_workbook, str_sheetname) {
  sheet <- RDCOMClient_workbook$Worksheets(str_sheetname)
  used_range = sheet$UsedRange()
  used_range$Columns()$Count()
}

