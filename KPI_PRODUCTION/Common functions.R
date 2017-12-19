library(dplyr)
library(RSQLite)
# load xlsx package
# Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_144")
# options(java.parameters = "-Xmx5120m") 

Sys.setenv(JAVA_HOME = "C:\\Program Files (x86)\\Java\\jre1.8.0_151")
options(java.parameters = "-Xmx1024m")
library(xlsx)
library (lubridate)
library(zoo)
library(tibble)
library(readxl)
library(tidyr)
library(openxlsx)
library(RODBC)
library(RDCOMClient)

color_country = "#00CCFF"
color_territory = "#CCFFFF"
color_region = '#FFFF99'
color_zone = '#CCFFFF'
color_team = '#FFFF99'
dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db'

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

get_office_casesize <- function(last_day_of_months, dbfile = 'KPI_PRODUCTION/main_database.db'){
  office_ape <- get_ape_sum_by_office(last_day_of_months, dbfile)
  office_main_case <- get_main_case_sum_by_office(last_day_of_months, dbfile)
  office_casesize <- office_ape %>%
    merge(
      x = .,
      y = office_main_case,
      by.x = c('time_view', 'OFFICE'),
      by.y = c('time_view', 'OFFICE')
    ) %>%
    dplyr::mutate(CASESIZE = APE / CASECOUNT) %>% 
    dplyr::select( -APE, -CASECOUNT)
  office_casesize
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
    data = mutate(data, time_view=strftime(d,"%Y%m"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE %>% 
    dplyr::mutate(APE = APE/10^6) %>% 
    tidyr::separate(., OFFICE, c('OFFICE'), sep=" ")
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
    data = mutate(data, time_view = strftime(d,"%Y%m"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE %>% 
    dplyr::mutate(APE = APE/10^6) %>% 
    tidyr::separate(., OFFICE, c('OFFICE'), sep = " ")
}

get_BANCA_mp <- function(dbfile){
  my_database <- src_sqlite(dbfile)
  SQL <- "
  SELECT BSSDT, OFFICE_NAME, DESIGNATIONCODE, AGENT_DESIGNATION FROM
  BANCASSURANCE_MANPOWER T1 LEFT JOIN DESIGNATION_MAPPING T2 ON UPPER(T1.AGENT_DESIGNATION)=UPPER(T2.DESIGNATION_DESC)
  "
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding = "utf-8")
  dbClearResult(result)
  data %>% 
    dplyr::mutate(BSSDT = as.Date(strptime(BSSDT, '%Y-%m-%d'))) %>% 
    dplyr::mutate(time_view = strftime(BSSDT, '%Y%m')) %>% 
    tidyr::separate(., OFFICE_NAME, c('OFFICE'), sep = " ")
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
    data = mutate(data, time_view=strftime(d,"%Y%m"))
    data
  })
  )
  dbDisconnect(my_database$con)
  APE %>% 
    dplyr::mutate(APE = APE/10^6) %>% 
    tidyr::separate(., OFFICE, c('OFFICE'), sep = " ")
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
    data = fetch(result, encoding = "utf-8")
    dbClearResult(result)
    data = mutate(data, time_view = strftime(d,"%Y%m"))
    data
  })
  )
  dbDisconnect(my_database$con)
  CASE %>% 
    tidyr::separate(., OFFICE, c('OFFICE'), sep = " ")  
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
    data = mutate(data, time_view = strftime(d, "%Y%m"))
    data
  })
  )
  dbDisconnect(my_database$con)
  CASE %>% 
    tidyr::separate(., OFFICE, c('OFFICE'), sep = " ") 
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
    # last date in pre of previous quarter
    ldpq = zoo::as.Date(zoo::as.yearqtr(ldpq)) - 1
    result <- dbSendQuery(my_database$con, sprintf(SQL, d, ldpq))
    data = fetch(result, encoding="utf-8")
    dbClearResult(result)
    data
  })
  )
  # dbDisconnect(my_database$con)
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
    AND (A.SERVICING_AGENT='Yes' OR A1.STAFFCD='SA')
    --AND A1.AGENT_STATUS IN ('Active','Suspended')
    ", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # dbDisconnect(my_database$con)
  data
}

get_Manpower <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.SUPERVISOR_CODE, A.SUPERVISOR_CODE_DESIGNATION, 
       A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.TERMINATION_DATE, 
       A.REINSTATEMENT_DATE, A.AGENT_DESIGNATION,
       A1.AGENT_STATUS, A.SERVICING_AGENT, A1.STAFFCD, A.ACTIVESP, A.MONTHSTART, A.MONTHEND, A.ZONECD, A.TEAMCD, A.CHANNELCD, A1.OFFICECD, A.UNITCD, A.BRANCHCD
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
    left JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    AND A1.AGENT_STATUS IN ('Active','Suspended', '%s')
    ", 
    paste(last_day_of_months, collapse = "','"),  ifelse(included_ter_ag,'Terminated',''))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # dbDisconnect(my_database$con)
  data %>% dplyr::mutate(time_view = strftime(as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d')), '%Y%m'))
}

get_Manpower_1.2 <- function(last_day_of_months, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT A.AGENT_CODE, A.SUPERVISOR_CODE, A.SUPERVISOR_CODE_DESIGNATION, 
       A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE, A.TERMINATION_DATE, 
       A.REINSTATEMENT_DATE, A.AGENT_DESIGNATION,
       A.SERVICING_AGENT, A.ACTIVESP, A.ACTIVE, A.MONTHSTART, A.MONTHEND, A.ZONECD, A.TEAMCD, A.UNITCD, A.BRANCHCD
    FROM RAWDATA_MANPOWER_ACTIVERATIO A
    JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
    WHERE A.BUSSINESSDATE IN ('%s') 
    ", 
    paste(last_day_of_months, collapse = "','"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # dbDisconnect(my_database$con)
  data %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
}

get_kpi <- function(bssdt, included_ter_ag=F, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT * FROM RAWDATA_KPITotal WHERE BUSSINESSDATE LIKE '%s'
    ", 
    bssdt)
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
  data %>% dplyr::mutate(time_view = strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')), '%Y%m'))
}

get_team_structure <- function(dbfile){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "select TEAM_NAME as team, ZONE_NAME as zone, REGION_NAME as region from RAWDATA_ADLIST
    ", 
    strftime(bssdt, '%Y%m%d'))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data
}

get_kpi_in_a_period <- function(frdt, todt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT * FROM RAWDATA_KPITotal WHERE BUSSINESSDATE >= '%s' AND BUSSINESSDATE <= '%s'
    ", 
    frdt, todt)
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # dbDisconnect(my_database$con)
  data
}

get_base_productcd <- function(dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
    "SELECT PRUDUCTCODE AS PRODUCTCODE FROM RAWDATA_Product WHERE upper(BASEPRODUCT)='YES'
    "
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data
}
all_product <- function(dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
      "
SELECT A.*, B.VIETNAMESENAME FROM (
  SELECT
  PRUDUCTCODE   AS PRODUCTCODE,
  PRODUCTNAME,
  'BASEPRODUCT' AS PRODUCT_TYPE
  FROM RAWDATA_Product
  WHERE UPPER(BASEPRODUCT) = 'YES'
  UNION ALL
  SELECT
  PRUDUCTCODE AS PRODUCTCODE,
  PRODUCTNAME,
  'RIDER'     AS PRODUCT_TYPE
  FROM RAWDATA_Product
  WHERE UPPER(BASEPRODUCT) = 'NO'
  UNION ALL
  SELECT
  PRUDUCTCODE AS PRODUCTCODE,
  PRODUCTNAME,
  'SME'       AS PRODUCT_TYPE
  FROM RAWDATA_Product_SME
  ) A LEFT JOIN GVL_PRODUCT B ON A.PRODUCTCODE = B.PRUDUCTCODE
      "
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
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
    "SELECT distinct A1.AGENTCD as AGENT_CODE, A1.REGIONCD, B.TERRITORY, A1.JOINING_DATE, A1.BUSSINESSDATE, A1.REINSTATEMENT_DATE, A1.ZONECD, A1.TEAMCD 
  FROM GVL_AGENTLIST A1 
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A1.REGIONCD = B.REGION_CODE
  WHERE A1.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended','%s')
  AND A1.AGENT_DESIGNATION='AG'
  ", paste(last_day_of_months, collapse = "','"), 'Terminated')
  result <- dbSendQuery(my_database$con, AG)
  data = fetch(result, encoding="utf-8")
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
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

get_kpitotal_policies_first_issued_for_baseproduct_8mIPup <- function(yyyy, mm, dbfile = 'KPI_PRODUCTION/main_database.db') {
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
    AND (
        (TXNCODE='T642' AND BUSSINESSDATE=HOISSDTE) 
        OR 
        (TXNCODE='T646' OR TXNCODE='TR7K') 
    )
    AND UPPER(T2.BASEPRODUCT)='YES'
    
    ", yyyy, mm, '%')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
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
  kpitotal_case <- get_kpitotal_policies_first_issued_for_baseproduct_8mIPup('%', '%')
    
  # kpitotal_case <- rbind(
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m0,'%Y'), strftime(m0,'%m')),
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m1,'%Y'), strftime(m1,'%m')),
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m2,'%Y'), strftime(m2,'%m')),
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m3,'%Y'), strftime(m3,'%m')),
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m4,'%Y'), strftime(m4,'%m')),
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m5,'%Y'), strftime(m5,'%m')),
  #   get_kpitotal_policies_first_issued_for_baseproduct_8mIPup(strftime(m6,'%Y'), strftime(m6,'%m'))
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

get_Rookie_Performance <- function(last_day_of_month, dbfile = 'KPI_PRODUCTION/main_database.db'){
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
  
  mp = get_Manpower(last_day_of_month, T, dbfile)
  
  kpitotal_case <- get_kpitotal_policies_first_issued('%', '%')
  
  
  # recruite_by_month <- data %>% 
  #   dplyr::group_by(recruit_month = strftime(JOINING_DATE, '%Y%m')) %>% 
  #   summarise(new_recruited_agents = n())
    
  casecount_summary <- mp %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
    # tư vấn có hợp đồng vào ngày thứ x sau ngày joined in date 
    dplyr::mutate(HAS_POLICY_ISSUED_IN_DAYS=HOISSDTE-JOINING_DATE) %>% 
    dplyr::mutate(has_policy_d15=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:15), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d30=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:30), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d60=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:60), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d90=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:90), CASE, 0)) %>% 
    dplyr::group_by(recruit_month = strftime(JOINING_DATE, '%Y%m'), TERRITORY, AGENT_CODE) %>% 
    # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
    dplyr::summarise(
      has_policy_d15=sum(has_policy_d15),
      has_policy_d30=sum(has_policy_d30),
      has_policy_d60=sum(has_policy_d60),
      has_policy_d90=sum(has_policy_d90)
      ) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate('1case/15d' = ifelse(has_policy_d15 >= 1, 1, 0)) %>% 
    dplyr::mutate('1case/30d' = ifelse(has_policy_d30 >= 1, 1, 0)) %>% 
    dplyr::mutate('3case/60d' = ifelse(has_policy_d60 >= 3, 1, 0)) %>% 
    dplyr::mutate('5case/90d' = ifelse(has_policy_d90 >= 5, 1, 0)) %>% 
    dplyr::group_by(time_view=recruit_month, territory=TERRITORY) %>% 
    dplyr::summarise(
      `1case/15d`=sum(`1case/15d`),
      `1case/30d`=sum(`1case/30d`),
      `3case/60d`=sum(`3case/60d`),
      `5case/90d`=sum(`5case/90d`)
    )
  
  (tidyr::gather(casecount_summary, kpi, value, -time_view, -territory))
}

get_segmentation_by_genlion_sa_rookie <- function(business_date, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # excluded ter agents
  genlion=get_Manpower_genlion(business_date, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) 
  
  # excluded ter agents
  sa=get_Manpower_sa(business_date, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) 
    
  # excluded ter agents
  mp = get_Manpower(business_date, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    # calculate different months between joining date and business date
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    # loại genlion
    dplyr::filter(!AGENT_CODE %in% genlion$AGENT_CODE) %>% 
    # loại sa
    dplyr::filter(!AGENT_CODE %in% sa$AGENT_CODE)
  
  # Manpower_by_rookie_GENLION:Rookie in month
  r1 = mp %>% 
    dplyr::filter(substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) )
  
  # Manpower_by_rookie_GENLION:Rookie last month
  r2 = mp %>% 
    dplyr::filter(MDIFF==1)
  
  # Manpower_by_rookie_GENLION:2-3 months
  r3 = mp %>% 
    dplyr::filter(MDIFF %in% c(2:3))
  
  # Manpower_by_rookie_mdrt:4 - 6 mths
  r4 = mp %>% 
    dplyr::filter(MDIFF %in% c(4:6))
  
  # Manpower_by_rookie_GENLION:7-12mth
  r5 = mp %>% 
    dplyr::filter(MDIFF %in% c(7:12))
  
  # Manpower_by_rookie_GENLION:13+mth
  r6 = mp %>% 
    dplyr::filter(MDIFF >= 13)
  
  #  Recruit_by_designation:AG 
  new_recuited_ag = r1 %>% 
    dplyr::filter(AGENT_DESIGNATION=='AG')
  
  #  Recruit_by_designation:AG 
  new_recuited_al = r1 %>% 
    dplyr::filter(AGENT_DESIGNATION!='AG')
  
  # summary
  rbind(
  genlion %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:MDRT/ GEN Lion (from Apr '17)") 
  ,
  sa %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:SA") 
  ,
  r1 %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:Rookie in month") 
  ,
  r2 %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:Rookie last month") 
  ,
  r3 %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:2-3 months") 
  ,
  r4 %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:4 - 6 mths") 
  ,
  r5 %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:7-12mth") 
  ,
  r6 %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="# Manpower_by_rookie_GENLION:13+mth") 
  ,
  new_recuited_ag %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="Recruit_by_designation:AG") 
  ,
   new_recuited_al %>% 
    dplyr::group_by(territory = sprintf("GEN Lion %s", TERRITORY), time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi="Recruit_AL") 
  )
}


# Manpower ----------------------------------------------------------------
Genlion <- function(bssdt, finaldt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
    "
    SELECT AGENTCD as AGENT_CODE FROM RAWDATA_Genlion_Report WHERE BUSSINESSDATE='%s' AND FINALDATE='%s' AND (XEPHANG IS NOT NULL AND XEPHANG != '')
    "
  result <- dbSendQuery(my_database$con, sprintf(SQL, bssdt, finaldt))
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data
}

Manpower <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  mp = rbind(
    mp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
      dplyr::summarise(value=n()) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(value=n()) %>% 
      dplyr::ungroup(.)
  )
  
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  mp[mp$SEG=='GENLION',]$IDX = 1
  mp[mp$SEG=='GENLION',]$kpi = '# Manpower_by_rookie_mdrt:MDRT'
  mp[mp$SEG=='Rookie in month',]$IDX = 2
  mp[mp$SEG=='Rookie in month',]$kpi = '# Manpower_by_rookie_mdrt:Rookie in month'
  mp[mp$SEG=='Rookie last month',]$IDX = 3
  mp[mp$SEG=='Rookie last month',]$kpi = '# Manpower_by_rookie_mdrt:Rookie last month'
  mp[mp$SEG=='2-3 months',]$IDX = 4
  mp[mp$SEG=='2-3 months',]$kpi = '# Manpower_by_rookie_mdrt:2-3 months'
  mp[mp$SEG=='4-6 months',]$IDX = 5
  mp[mp$SEG=='4-6 months',]$kpi = '# Manpower_by_rookie_mdrt:4 - 6 mths'
  mp[mp$SEG=='7-12 months',]$IDX = 6
  mp[mp$SEG=='7-12 months',]$kpi = '# Manpower_by_rookie_mdrt:7-12mth'
  mp[mp$SEG=='13+ months',]$IDX = 7
  mp[mp$SEG=='13+ months',]$kpi = '# Manpower_by_rookie_mdrt:13+mth'
  mp[mp$SEG=='SA',]$IDX = 8
  mp[mp$SEG=='SA',]$kpi = '# Manpower_by_rookie_mdrt:SA'
  
  rbind(
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::mutate(territory=TERRITORY) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX)
  ,
  # total
  mp %>% dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, level) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Manpower_by_designation:Total')
  ) 
}
Manpower_exclude_2GA <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
       SELECT AGENTCD FROM GVL_AGENTLIST WHERE BUSSINESSDATE='%s' AND OFFICECD in (
   select OFFICECD from RAWDATA_GATotal where OFFICE_NAME in ('003 GA HCM 2', '010 GA TIỀN GIANG 1')
  )
    ",bssdt)
  result <- dbSendQuery(my_database$con, SQL)
  excluded_GA = fetch(result, encoding="utf-8")
  dbClearResult(result)
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD)
  
  # genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
  #   dplyr::mutate(SEG='GENLION') %>% 
  #   dplyr::select(AGENT_CODE, SEG) %>% 
  #   dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE) %>% 
      dplyr::filter(., !AGENT_CODE %in% excluded_GA$AGENTCD)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T) %>% 
    dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD)
  
  try((mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'), silent= T)
  
  mp = rbind(
    mp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
      dplyr::summarise(value=n()) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(value=n()) %>% 
      dplyr::ungroup(.)
  )
  
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  try((mp[mp$SEG=='GENLION',]$IDX = 1), silent= T)
  try((mp[mp$SEG=='GENLION',]$kpi = '# Manpower_by_rookie_mdrt:MDRT_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='Rookie in month',]$IDX = 2), silent= T)
  try((mp[mp$SEG=='Rookie in month',]$kpi = '# Manpower_by_rookie_mdrt:Rookie in month_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='Rookie last month',]$IDX = 3), silent= T)
  try((mp[mp$SEG=='Rookie last month',]$kpi = '# Manpower_by_rookie_mdrt:Rookie last month_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='2-3 months',]$IDX = 4), silent= T)
  try((mp[mp$SEG=='2-3 months',]$kpi = '# Manpower_by_rookie_mdrt:2-3 months_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='4-6 months',]$IDX = 5), silent= T)
  try((mp[mp$SEG=='4-6 months',]$kpi = '# Manpower_by_rookie_mdrt:4 - 6 mths_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='7-12 months',]$IDX = 6), silent= T)
  try((mp[mp$SEG=='7-12 months',]$kpi = '# Manpower_by_rookie_mdrt:7-12mth_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='13+ months',]$IDX = 7), silent= T)
  try((mp[mp$SEG=='13+ months',]$kpi = '# Manpower_by_rookie_mdrt:13+mth_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='SA',]$IDX = 8), silent= T)
  try((mp[mp$SEG=='SA',]$kpi = '# Manpower_by_rookie_mdrt:SA_excluded_2GA'), silent= T)
  
  rbind(
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::mutate(territory=TERRITORY) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX)
  ,
  # total
  mp %>% dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, level) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Manpower_by_designation:Total_excluded_2GA')
  ) 
}

Active <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::filter(ACTIVESP=='Yes') %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  mp = mp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.)
  
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  mp[mp$SEG=='GENLION',]$IDX = 1
  mp[mp$SEG=='GENLION',]$kpi = '# Active_by_rookie_mdrt:MDRT'
  mp[mp$SEG=='Rookie in month',]$IDX = 2
  mp[mp$SEG=='Rookie in month',]$kpi = '# Active_by_rookie_mdrt:Rookie in month'
  mp[mp$SEG=='Rookie last month',]$IDX = 3
  mp[mp$SEG=='Rookie last month',]$kpi = '# Active_by_rookie_mdrt:Rookie last month'
  mp[mp$SEG=='2-3 months',]$IDX = 4
  mp[mp$SEG=='2-3 months',]$kpi = '# Active_by_rookie_mdrt:2-3 months'
  mp[mp$SEG=='4-6 months',]$IDX = 5
  mp[mp$SEG=='4-6 months',]$kpi = '# Active_by_rookie_mdrt:4 - 6 mths'
  mp[mp$SEG=='7-12 months',]$IDX = 6
  mp[mp$SEG=='7-12 months',]$kpi = '# Active_by_rookie_mdrt:7-12mth'
  mp[mp$SEG=='13+ months',]$IDX = 7
  mp[mp$SEG=='13+ months',]$kpi = '# Active_by_rookie_mdrt:13+mth'
  mp[mp$SEG=='SA',]$IDX = 8
  mp[mp$SEG=='SA',]$kpi = '# Active_by_rookie_mdrt:SA'
  
  rbind(
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::mutate(territory=TERRITORY) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX)
  ,
  # total
  mp %>% dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Active_by_rookie_mdrt:Total')
  ,
  # total_exclude_sa
  mp %>% 
    dplyr::filter(SEG!='SA') %>% 
    dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Active_by_rookie_mdrt:Total (excl. SA)')
  ) %>% dplyr::mutate(level='TERRITORY')
}

Active1.1 <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  # 
  # BEFORE 201704 -> SEG BY MDRT
  # 201704 -> SEG BY GENLION
  # 
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }

  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::filter(ACTIVESP=='Yes') %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  mp = mp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.)
  
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  mp[mp$SEG=='GENLION',]$IDX = 1
  mp[mp$SEG=='GENLION',]$kpi = '# Active_by_rookie_mdrt:MDRT'
  mp[mp$SEG=='Rookie in month',]$IDX = 2
  mp[mp$SEG=='Rookie in month',]$kpi = '# Active_by_rookie_mdrt:Rookie in month'
  mp[mp$SEG=='Rookie last month',]$IDX = 3
  mp[mp$SEG=='Rookie last month',]$kpi = '# Active_by_rookie_mdrt:Rookie last month'
  mp[mp$SEG=='2-3 months',]$IDX = 4
  mp[mp$SEG=='2-3 months',]$kpi = '# Active_by_rookie_mdrt:2-3 months'
  mp[mp$SEG=='4-6 months',]$IDX = 5
  mp[mp$SEG=='4-6 months',]$kpi = '# Active_by_rookie_mdrt:4 - 6 mths'
  mp[mp$SEG=='7-12 months',]$IDX = 6
  mp[mp$SEG=='7-12 months',]$kpi = '# Active_by_rookie_mdrt:7-12mth'
  mp[mp$SEG=='13+ months',]$IDX = 7
  mp[mp$SEG=='13+ months',]$kpi = '# Active_by_rookie_mdrt:13+mth'
  mp[mp$SEG=='SA',]$IDX = 8
  mp[mp$SEG=='SA',]$kpi = '# Active_by_rookie_mdrt:SA'
  
  rbind(
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::mutate(territory=TERRITORY) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX)
  ,
  # total
  mp %>% dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Active_by_rookie_mdrt:Total')
  ,
  # total_exclude_sa
  mp %>% 
    dplyr::filter(SEG!='SA') %>% 
    dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Active_by_rookie_mdrt:Total (excl. SA)')
  ) %>% dplyr::mutate(level='TERRITORY')
}

active_individual <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>%
    dplyr::filter(ACTIVESP=='Yes') %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) 
  mp 
}
active_individual_sa_excl <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>%
    dplyr::filter(ACTIVESP=='Yes') %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) 
  sa = get_Manpower_sa(bssdt, dbfile)
  mp %>% dplyr::filter(!AGENT_CODE %in% sa$AGENT_CODE) 
}
active_individual_fullyear <- function(yyyy, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    active_individual(d, dbfile)
  })
  ) %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
}
active_individual_fullyear_sa_excl <- function(yyyy, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    active_individual_sa_excl(d, dbfile)
  })
  )%>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
}

get_manpower_individual_sa_excl <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_manpower_individual(bssdt, dbfile) 
  sa = get_Manpower_sa(bssdt, dbfile)
  mp %>% dplyr::filter(!AGENT_CODE %in% sa$AGENT_CODE)
}
get_manpower_total_fullyear <- function(yyyy, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    get_manpower_individual(d, dbfile)
  })
  ) 
  # %>%  
  #   merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
}
get_manpower_total_fullyear_sa_excl <- function(yyyy, dbfile){
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    get_manpower_individual_sa_excl(d, dbfile)
  })
  )
}
get_sa_total_fullyear <- function(yyyy, dbfile){
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    get_Manpower_sa(d, dbfile)
  })
  )
}
new_recruit_individual <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    dplyr::filter(MDIFF==0) 
  mp %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}
active_recruit_leader_individual <- function(bssdt, dbfile){
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) 
  mp %>% 
    # filter agent whose joining dates in the month of the business date
    dplyr::filter(
      substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
    ) %>% 
    # chỉ đếm những leader nào tuyển AG mới, còn leader tuyển um, us... thì không đếm
    # dplyr::filter(AGENT_DESIGNATION=='AG') %>% 
    # tuy nhiên theo Châu Huỳnh thì trường hợp leader tuyển leader vẫn đếm luôn nên không lọc điều kiện này
    convert_string_to_date('BUSSINESSDATE', "%Y-%m-%d") %>% 
    # tìm các leader có tuyển dụng trong tháng
    group_by(BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m'), SUPERVISOR_CODE) %>% # group data by month
    summarise(`value` = n()) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::filter(value>=1) %>% # leader recruited at least 1 agent is counted as active recruit leader
    dplyr::filter(!is.na(SUPERVISOR_CODE) & SUPERVISOR_CODE != '') %>% 
    merge(
      x = .,
      y = mp %>% dplyr::select(-SUPERVISOR_CODE, -BUSSINESSDATE),
      by.x = c('SUPERVISOR_CODE'),
      by.y = c('AGENT_CODE'),
      all.x = TRUE
    )
}
new_recruit_individual_fullyear <- function(yyyy, dbfile){
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    new_recruit_individual(d, dbfile)
  })
  )
}
rookie_performance_individual_in_first_90days <- function(bssdt, number_of_studied_months, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("bssdt should be a Date type")
    stop()
  }
  y <- lubridate::year(bssdt)
  m <- lubridate::month(bssdt)
  studied_months = seq(1:number_of_studied_months)-1
  # apply-like function that returns a data frame
  bssdts <- do.call(rbind,lapply(studied_months, function(d) {
    # strftime(get_last_day_of_month_x(y, m, d), '%Y-%m-%d')
    get_last_day_of_month_x_v1(y, m, d)
  })
  )
  mp = get_Manpower(bssdt, T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::filter(strftime(JOINING_DATE, '%Y-%m') %in% strftime(as.Date(bssdts$time_view), '%Y-%m'))
  
  # kpitotal_case <- do.call(rbind,lapply(bssdts, function(d) {
  #   print(strftime((as.Date(d)), '%Y-%m-%d'))
  #   get_kpitotal_policies_first_issued(
  #     strftime((as.Date(d)), '%Y'), 
  #     strftime((as.Date(d)), '%m'), 
  #     dbfile)
  # })
  # )
  # 
  
  kpitotal_case <- do.call(rbind,apply(bssdts, 1, function(d) {
    
    get_kpitotal_policies_first_issued(
      strftime(d['time_view'], '%Y'), 
      strftime(d['time_view'], '%m'), 
      dbfile) %>% dplyr::mutate(M=d['M'])
    
  })
  )
  
  bssdts = bssdts %>% 
    dplyr::mutate(time_view = strftime(time_view, '%Y%m'))
  
  casecount_summary <- mp %>% 
    merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
    # tư vấn có hợp đồng vào ngày thứ x sau ngày joined in date 
    dplyr::mutate(HAS_POLICY_ISSUED_IN_DAYS=HOISSDTE-JOINING_DATE+1) %>% 
    dplyr::mutate(has_policy_d15=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:15), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d30=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:30), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d60=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:60), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d90=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:90), CASE, 0)) %>% 
    dplyr::group_by(recruit_month = strftime(JOINING_DATE, '%Y%m'), REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE) %>% 
    # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
    dplyr::summarise(
      has_policy_d15=sum(has_policy_d15),
      has_policy_d30=sum(has_policy_d30),
      has_policy_d60=sum(has_policy_d60),
      has_policy_d90=sum(has_policy_d90)
    ) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate('1case/15d' = ifelse(has_policy_d15 >= 1, 1, 0)) %>% 
    dplyr::mutate('1case/30d' = ifelse(has_policy_d30 >= 1, 1, 0)) %>% 
    dplyr::mutate('3case/60d' = ifelse(has_policy_d60 >= 3, 1, 0)) %>% 
    dplyr::mutate('5case/90d' = ifelse(has_policy_d90 >= 5, 1, 0)) %>% 
    dplyr::group_by(time_view=recruit_month, REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE) %>% 
    dplyr::summarise(
      `1case/15d`=sum(`1case/15d`),
      `1case/30d`=sum(`1case/30d`),
      `3case/60d`=sum(`3case/60d`),
      `5case/90d`=sum(`5case/90d`)
    ) %>% 
    dplyr::ungroup() %>% 
    merge(
      x = .,
      y = bssdts,
      by.x = 'time_view',
      by.y = 'time_view',
      all.x = T
    )
  (tidyr::gather(casecount_summary, SEG, CASE, -time_view, -M, -REGIONCD, -ZONECD, -OFFICECD, -TEAMCD, -UNITCD, -BRANCHCD, -AGENT_CODE))
}

rookie_performance_individual_in_first_90days_fullyear <- function(yyyy, dbfile) {
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    rookie_performance_individual_in_first_90days_v2(d, dbfile)
  })
  )
}
rookie_agent_numberofday_joined_first90days_fullyear <- function(yyyy, dbfile) {
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    rookie_agent_numberofday_joined_first90days(d, dbfile) 
  })
  )
}
# rookie_performance_individual_in_first_90days_v1 delete ----
rookie_performance_individual_in_first_90days_v1 <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("bssdt should be a Date type")
    stop()
  }
  m1 = get_last_day_of_m1(strftime(bssdt, "%Y"), strftime(bssdt, "%m"))
  m2 = get_last_day_of_m1(strftime(m1, "%Y"), strftime(m1, "%m"))
  m3 = get_last_day_of_m1(strftime(m2, "%Y"), strftime(m1, "%m"))
  bssdts <- c(bssdt, m1, m2, m3)
  mp = new_recruit_individual(bssdt, dbfile) %>% 
    dplyr::mutate(JOINED_DAY=BUSSINESSDATE-JOINING_DATE+1) 
  
  day15 = mp %>% dplyr::filter(JOINED_DAY <= 15)
  day30 = mp %>% dplyr::filter(JOINED_DAY <= 30)
  day60 = mp %>% dplyr::filter(JOINED_DAY <= 60)
  day90 = mp %>% dplyr::filter(JOINED_DAY <= 90)
  
  kpitotal_case <- do.call(rbind,lapply(bssdts, function(d) {
    get_kpitotal_policies_first_issued(
      strftime(d, '%Y'), 
      strftime(d, '%m'), 
      dbfile)
    
  })
  )
  
  casecount_summary = rbind(
    day15 %>% 
      merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
      dplyr::mutate(casetype='has_policy_d15') %>% 
      dplyr::group_by(time_view = strftime(BUSSINESSDATE, '%Y%m'), REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE, casetype) %>% 
      # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
      dplyr::summarise(
        total_case=sum(CASE)
      ) %>% 
      dplyr::filter(total_case >= 1) %>% 
      dplyr::mutate(SEG= '1case/15d',NUMBER_OF_CASE_MEET_CRITERIA=1) %>% 
      dplyr::ungroup()
    ,
    day30 %>% 
      merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
      dplyr::mutate(casetype='has_policy_d30') %>% 
      dplyr::group_by(time_view = strftime(BUSSINESSDATE, '%Y%m'), REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE, casetype) %>% 
      # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
      dplyr::summarise(
        total_case=sum(CASE)
      ) %>% 
      dplyr::filter(total_case >= 1) %>% 
      dplyr::mutate(SEG= '1case/30d',NUMBER_OF_CASE_MEET_CRITERIA=1) %>% 
      dplyr::ungroup()
    ,
    day60 %>% 
      merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
      dplyr::mutate(casetype='has_policy_d60') %>% 
      dplyr::group_by(time_view = strftime(BUSSINESSDATE, '%Y%m'), REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE, casetype) %>% 
      # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
      dplyr::summarise(
        total_case=sum(CASE)
      ) %>% 
      dplyr::filter(total_case >= 3) %>% 
      dplyr::mutate(SEG= '3case/60d',NUMBER_OF_CASE_MEET_CRITERIA=1) %>% 
      dplyr::ungroup()
    ,
    day90 %>% 
      merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
      dplyr::mutate(casetype='has_policy_d90') %>% 
      dplyr::group_by(time_view = strftime(BUSSINESSDATE, '%Y%m'), REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE, casetype) %>% 
      # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
      dplyr::summarise(
        total_case=sum(CASE)
      ) %>% 
      dplyr::filter(total_case >= 5) %>% 
      dplyr::mutate(SEG= '5case/90d',NUMBER_OF_CASE_MEET_CRITERIA=1) %>% 
      dplyr::ungroup()
  ) %>% 
    dplyr::select(-casetype, -total_case) 
  casecount_summary
}
rookie_performance_individual_in_first_90days_v2 <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("bssdt should be a Date type")
    stop()
  }
  
  m1 = get_last_day_of_next_m1(strftime(bssdt, "%Y"), strftime(bssdt, "%m"))
  m2 = get_last_day_of_next_m2(strftime(bssdt, "%Y"), strftime(bssdt, "%m"))
  m3 = get_last_day_of_next_m3(strftime(bssdt, "%Y"), strftime(bssdt, "%m"))
  m4 = get_last_day_of_next_m4(strftime(bssdt, "%Y"), strftime(bssdt, "%m"))
  bssdts <- c(bssdt, m1, m2, m3, m4)
  
  mp = new_recruit_individual(bssdt, dbfile) %>% 
    dplyr::mutate(JOINED_DAY=BUSSINESSDATE-JOINING_DATE+1) 
  
  kpitotal_case <- do.call(rbind,lapply(bssdts, function(d) {
    get_kpitotal_policies_first_issued(
      strftime(d, '%Y'), 
      strftime(d, '%m'), 
      dbfile)
    
  })
  )
  
  casecount_summary <- mp %>% 
    merge(x=., y=kpitotal_case, by.x=c('AGENT_CODE'), by.y=c('AGCODE')) %>% 
    # tư vấn có hợp đồng vào ngày thứ x sau ngày joined in date 
    dplyr::mutate(HAS_POLICY_ISSUED_IN_DAYS=HOISSDTE-JOINING_DATE+1) %>% 
    dplyr::mutate(has_policy_d15=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:15), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d30=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:30), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d60=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:60), CASE, 0)) %>% 
    dplyr::mutate(has_policy_d90=ifelse(HAS_POLICY_ISSUED_IN_DAYS %in% c(0:90), CASE, 0)) %>% 
    dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE) %>% 
    # mỗi tư vấn có bao nhiêu hđ trong d1-d30, d31-d60, d61-d90
    dplyr::summarise(
      has_policy_d15=sum(has_policy_d15),
      has_policy_d30=sum(has_policy_d30),
      has_policy_d60=sum(has_policy_d60),
      has_policy_d90=sum(has_policy_d90)
    ) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate('1case/15d' = ifelse(has_policy_d15 >= 1, 1, 0)) %>% 
    dplyr::mutate('1case/30d' = ifelse(has_policy_d30 >= 1, 1, 0)) %>% 
    dplyr::mutate('3case/60d' = ifelse(has_policy_d60 >= 3, 1, 0)) %>% 
    dplyr::mutate('5case/90d' = ifelse(has_policy_d90 >= 5, 1, 0)) %>% 
    dplyr::group_by(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE) %>% 
    dplyr::summarise(
      `1case/15d`=sum(`1case/15d`),
      `1case/30d`=sum(`1case/30d`),
      `3case/60d`=sum(`3case/60d`),
      `5case/90d`=sum(`5case/90d`)
    ) %>% 
    dplyr::ungroup() 
  (tidyr::gather(casecount_summary, SEG, NUMBER_OF_CASE_MEET_CRITERIA, -time_view, -REGIONCD, -ZONECD, -OFFICECD, -TEAMCD, -UNITCD, -BRANCHCD, -AGENT_CODE))
}
rookie_agent_numberofday_joined_first90days <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("bssdt should be a Date type")
    stop()
  }
  mp = new_recruit_individual(bssdt, dbfile) %>% 
    dplyr::mutate(JOINED_DAY=BUSSINESSDATE-JOINING_DATE+1) 
  
  rbind(
    mp %>% dplyr::mutate(SEG= '1case/15d') ,
    mp %>% dplyr::mutate(SEG= '1case/30d') ,
    mp %>% dplyr::mutate(SEG= '3case/60d') ,
    mp %>% dplyr::mutate(SEG= '5case/90d') # dat seg chi de tinh % 90-day-agents co 5cases up
  )  %>% 
    dplyr::mutate(time_view = strftime(BUSSINESSDATE, '%Y%m'), AGENT_COUNT =1) %>% 
    dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, UNITCD, BRANCHCD, AGENT_CODE, SEG, AGENT_COUNT) 
}

Active1.1_exclude_2GA <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
       SELECT AGENTCD FROM GVL_AGENTLIST WHERE BUSSINESSDATE='%s' AND OFFICECD in (
   select OFFICECD from RAWDATA_GATotal where OFFICE_NAME in ('003 GA HCM 2', '010 GA TIỀN GIANG 1')
  )
    ",bssdt)
  result <- dbSendQuery(my_database$con, SQL)
  excluded_GA = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # 
  # BEFORE 201704 -> SEG BY MDRT
  # 201704 -> SEG BY GENLION
  # 
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD)
  
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE) %>% 
      dplyr::filter(., !AGENT_CODE %in% excluded_GA$AGENTCD)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }

  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::filter(ACTIVESP=='Yes') %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T) %>% 
    dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  mp = mp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.)
  
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  try((mp[mp$SEG=='GENLION',]$IDX = 1), silent= T)
  try((mp[mp$SEG=='GENLION',]$kpi = '# Active_by_rookie_mdrt:MDRT_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='Rookie in month',]$IDX = 2), silent= T)
  try((mp[mp$SEG=='Rookie in month',]$kpi = '# Active_by_rookie_mdrt:Rookie in month_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='Rookie last month',]$IDX = 3), silent= T)
  try((mp[mp$SEG=='Rookie last month',]$kpi = '# Active_by_rookie_mdrt:Rookie last month_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='2-3 months',]$IDX = 4), silent= T)
  try((mp[mp$SEG=='2-3 months',]$kpi = '# Active_by_rookie_mdrt:2-3 months_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='4-6 months',]$IDX = 5), silent= T)
  try((mp[mp$SEG=='4-6 months',]$kpi = '# Active_by_rookie_mdrt:4 - 6 mths_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='7-12 months',]$IDX = 6), silent= T)
  try((mp[mp$SEG=='7-12 months',]$kpi = '# Active_by_rookie_mdrt:7-12mth_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='13+ months',]$IDX = 7), silent= T)
  try((mp[mp$SEG=='13+ months',]$kpi = '# Active_by_rookie_mdrt:13+mth_excluded_2GA'), silent= T)
  try((mp[mp$SEG=='SA',]$IDX = 8), silent= T)
  try((mp[mp$SEG=='SA',]$kpi = '# Active_by_rookie_mdrt:SA_excluded_2GA'), silent= T)
  
  rbind(
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::mutate(territory=TERRITORY) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX)
  ,
  # total
  mp %>% dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Active_by_rookie_mdrt:Total_excluded_2GA')
  ,
  # total_exclude_sa
  mp %>% 
    dplyr::filter(SEG!='SA') %>% 
    dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Active_by_rookie_mdrt:Total (excl. SA)_excluded_2GA')
  ) %>% dplyr::mutate(level='TERRITORY')
}


Activity_Ratio <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  
  
  mp = rbind(
    
    mp %>% 
    dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
    dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
    dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
    # dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, REGION_NAME, ZONE_NAME, TEAM_NAME) %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(AR=ACT*2/(MS+ME))
  ,
  # ar TERRITORY
  mp %>% 
    dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
    dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
    dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
    # dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, REGION_NAME, ZONE_NAME, TEAM_NAME) %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
    dplyr::mutate(SEG='TOTAL')
  ,
  # ar region EXCLUDE SA
  mp %>% 
    dplyr::filter(SEG!='SA') %>% 
    dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
    dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
    dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
    # dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, REGION_NAME, ZONE_NAME, TEAM_NAME) %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
    dplyr::mutate(SEG='TOTAL_EXCL_SA')
  ) %>% dplyr::mutate(level='TERRITORY')
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  mp[mp$SEG=='GENLION',]$IDX = 1
  mp[mp$SEG=='GENLION',]$kpi = 'Activity Ratio_by_rookie_mdrt:MDRT'
  mp[mp$SEG=='Rookie in month',]$IDX = 2
  mp[mp$SEG=='Rookie in month',]$kpi = 'Activity Ratio_by_rookie_mdrt:Rookie in month'
  mp[mp$SEG=='Rookie last month',]$IDX = 3
  mp[mp$SEG=='Rookie last month',]$kpi = 'Activity Ratio_by_rookie_mdrt:Rookie last month'
  mp[mp$SEG=='2-3 months',]$IDX = 4
  mp[mp$SEG=='2-3 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:2-3 months'
  mp[mp$SEG=='4-6 months',]$IDX = 5
  mp[mp$SEG=='4-6 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:4 - 6 mths'
  mp[mp$SEG=='7-12 months',]$IDX = 6
  mp[mp$SEG=='7-12 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:7-12mth'
  mp[mp$SEG=='13+ months',]$IDX = 7
  mp[mp$SEG=='13+ months',]$kpi = 'Activity Ratio_by_rookie_mdrt:13+mth'
  mp[mp$SEG=='SA',]$IDX = 8
  mp[mp$SEG=='SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:SA'
  mp[mp$SEG=='TOTAL_EXCL_SA',]$IDX = 9
  mp[mp$SEG=='TOTAL_EXCL_SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total_Excl_SA'
  mp[mp$SEG=='TOTAL',]$IDX = 10
  mp[mp$SEG=='TOTAL',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total'
  
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=AR) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX, -SEG, -MS, -ME, -ACT, -AR) 
    
}

# Activity_Ratio_1.1 về cơ bản giống với Activity_Ratio, có add thêm group by các level region, zone, team
Activity_Ratio_1.1 <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  
  
  mp = rbind(
    # group by region
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY=REGIONCD, SEG) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% dplyr::mutate(level='REGION')
    ,
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY=REGIONCD) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='REGION')
    ,
    # exclude sa
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY=REGIONCD) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='REGION')
    ,
    # group by territory
    mp %>% 
    dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
    dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
    dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(AR=ACT*2/(MS+ME)) %>% dplyr::mutate(level='TERRITORY')
    ,
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='TERRITORY')
    ,
    # group by territory excludes SA
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='TERRITORY')
    ,
    # group by country
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% dplyr::mutate(level='COUNTRY')
    ,
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY') %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='COUNTRY')
    ,
    # group by territory excludes SA
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY') %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='COUNTRY')
  ) 
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  mp[mp$SEG=='GENLION',]$IDX = 1
  mp[mp$SEG=='GENLION',]$kpi = 'Activity Ratio_by_rookie_mdrt:MDRT'
  mp[mp$SEG=='Rookie in month',]$IDX = 2
  mp[mp$SEG=='Rookie in month',]$kpi = 'Activity Ratio_by_rookie_mdrt:Rookie in month'
  mp[mp$SEG=='Rookie last month',]$IDX = 3
  mp[mp$SEG=='Rookie last month',]$kpi = 'Activity Ratio_by_rookie_mdrt:Rookie last month'
  mp[mp$SEG=='2-3 months',]$IDX = 4
  mp[mp$SEG=='2-3 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:2-3 months'
  mp[mp$SEG=='4-6 months',]$IDX = 5
  mp[mp$SEG=='4-6 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:4 - 6 mths'
  mp[mp$SEG=='7-12 months',]$IDX = 6
  mp[mp$SEG=='7-12 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:7-12mth'
  mp[mp$SEG=='13+ months',]$IDX = 7
  mp[mp$SEG=='13+ months',]$kpi = 'Activity Ratio_by_rookie_mdrt:13+mth'
  mp[mp$SEG=='SA',]$IDX = 8
  mp[mp$SEG=='SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:SA'
  mp[mp$SEG=='TOTAL_EXCL_SA',]$IDX = 9
  mp[mp$SEG=='TOTAL_EXCL_SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total_Excl_SA'
  mp[mp$SEG=='TOTAL',]$IDX = 10
  mp[mp$SEG=='TOTAL',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total'
  
  mp %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=AR) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -IDX, -SEG, -MS, -ME, -ACT, -AR) 
    
}

Activity_Ratio_1.2 <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  #
  # hien tai function nay cho cung ket qua voi phien ban 1.1
  # dang can bo sung phan ytd cho activity ratio
  # bang cach get man power full year
  #
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  
  
  mp = rbind(
    # group by region
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=REGIONCD, SEG) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% dplyr::mutate(level='REGION')
    ,
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=REGIONCD) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='REGION')
    ,
    # exclude sa
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=REGIONCD) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='REGION')
    ,
    # group by territory
    mp %>% 
    dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
    dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
    dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
    dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, SEG) %>% 
    dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(AR=ACT*2/(MS+ME)) %>% dplyr::mutate(level='TERRITORY')
    ,
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='TERRITORY')
    ,
    # group by territory excludes SA
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='TERRITORY')
    ,
    # group by country
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory='COUNTRY', SEG) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% dplyr::mutate(level='COUNTRY')
    ,
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory='COUNTRY') %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='COUNTRY')
    , # country ytd
    # group by territory excludes SA
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory='COUNTRY') %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='COUNTRY')
  ) 
  
  # create index
  mp['IDX']=0
  mp['kpi']=0
  mp[mp$SEG=='GENLION',]$IDX = 1
  mp[mp$SEG=='GENLION',]$kpi = 'Activity Ratio_by_rookie_mdrt:MDRT'
  mp[mp$SEG=='Rookie in month',]$IDX = 2
  mp[mp$SEG=='Rookie in month',]$kpi = 'Activity Ratio_by_rookie_mdrt:Rookie in month'
  mp[mp$SEG=='Rookie last month',]$IDX = 3
  mp[mp$SEG=='Rookie last month',]$kpi = 'Activity Ratio_by_rookie_mdrt:Rookie last month'
  mp[mp$SEG=='2-3 months',]$IDX = 4
  mp[mp$SEG=='2-3 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:2-3 months'
  mp[mp$SEG=='4-6 months',]$IDX = 5
  mp[mp$SEG=='4-6 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:4 - 6 mths'
  mp[mp$SEG=='7-12 months',]$IDX = 6
  mp[mp$SEG=='7-12 months',]$kpi = 'Activity Ratio_by_rookie_mdrt:7-12mth'
  mp[mp$SEG=='13+ months',]$IDX = 7
  mp[mp$SEG=='13+ months',]$kpi = 'Activity Ratio_by_rookie_mdrt:13+mth'
  mp[mp$SEG=='SA',]$IDX = 8
  mp[mp$SEG=='SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:SA'
  mp[mp$SEG=='TOTAL_EXCL_SA',]$IDX = 9
  mp[mp$SEG=='TOTAL_EXCL_SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total_Excl_SA'
  mp[mp$SEG=='TOTAL',]$IDX = 10
  mp[mp$SEG=='TOTAL',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total'
  
  mp %>% 
    dplyr::mutate(value=AR) %>% 
    dplyr::arrange(territory, IDX) %>% 
    dplyr::select(-SEG, -IDX, -SEG, -MS, -ME, -ACT, -AR) 
    
}
Activity_Ratio_1.3_TOTAL <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  # --------------------------------------------#
  #                                             #
  # Châu yều cầu the activity ratio non SA      #
  #                                             #
  # --------------------------------------------#
  #
  # hien tai function nay cho cung ket qua voi phien ban 1.1
  # dang can bo sung phan ytd cho activity ratio
  # bang cach get man power full year
  #
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  
  
  
  mp = rbind(
    # group by territory
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='TERRITORY')
    ,
    # group by territory excludes SA
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='TERRITORY')
    ,
    # group by country
    mp %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory='COUNTRY') %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL') %>% dplyr::mutate(level='COUNTRY')
    , # country ytd
    # group by territory excludes SA
    mp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::mutate(MS=ifelse(MONTHSTART=='Yes', 1, 0)) %>% 
      dplyr::mutate(ME=ifelse(MONTHEND=='Yes', 1, 0)) %>% 
      dplyr::mutate(ACT=ifelse(ACTIVESP=='Yes', 1, 0)) %>% 
      dplyr::group_by(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory='COUNTRY') %>% 
      dplyr::summarise(MS=sum(MS), ME=sum(ME), ACT=sum(ACT)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(AR=ACT*2/(MS+ME)) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA') %>% dplyr::mutate(level='COUNTRY')
  ) 
  
  # create index
  mp['kpi']=0
  mp[mp$SEG=='TOTAL_EXCL_SA',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total_Excl_SA'
  mp[mp$SEG=='TOTAL',]$kpi = 'Activity Ratio_by_rookie_mdrt:Total'
  
  mp %>% 
    dplyr::mutate(value=AR) %>% 
    dplyr::select(-SEG, -MS, -ME, -ACT, -AR) 
    
}

Ending_MP <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  # genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
  #   dplyr::mutate(SEG='GENLION') %>% 
  #   dplyr::select(AGENT_CODE, SEG) %>% 
  #   dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=sa, by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='AG',]$SEG='AG'
  mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='US',]$SEG='US'
  mp[is.na(mp$SEG) & mp$STAFFCD=='SUM',]$SEG='SUM'
  mp[is.na(mp$SEG) & mp$STAFFCD=='SBM',]$SEG='SBM'
  mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='UM',]$SEG='UM'
  mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='BM',]$SEG='BM'
  
  mp = mp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.)
  
  re = rbind(
    mp
    ,
    mp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      dplyr::filter(SEG != 'SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_SA_EXCL')
  ) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::mutate(level='TERRITORY')
  
  re['kpi']=''
  re[re$SEG=='AG',]$kpi = '# Manpower_by_designation:AG'
  re[re$SEG=='US',]$kpi = '# Manpower_by_designation:US'
  re[re$SEG=='UM',]$kpi = '# Manpower_by_designation:UM'
  re[re$SEG=='SUM',]$kpi = '# Manpower_by_designation:SUM'
  re[re$SEG=='BM',]$kpi = '# Manpower_by_designation:BM'
  re[re$SEG=='SBM',]$kpi = '# Manpower_by_designation:SBM'
  re[re$SEG=='SA',]$kpi = '# Manpower_by_designation:SA'
  re[re$SEG=='TOTAL',]$kpi = '# Manpower_by_designation:Total'
  re[re$SEG=='TOTAL_SA_EXCL',]$kpi = '# Manpower_by_designation:Total (excl. SA)'
  
  re %>% dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG) 
}

Recruitment <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    dplyr::filter(MDIFF==0) %>% 
    dplyr::mutate(SEG=NA)
  
  try((mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='AG',]$SEG='AG'), silent=T)
  try((mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='US',]$SEG='US'), silent=T)
  try((mp[is.na(mp$SEG) & mp$STAFFCD=='SUM',]$SEG='SUM'), silent=T)
  try((mp[is.na(mp$SEG) & mp$STAFFCD=='SBM',]$SEG='SBM'), silent=T)
  try((mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='UM',]$SEG='UM'), silent=T)
  try((mp[is.na(mp$SEG) & mp$AGENT_DESIGNATION=='BM',]$SEG='BM'), silent=T)
  
  mp = mp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.)
  
  re = rbind(
    mp
    ,
    mp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      dplyr::filter(SEG %in% c('US','UM','BM','SUM','SBM')) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_AL')
  ) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY) %>% 
    dplyr::mutate(level='TERRITORY')
  
  re['kpi']=''
  try((re[re$SEG=='AG',]$kpi = 'Recruit_by_designation:AG'), silent=T)
  try((re[re$SEG=='US',]$kpi = 'Recruit_by_designation:US'), silent=T)
  try((re[re$SEG=='UM',]$kpi = 'Recruit_by_designation:UM'), silent=T)
  try((re[re$SEG=='SUM',]$kpi = 'Recruit_by_designation:SUM'), silent=T)
  try((re[re$SEG=='BM',]$kpi = 'Recruit_by_designation:BM'), silent=T)
  try((re[re$SEG=='SBM',]$kpi = 'Recruit_by_designation:SBM'), silent=T)
  try((re[re$SEG=='TOTAL',]$kpi = 'Recruit_by_designation:Total'), silent=T)
  try((re[re$SEG=='TOTAL_AL',]$kpi = 'Recruit_AL'), silent=T)
  
  rbind(
  re %>% dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG) 
  ,
  active_recruit_leader(bssdt, dbfile) %>% dplyr::mutate(level='TERRITORY')
  )
}

apply_SEG <- function(values, seg_var, seg_def, idx = F) {
  sapply(
    values
    ,
    FUN = function(val) {
      eval(parse(text = (paste(seg_var, val, sep = "="))))
      return = ''
      if (!is.na(eval(parse(text = seg_var)))) {
        for (s in seg_def) {
          if (eval(parse(text = s))) {
            return = ifelse(!idx,s, which(seg_def == s)) # index of vector element)
          }
        }
      }
      return
    }
  )
}	


SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.COUNTRY <- function(bssdt, genlion_final_dt, dbfile) {
  SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, genlion_final_dt, dbfile) %>% 
    dplyr::mutate(
      territory = 'COUNTRY', level = 'COUNTRY'
    )
}
  
  
# SEGMENT_AGENTS(as.Date('2017-09-30'), as.Date('2017-06-30'), 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db')
SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY_delete <- function(bssdt, genlion_final_dt, dbfile, index_from = 0) {
  genlion_final_dt = generate_last_day_of_q1(bssdt)
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  # if (nrow(genlion) == 0) {
  #   warning(sprintf('Genlion of business date: %s - has no data', bssdt))
  #   stop()
  # }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  if (!'SEG' %in% names(mp)) {
    mp[,'SEG'] = NA
  }
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  # create index
  try((mp['idx']=0 + index_from), silent = T)
  try((mp[mp$SEG=='GENLION',]$idx = 1 + index_from), silent = T)
  try((mp[mp$SEG=='Rookie in month',]$idx = 2 + index_from), silent = T)
  try((mp[mp$SEG=='Rookie last month',]$idx = 3 + index_from), silent = T)
  try((mp[mp$SEG=='2-3 months',]$idx = 4 + index_from), silent = T)
  try((mp[mp$SEG=='4-6 months',]$idx = 5 + index_from), silent = T)
  try((mp[mp$SEG=='7-12 months',]$idx = 6 + index_from), silent = T)
  try((mp[mp$SEG=='13+ months',]$idx = 7 + index_from), silent = T)
  try((mp[mp$SEG=='SA',]$idx = 8 + index_from), silent = T)
  
  mp %>% 
    dplyr::mutate(
      territory = TERRITORY, level = 'TERRITORY'
    ) %>% 
    dplyr::select(SEG, AGENT_CODE, territory, level, idx)
}
SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY <- function(bssdt, genlion_final_dt, dbfile, index_from = 0) {
  genlion_final_dt = generate_last_day_of_q1(bssdt)
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  # if (nrow(genlion) == 0) {
  #   warning(sprintf('Genlion of business date: %s - has no data', bssdt))
  #   stop()
  # }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  if (!'SEG' %in% names(mp)) {
    mp[,'SEG'] = NA
  }
  
  try((mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'), silent = T)
  try((mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'), silent = T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'), silent = T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'), silent = T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'), silent = T)
  try((mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'), silent = T)
  
  # create index
  mp = dplyr::mutate(mp, idx=(0 + index_from))
  # try((mp['idx']=0 + index_from), silent = T)
  try((mp[mp$SEG=='GENLION',]$idx = 1 + index_from), silent = T)
  try((mp[mp$SEG=='Rookie in month',]$idx = 2 + index_from), silent = T)
  try((mp[mp$SEG=='Rookie last month',]$idx = 3 + index_from), silent = T)
  try((mp[mp$SEG=='2-3 months',]$idx = 4 + index_from), silent = T)
  try((mp[mp$SEG=='4-6 months',]$idx = 5 + index_from), silent = T)
  try((mp[mp$SEG=='7-12 months',]$idx = 6 + index_from), silent = T)
  try((mp[mp$SEG=='13+ months',]$idx = 7 + index_from), silent = T)
  try((mp[mp$SEG=='SA',]$idx = 8 + index_from), silent = T)
  
  mp %>% 
    dplyr::mutate(
      territory = TERRITORY, level = 'TERRITORY'
    ) %>% 
    dplyr::select(SEG, AGENT_CODE, territory, level, idx)
}

# delete ----
# SEGMENT_UM_SUM_BM_SBM <- function(bssdt, dbfile) {
#   if(!inherits(bssdt, "Date")) {
#     message("date parameter should be a Date type")
#     stop()
#   }
#   mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
#     dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
#     dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
#     dplyr::mutate(MDIFF = as.integer(round((
#       as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
#     ) * 12))) 
#   my_database <- src_sqlite(dbfile, create = TRUE)
#   SQL <- "select ul.AGENT_CODE, ul.UNIT_CODE from RAWDATA_UnitList ul"
#   result <- dbSendQuery(my_database$con, SQL)
#   unitlist = fetch(result, encoding="utf-8")
#   dbClearResult(result)
#   mp %>% merge(
#     x = .
#     ,
#     y = unitlist
#     ,
#     by.x = 'AGENT_CODE'
#     ,
#     by.y = 'AGENT_CODE'
#   ) %>% 
#     dplyr::mutate(SEG = ifelse(!is.na(STAFFCD) & STAFFCD != '', STAFFCD, AGENT_DESIGNATION)) %>% 
#     dplyr::mutate(time_view =  as.Date(as.character(BUSSINESSDATE), '%Y-%m-%d')) %>% # convert to date
#     dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-")) %>% 
#     dplyr::select(time_view, UNIT_CODE, SEG)
#}

SEGMENT_AGENTS_IN_GAHCM2_GATIENGIANG1_OTHERS <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) 
  
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- "select OFFICECD, OFFICE_NAME from RAWDATA_GATotal where OFFICE_NAME in ('003 GA HCM 2', '010 GA TIỀN GIANG 1')"
  result <- dbSendQuery(my_database$con, SQL)
  GA_info = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  re = mp %>% merge(
    x = .
    ,
    y = GA_info
    ,
    by.x = 'OFFICECD'
    ,
    by.y = 'OFFICECD'
    ,
    all.x = TRUE
  ) %>% 
    dplyr::mutate(
      territory = TERRITORY, level = 'TERRITORY', 
      SEG = ifelse(is.na(OFFICE_NAME), 'OTHERS', OFFICECD),
      SEG_DESC = ifelse(is.na(OFFICE_NAME), 'OTHERS', OFFICE_NAME)
      ) %>% 
    dplyr::select(SEG, AGENT_CODE, territory, level)
  re
}

APE <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(AGCODE, APE)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(AGCODE, ULTP)
  
  mp_ape = mp %>% 
    merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
  mp_ultp = mp %>% 
    merge(x=., y=ultp, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(APE=sum(ULTP)/10^6) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(SEG='', kpi='APE_by_rookie_mdrt:SP 100%')
  
  # create index
  mp_ape['IDX']=0
  mp_ape[mp_ape$SEG=='GENLION',]$IDX = 1
  mp_ape[mp_ape$SEG=='Rookie in month',]$IDX = 2
  mp_ape[mp_ape$SEG=='Rookie last month',]$IDX = 3
  mp_ape[mp_ape$SEG=='2-3 months',]$IDX = 4
  mp_ape[mp_ape$SEG=='4-6 months',]$IDX = 5
  mp_ape[mp_ape$SEG=='7-12 months',]$IDX = 6
  mp_ape[mp_ape$SEG=='13+ months',]$IDX = 7
  mp_ape[mp_ape$SEG=='SA',]$IDX = 8
  
  # CREATE KPI
  mp_ape['kpi']=''
  mp_ape[mp_ape$SEG=='GENLION',]$kpi = 'APE_by_rookie_mdrt:MDRT'
  mp_ape[mp_ape$SEG=='Rookie in month',]$kpi = 'APE_by_rookie_mdrt:Rookie in month'
  mp_ape[mp_ape$SEG=='Rookie last month',]$kpi = 'APE_by_rookie_mdrt:Rookie last month'
  mp_ape[mp_ape$SEG=='2-3 months',]$kpi = 'APE_by_rookie_mdrt:2-3 months'
  mp_ape[mp_ape$SEG=='4-6 months',]$kpi = 'APE_by_rookie_mdrt:4 - 6 mths'
  mp_ape[mp_ape$SEG=='7-12 months',]$kpi = 'APE_by_rookie_mdrt:7-12mth'
  mp_ape[mp_ape$SEG=='13+ months',]$kpi = 'APE_by_rookie_mdrt:13+mth'
  mp_ape[mp_ape$SEG=='SA',]$kpi = 'APE_by_rookie_mdrt:SA'
  
  ape = rbind(
    mp_ape
    , 
    mp_ultp %>% dplyr::mutate(IDX=9)
    ,
    # 
    # sum MDRT/ GEN Lion (from Apr '17)
    # Rookie in month
    # Rookie last month
    # 2-3 months
    # 4 - 6 mths
    # 7-12mth
    # 13+mth
    # SA
    #
    mp_ape %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(APE=sum(APE)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='APE_by_rookie_mdrt:Total', SEG='', IDX=10)
    ,
    # sum ape + 10%ultp
    mp_ultp %>% 
      dplyr::mutate(APE=APE*0.1) %>% 
      rbind(., mp_ape[,-grep('IDX', names(mp_ape))]) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(APE=sum(APE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='APE_total_mdrt_rookie_sa_10%sp', SEG='', IDX=11)
    
  )
  
  ape %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=APE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -APE, -IDX) %>% 
    dplyr::mutate(level='TERRITORY')
}
APE1.1 <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  mp = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, genlion_final_dt, dbfile)
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(time_view, AGCODE, APE)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(time_view, AGCODE, ULTP)
  
  mp_ape = mp %>% 
    merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(time_view, territory, SEG, level='TERRITORY') %>% 
    dplyr::summarise(APE=sum(APE)) %>% 
    dplyr::ungroup(.)
  mp_ultp = mp %>% 
    merge(x=., y=ultp, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
    dplyr::summarise(APE=sum(ULTP)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(SEG='', kpi='APE_by_rookie_mdrt:SP 100%')
  
  # CREATE KPI
  mp_ape['kpi']=''
  mp_ape[mp_ape$SEG=='GENLION',]$kpi = 'APE_by_rookie_mdrt:MDRT'
  mp_ape[mp_ape$SEG=='Rookie in month',]$kpi = 'APE_by_rookie_mdrt:Rookie in month'
  mp_ape[mp_ape$SEG=='Rookie last month',]$kpi = 'APE_by_rookie_mdrt:Rookie last month'
  mp_ape[mp_ape$SEG=='2-3 months',]$kpi = 'APE_by_rookie_mdrt:2-3 months'
  mp_ape[mp_ape$SEG=='4-6 months',]$kpi = 'APE_by_rookie_mdrt:4 - 6 mths'
  mp_ape[mp_ape$SEG=='7-12 months',]$kpi = 'APE_by_rookie_mdrt:7-12mth'
  mp_ape[mp_ape$SEG=='13+ months',]$kpi = 'APE_by_rookie_mdrt:13+mth'
  mp_ape[mp_ape$SEG=='SA',]$kpi = 'APE_by_rookie_mdrt:SA'
  
  ape = rbind(
    mp_ape
    , 
    mp_ape %>% 
      dplyr::group_by(time_view, territory = 'COUNTRY', SEG, level='COUNTRY', kpi) %>% 
      dplyr::summarise(APE=sum(APE)) %>% 
      dplyr::ungroup(.)
    , 
    mp_ultp 
    ,
    mp_ultp %>% 
      dplyr::group_by(time_view, territory = 'COUNTRY', SEG, level='COUNTRY', kpi) %>% 
      dplyr::summarise(APE=sum(APE)) %>% 
      dplyr::ungroup(.)
    ,
    mp_ape %>% 
    dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
    dplyr::summarise(APE=sum(APE)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='APE_by_rookie_mdrt:Total', SEG='')
    ,
    mp_ape %>% 
    dplyr::group_by(time_view, territory = 'COUNTRY', level='COUNTRY') %>% 
    dplyr::summarise(APE=sum(APE)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='APE_by_rookie_mdrt:Total', SEG='')
    ,
    # sum ape + 10%ultp
    mp_ultp %>% 
      dplyr::mutate(APE=APE*0.1) %>% 
      rbind(., mp_ape) %>% 
      dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='APE_total_mdrt_rookie_sa_10%sp', SEG='')
    ,
    # sum ape + 10%ultp
    mp_ultp %>% 
      dplyr::mutate(APE=APE*0.1) %>% 
      rbind(., mp_ape) %>% 
      dplyr::group_by(time_view, territory = 'COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='APE_total_mdrt_rookie_sa_10%sp', SEG='')
    
  )
  
  ape %>% 
    dplyr::mutate(value=APE/10^6) %>% 
    dplyr::select(-SEG, -APE)
    
}
ape_individual_10per_topup_incl <- function(bssdt, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, APE)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, ULTP)
  
  ape = rbind(
    ape
    , 
    ultp %>% 
      dplyr::mutate(APE=ULTP*0.1) %>% 
      dplyr::select(-ULTP)
  )
  ape %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}
ape_individual_10per_topup_incl_fullyear <- function(yyyy, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  kpi = get_kpi(sprintf('%s%s', yyyy, '%'), dbfile)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, APE)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, ULTP)
  
  ape = rbind(
    ape
    , 
    ultp %>% 
      dplyr::mutate(APE=ULTP*0.1) %>% 
      dplyr::select(-ULTP)
  )
  ape %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}
ape_individual <- function(bssdt, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, APE)
  ape %>% 
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}

product_mix_individual <- function(bssdt, dbfile) {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, PRODUCTCODE, APE)
  
  products = all_product(dbfile)
  
  ape %>% 
    merge(x=., y=products, by.x='PRODUCTCODE', by.y='PRODUCTCODE') 
}

fyp_individual_10per_topup_incl <- function(bssdt, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  fyp = kpi %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, FYP)
  # fyp = dplyr::filter(kpi, kpi$FYP != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, FYP)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, ULTP)
  
  fyp = rbind(
    fyp
    , 
    ultp %>% 
      dplyr::mutate(FYP=ULTP*0.1) %>% 
      dplyr::select(-ULTP)
  )
  fyp %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}

fyp_individual_10per_topup_incl_fullyear <- function(yyyy, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  kpi = get_kpi(sprintf('%s%s', yyyy, '%'), dbfile)
  fyp = dplyr::filter(kpi, kpi$FYP != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, FYP)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, ULTP)
  
  fyp = rbind(
    fyp
    , 
    ultp %>% 
      dplyr::mutate(FYP=ULTP*0.1) %>% 
      dplyr::select(-ULTP)
  )
  fyp %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}
fyp_individual <- function(bssdt, dbfile){
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  fyp = dplyr::filter(kpi, kpi$FYP != 0) %>% dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, FYP)
  fyp
}
case_individual <- function(bssdt, dbfile){
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  baseproduct = get_base_productcd(dbfile)
  dplyr::filter(kpi, kpi$CASE != 0) %>% 
    merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
    dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, CASE)
}
case_individual_fullyear <- function(yyyy, dbfile){
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  kpi = get_kpi(sprintf('%s%s', yyyy, '%'), dbfile)
  baseproduct = get_base_productcd(dbfile)
  dplyr::filter(kpi, kpi$CASE != 0) %>% 
    merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
    dplyr::select(time_view, REGIONCD, ZONECD, OFFICECD, TEAMCD, AGCODE, CASE) %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME)
}
APE_exclude_GA_for_meeting_purpose_only <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- "
    select OFFICECD from RAWDATA_GATotal where OFFICE_NAME in ('003 GA HCM 2', '010 GA TIỀN GIANG 1')
    "
  result <- dbSendQuery(my_database$con, SQL)
  excluded_GA = fetch(result, encoding="utf-8")
  dbClearResult(result)
    
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  # genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
  #   dplyr::mutate(SEG='GENLION') %>% 
  #   dplyr::select(AGENT_CODE, SEG) %>% 
  #   dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  # if (nrow(genlion) == 0) {
  #   warning(sprintf('Genlion of business date: %s - has no data', bssdt))
  #   stop()
  # }
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile) %>% dplyr::filter(!OFFICECD %in% excluded_GA$OFFICECD)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% 
    dplyr::select(AGCODE, APE)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(AGCODE, ULTP)
  
  mp_ape = mp %>% 
    merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
  mp_ultp = mp %>% 
    merge(x=., y=ultp, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(APE=sum(ULTP)/10^6) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(SEG='', kpi='APE_by_rookie_mdrt:SP 100%_excluded_2GA')
  
  # create index
  try((mp_ape['IDX']=0), silent=T)
  try((mp_ape[mp_ape$SEG=='GENLION',]$IDX = 1), silent=T)
  try((mp_ape[mp_ape$SEG=='Rookie in month',]$IDX = 2), silent=T)
  try((mp_ape[mp_ape$SEG=='Rookie last month',]$IDX = 3), silent=T)
  try((mp_ape[mp_ape$SEG=='2-3 months',]$IDX = 4), silent=T)
  try((mp_ape[mp_ape$SEG=='4-6 months',]$IDX = 5), silent=T)
  try((mp_ape[mp_ape$SEG=='7-12 months',]$IDX = 6), silent=T)
  try((mp_ape[mp_ape$SEG=='13+ months',]$IDX = 7), silent=T)
  try((mp_ape[mp_ape$SEG=='SA',]$IDX = 8), silent=T)
  
  # CREATE KPI
  try((mp_ape['kpi']=''), silent= T)
  try((mp_ape[mp_ape$SEG=='GENLION',]$kpi = 'APE_by_rookie_mdrt:MDRT_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='Rookie in month',]$kpi = 'APE_by_rookie_mdrt:Rookie in month_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='Rookie last month',]$kpi = 'APE_by_rookie_mdrt:Rookie last month_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='2-3 months',]$kpi = 'APE_by_rookie_mdrt:2-3 months_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='4-6 months',]$kpi = 'APE_by_rookie_mdrt:4 - 6 mths_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='7-12 months',]$kpi = 'APE_by_rookie_mdrt:7-12mth_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='13+ months',]$kpi = 'APE_by_rookie_mdrt:13+mth_excluded_2GA'), silent= T)
  try((mp_ape[mp_ape$SEG=='SA',]$kpi = 'APE_by_rookie_mdrt:SA_excluded_2GA'), silent= T)
  
  ape = rbind(
    mp_ape
    , 
    mp_ultp %>% dplyr::mutate(IDX=9)
    ,
    # 
    # sum MDRT/ GEN Lion (from Apr '17)
    # Rookie in month
    # Rookie last month
    # 2-3 months
    # 4 - 6 mths
    # 7-12mth
    # 13+mth
    # SA
    #
    mp_ape %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(APE=sum(APE)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='APE_by_rookie_mdrt:Total_excluded_2GA', SEG='', IDX=10)
    ,
    # sum ape + 10%ultp
    mp_ultp %>% 
      dplyr::mutate(APE=APE*0.1) %>% 
      rbind(., mp_ape[,-grep('IDX', names(mp_ape))]) %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(APE=sum(APE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='APE_total_mdrt_rookie_sa_10%sp_excluded_2GA', SEG='', IDX=11)
    
  )
  
  ape %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=APE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -APE, -IDX) %>% 
    dplyr::mutate(level='TERRITORY')
}
APE_GAHCM2_GATIENGIANG1_OTHERS <- function(bssdt, dbfile) {
  seg = SEGMENT_AGENTS_IN_GAHCM2_GATIENGIANG1_OTHERS(bssdt, dbfile)
  ape = indi_APE_mtd_include_SP_10percent(bssdt, dbfile) %>%  dplyr::mutate(time_view = strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')), '%Y%m'))
  seg %>% merge(x = ., y = ape, by.x = 'AGENT_CODE', by.y = 'AGCODE', all.y = T) %>% 
    dplyr::group_by(time_view, territory, level, SEG) %>% 
    dplyr::summarise(value = sum(APE, na.rm = TRUE)/10^6)  %>% 
    dplyr::filter(territory == 'SOUTH') %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(kpi = ifelse(SEG == 'OTHERS', '10234_10266_OTHERS', SEG)) %>% 
    dplyr::mutate(kpi = paste('APE', kpi, sep = '_')) %>% 
    dplyr::select(-SEG) 
}
# APE_total_include_SP_10percent_lv.TERRITORY(as.Date('2017-09-30'), as.Date('2017-06-30'), 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db')
APE_total_include_SP_10percent_lv.TERRITORY <- function(bssdt, genlion_final_dt, dbfile) {
  seg = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, genlion_final_dt, dbfile)
  ape = indi_APE_mtd_include_SP_10percent(bssdt, dbfile) %>%  dplyr::mutate(time_view = strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')), '%Y%m'))
  seg %>% merge(x = ., y = ape, by.x = 'AGENT_CODE', by.y = 'AGCODE', all.y = T) %>% 
    dplyr::group_by(time_view, territory, level, SEG) %>% 
    dplyr::summarise(value = sum(APE, na.rm = TRUE)/10^6)  %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(kpi = SEG) %>% 
    dplyr::select(-SEG) 
}
APE_total_include_SP_10percent_lv.COUNTRY <- function(bssdt, genlion_final_dt, dbfile) {
  ape = indi_APE_mtd_include_SP_10percent(bssdt, dbfile) %>%  dplyr::mutate(time_view = strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')), '%Y%m'))
  ape %>% dplyr::group_by(time_view) %>% 
    dplyr::summarise(value = sum(APE, na.rm = TRUE)/10^6)  %>% 
    dplyr::ungroup() 
}

ape_country <- function(yyyy, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
    select time_view, value from kpi_segmentation where time_view like '%s' 
    and kpi = '%s' 
    and territory = 'COUNTRY' and level = 'COUNTRY'
    ", 
    paste(yyyy, '%', sep=''), 'APE_total_mdrt_rookie_sa_10%sp')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  if (nrow(data)) {
    re = rbind(
      data
      ,
      data %>% 
        dplyr::group_by(time_view=yyyy) %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
      ) %>% 
      dplyr::mutate(kpi = sprintf('APE-%s-%s', yyyy, 'COUNTRY'))
    re = re %>% tidyr::spread(time_view, value)
    try((rownames(re) <- re$kpi), silent=T)
    re
  } else {
    data
  }
}
# casesize_country <- function(yyyy, dbfile) {
#   my_database <- src_sqlite(dbfile, create = TRUE)
#   SQL <- sprintf("
#     select time_view, value from kpi_segmentation where time_view like '%s' 
#     and kpi = '%s' 
#     and territory = 'COUNTRY' and level = 'COUNTRY'
#     ", 
#     paste(yyyy, '%', sep=''), 'CaseSize_by_rookie_mdrt:Total')
#   result <- dbSendQuery(my_database$con, SQL)
#   data = fetch(result, encoding="utf-8")
#   dbClearResult(result)
#   if (nrow(data)) {
#     re = data %>% 
#       dplyr::mutate(kpi = sprintf('CASESIZE-%s-%s', yyyy, 'COUNTRY'))
#     re = re %>% tidyr::spread(time_view, value)
#     try((rownames(re) <- re$kpi), silent=T)
#     re
#   } else {
#     data
#   }
# }
ape_territory <- function(yyyy, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
    select time_view, territory, value from kpi_segmentation where time_view like '%s' 
    and kpi = '%s' 
    and level = 'TERRITORY'
    ", 
                 paste(yyyy, '%', sep=''), 'APE_total_mdrt_rookie_sa_10%sp')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  if (nrow(data)) {
    re = rbind(
      data
      ,
      data %>% 
        dplyr::group_by(time_view=yyyy, territory) %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
      
    ) %>% 
      dplyr::mutate(kpi = sprintf('APE-%s-%s', yyyy, territory))
    re = re %>% tidyr::spread(time_view, value)
    try((rownames(re) <- re$kpi), silent=T)
    re
  } else {
    data
  }
}
# casesize_territory <- function(yyyy, dbfile) {
#   my_database <- src_sqlite(dbfile, create = TRUE)
#   SQL <- sprintf("
#     select time_view, territory, value from kpi_segmentation where time_view like '%s' 
#     and kpi = '%s' 
#     and level = 'TERRITORY'
#     ", 
#                  paste(yyyy, '%', sep=''), 'CaseSize_by_rookie_mdrt:Total')
#   result <- dbSendQuery(my_database$con, SQL)
#   data = fetch(result, encoding="utf-8")
#   dbClearResult(result)
#   if (nrow(data)) {
#     re = data %>% 
#       dplyr::mutate(kpi = sprintf('CASESIZE-%s-%s', yyyy, territory))
#     re = re %>% tidyr::spread(time_view, value)
#     try((rownames(re) <- re$kpi), silent=T)
#     re
#   } else {
#     data
#   }
# }
active_territory <- function(yyyy, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
    select time_view, territory, value from kpi_segmentation where time_view like '%s' 
    and kpi = '%s' 
    and level = 'TERRITORY'
    ", 
                 paste(yyyy, '%', sep=''), '# Active_by_rookie_mdrt:Total')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  if (nrow(data)) {
    re = rbind(
      data
      ,
      # YTD
      data %>% 
        dplyr::group_by(time_view=yyyy, territory) %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
      ,
      # COUNTRY
      data %>% 
        dplyr::group_by(time_view, territory = 'COUNTRY') %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
      ,
      # YTD COUNTRY
      data %>% 
        dplyr::group_by(time_view=yyyy, territory = 'COUNTRY') %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
    ) %>% 
      dplyr::mutate(kpi = sprintf('ACTIVE-%s-%s', yyyy, territory))
    re = re %>% tidyr::spread(time_view, value)
    try((rownames(re) <- re$kpi), silent=T)
    re
  } else {
    data
  }
}
case_territory <- function(yyyy, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
    select time_view, territory, value from kpi_segmentation where time_view like '%s' 
    and kpi = '%s' 
    and level = 'TERRITORY'
    ", 
                 paste(yyyy, '%', sep=''), '# Case_by_rookie_mdrt:Total')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  if (nrow(data)) {
    re = rbind(
      data
      ,
      # YTD
      data %>% 
        dplyr::group_by(time_view=yyyy, territory) %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
      ,
      # COUNTRY
      data %>% 
        dplyr::group_by(time_view, territory = 'COUNTRY') %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
      ,
      # YTD COUNTRY
      data %>% 
        dplyr::group_by(time_view=yyyy, territory = 'COUNTRY') %>% 
        dplyr::summarise(value = sum(value)) %>% 
        dplyr::ungroup()
    ) %>% 
      dplyr::mutate(kpi = sprintf('CASE-%s-%s', yyyy, territory))
    re = re %>% tidyr::spread(time_view, value)
    try((rownames(re) <- re$kpi), silent=T)
    re
  } else {
    data
  }
}
casesize_territory <- function(yyyy, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
    select time_view, territory, value from kpi_segmentation where time_view like '%s' 
    and kpi = '%s' 
    and level = 'TERRITORY'
    ", 
                 paste(yyyy, '%', sep=''), '# Case_by_rookie_mdrt:Total')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  case = rbind(
    data
    ,
    # YTD
    data %>% 
      dplyr::group_by(time_view=yyyy, territory) %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup()
    ,
    # COUNTRY
    data %>% 
      dplyr::group_by(time_view, territory = 'COUNTRY') %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup()
    ,
    # YTD COUNTRY
    data %>% 
      dplyr::group_by(time_view=yyyy, territory = 'COUNTRY') %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup()
  ) 
  #
  # ape
  #
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
    select time_view, territory, value from kpi_segmentation where time_view like '%s' 
    and kpi = '%s' 
    and level = 'TERRITORY'
    ", 
                 paste(yyyy, '%', sep=''), 'APE_total_mdrt_rookie_sa_10%sp')
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  ape = rbind(
    data
    ,
    data %>% 
      dplyr::group_by(time_view=yyyy, territory) %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup()
    , 
    # country
    data %>% 
      dplyr::group_by(time_view, territory = 'COUNTRY') %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup()
    ,
    # country ytd
    data %>% 
      dplyr::group_by(time_view=yyyy, territory = 'COUNTRY') %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup()
    
  ) 
  
  re = ape %>% 
    merge(
      x = .,
      y = case,
      by.x = c('time_view', 'territory'),
      by.y = c('time_view', 'territory')
    ) %>% 
    dplyr::mutate(value = value.x/value.y) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) %>%  # remove columns that have sufix is ".x" or '.y' (if any)
    dplyr::mutate(kpi = sprintf('CASESIZE-%s-%s', yyyy, territory))
  re = re %>% tidyr::spread(time_view, value)
  try((rownames(re) <- re$kpi), silent=T)
  re
}
fyp_territory <- function(yyyy, dbfile) {
  #####################################################################
  # ytd fyp of country at a business date, 10% top up (sp) INCLUDED
  #####################################################################
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
                  SELECT time_view, territory, value
                  FROM kpi_segmentation
                  WHERE time_view LIKE '%s' AND kpi = '%s' 
                  AND level = 'TERRITORY'
                 ", 
                 paste(yyyy, '%', sep=''), 'FYP_by_rookie_mdrt:Total'
                 )
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  re = rbind(
    data,
    data %>% 
      dplyr::group_by(time_view, territory = 'COUNTRY') %>% 
      dplyr::summarise(value = sum(value)) %>% 
      dplyr::ungroup() 
  )%>% 
  dplyr::mutate(kpi = sprintf('FYP-%s-%s', yyyy, territory))
  re = re %>% tidyr::spread(time_view, value)
  try((rownames(re) <- re$kpi), silent=T)
  re
}

# Case_total_lv.COUNTRY <- function(bssdt, dbfile) {
#   if(!inherits(bssdt, "Date")) {
#     message ("date parameter should be a Date type")
#     stop()
#   }
#   kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile) 
#   case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
#     merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
#     dplyr::select(time_view, CASE) %>% 
#     dplyr::group_by(time_view) %>% 
#     dplyr::summarise(value = sum(CASE))
#   case
# }

Recruitement_total_lv.COUNTRY <- function(bssdt, dbfile) {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    dplyr::filter(MDIFF==0) %>% 
    dplyr::mutate(SEG=NA) %>% 
    dplyr::mutate(time_view = strftime(BUSSINESSDATE, '%Y%m'))
  mp %>% 
    dplyr::group_by(time_view) %>% 
    dplyr::summarise(value = n()) %>% 
    dplyr::ungroup()
}
Manpower_total_lv.COUNTRY <- function(bssdt, dbfile) {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% dplyr::mutate(time_view = strftime(BUSSINESSDATE, '%Y%m'))
  mp %>% 
    dplyr::group_by(time_view) %>% 
    dplyr::summarise(value = n()) %>% 
    dplyr::ungroup()
}

Manpower_GAHCM2_GATIENGIANG1_OTHERS <- function(bssdt, dbfile) {
  seg = SEGMENT_AGENTS_IN_GAHCM2_GATIENGIANG1_OTHERS(bssdt, dbfile)
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(time_view = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::select(time_view, AGENT_CODE)
  
  seg %>% merge(x = ., y = mp, by.x = 'AGENT_CODE', by.y = 'AGENT_CODE', all.y = T) %>% 
    dplyr::group_by(time_view, territory, level, SEG) %>% 
    dplyr::summarise(value = n())  %>% 
    dplyr::filter(territory == 'SOUTH') %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(kpi = ifelse(SEG == 'OTHERS', '10234_10266_OTHERS', SEG)) %>% 
    dplyr::mutate(kpi = paste('Manpower', kpi, sep = '_')) %>% 
    dplyr::select(-SEG) 
}

indi_APE_mtd_include_SP_10percent <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message("date parameter should be a Date type")
    stop()
  }
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile) 
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% dplyr::select(BUSSINESSDATE, AGCODE, APE)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% 
    dplyr::select(BUSSINESSDATE, AGCODE, ULTP) %>% 
    dplyr::mutate(APE = ULTP*10/100) %>% # include 10% sp to ape
    dplyr::select(-ULTP)
  rbind(
    ape
    , 
    ultp 
  )
}



RYP <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  ryp = dplyr::filter(kpi, kpi$RYP != 0) %>% dplyr::select(AGCODE, RYP)
  
  mp_ryp = mp %>% 
    merge(x=., y=ryp, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(RYP=sum(RYP)/10^6) %>% 
    dplyr::ungroup(.)
  
  # create index
  try((mp_ryp['IDX']=0), silent = T)
  try((mp_ryp[mp_ryp$SEG=='GENLION',]$IDX = 1), silent = T)
  try((mp_ryp[mp_ryp$SEG=='Rookie in month',]$IDX = 2), silent = T)
  try((mp_ryp[mp_ryp$SEG=='Rookie last month',]$IDX = 3), silent = T)
  try((mp_ryp[mp_ryp$SEG=='2-3 months',]$IDX = 4), silent = T)
  try((mp_ryp[mp_ryp$SEG=='4-6 months',]$IDX = 5), silent = T)
  try((mp_ryp[mp_ryp$SEG=='7-12 months',]$IDX = 6), silent = T)
  try((mp_ryp[mp_ryp$SEG=='13+ months',]$IDX = 7), silent = T)
  try((mp_ryp[mp_ryp$SEG=='SA',]$IDX = 8), silent = T)
  
  # CREATE KPI
  try((mp_ryp['kpi']=''), silent = T)
  try((mp_ryp[mp_ryp$SEG=='GENLION',]$kpi = 'RYP_by_rookie_mdrt:MDRT'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='Rookie in month',]$kpi = 'RYP_by_rookie_mdrt:Rookie in month'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='Rookie last month',]$kpi = 'RYP_by_rookie_mdrt:Rookie last month'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='2-3 months',]$kpi = 'RYP_by_rookie_mdrt:2-3 months'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='4-6 months',]$kpi = 'RYP_by_rookie_mdrt:4 - 6 mths'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='7-12 months',]$kpi = 'RYP_by_rookie_mdrt:7-12mth'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='13+ months',]$kpi = 'RYP_by_rookie_mdrt:13+mth'), silent = T)
  try((mp_ryp[mp_ryp$SEG=='SA',]$kpi = 'RYP_by_rookie_mdrt:SA'), silent = T)
  
  ryp = rbind(
    mp_ryp
    , 
    mp_ryp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(RYP=sum(RYP)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='RYP_by_rookie_mdrt:Total', SEG='', IDX=10)
    ,
    mp_ryp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
      dplyr::summarise(RYP=sum(RYP)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='RYP_by_rookie_mdrt:Total_EXCL_SA', SEG='', IDX=11)
    
  )
  
  ryp %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=RYP) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -RYP, -IDX) %>% 
    dplyr::mutate(level='TERRITORY')
}
FYP <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  # genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
  #   dplyr::mutate(SEG='GENLION') %>% 
  #   dplyr::select(AGENT_CODE, SEG) %>% 
  #   dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  # if (nrow(genlion) == 0) {
  #   warning(sprintf('Genlion of business date: %s - has no data', bssdt))
  #   stop()
  # }
  
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  fyp = dplyr::filter(kpi, kpi$FYP != 0) %>% dplyr::select(AGCODE, FYP)
  ultp = dplyr::filter(kpi, kpi$SACSTYP=='EP') %>% dplyr::select(AGCODE, ULTP)
  
  mp_fyp = mp %>% 
    merge(x=., y=fyp, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY', SEG) %>% 
    dplyr::summarise(FYP=sum(FYP)/10^6) %>% 
    dplyr::ungroup(.)
  mp_ultp = mp %>% 
    merge(x=., y=ultp, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(FYP=sum(ULTP)/10^6) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(SEG='', kpi='FYP_by_rookie_mdrt:SP 100%')
  
  # create index
  try((mp_fyp['IDX']=0), silent = T)
  try((mp_fyp[mp_fyp$SEG=='GENLION',]$IDX = 1), silent = T)
  try((mp_fyp[mp_fyp$SEG=='Rookie in month',]$IDX = 2), silent = T)
  try((mp_fyp[mp_fyp$SEG=='Rookie last month',]$IDX = 3), silent = T)
  try((mp_fyp[mp_fyp$SEG=='2-3 months',]$IDX = 4), silent = T)
  try((mp_fyp[mp_fyp$SEG=='4-6 months',]$IDX = 5), silent = T)
  try((mp_fyp[mp_fyp$SEG=='7-12 months',]$IDX = 6), silent = T)
  try((mp_fyp[mp_fyp$SEG=='13+ months',]$IDX = 7), silent = T)
  try((mp_fyp[mp_fyp$SEG=='SA',]$IDX = 8), silent = T)
  
  # CREATE KPI
  try((mp_fyp['kpi']=''), silent = T)
  try((mp_fyp[mp_fyp$SEG=='GENLION',]$kpi = 'FYP_by_rookie_mdrt:MDRT'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='Rookie in month',]$kpi = 'FYP_by_rookie_mdrt:Rookie in month'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='Rookie last month',]$kpi = 'FYP_by_rookie_mdrt:Rookie last month'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='2-3 months',]$kpi = 'FYP_by_rookie_mdrt:2-3 months'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='4-6 months',]$kpi = 'FYP_by_rookie_mdrt:4 - 6 mths'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='7-12 months',]$kpi = 'FYP_by_rookie_mdrt:7-12mth'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='13+ months',]$kpi = 'FYP_by_rookie_mdrt:13+mth'), silent = T)
  try((mp_fyp[mp_fyp$SEG=='SA',]$kpi = 'FYP_by_rookie_mdrt:SA'), silent = T)
  
  fyp = rbind(
    mp_fyp
    , 
    mp_fyp %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
    dplyr::summarise(FYP=sum(FYP)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='FYP_by_rookie_mdrt:Total', SEG='', IDX=10)
    ,
    mp_fyp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(FYP=sum(FYP)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='FYP_by_rookie_mdrt:Total_EXCL_SA', SEG='', IDX=11)
    ,
    # country
    mp_fyp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY', SEG, IDX, kpi) %>% 
      dplyr::summarise(FYP=sum(FYP)) %>% 
      dplyr::ungroup(.) 
    , 
    mp_fyp %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(FYP=sum(FYP)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='FYP_by_rookie_mdrt:Total', SEG='', IDX=10)
    ,
    mp_fyp %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(FYP=sum(FYP)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='FYP_by_rookie_mdrt:Total_EXCL_SA', SEG='', IDX=11)
    ,#TOTAL FYP INCLUDE SA + 10%SP
    rbind(
      dplyr::select(mp_fyp, -IDX, -level)
      ,
      mp_ultp %>% dplyr::mutate(FYP=0.1*FYP)
    ) %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(FYP=sum(FYP)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(kpi='FYP_total_mdrt_rookie_sa_10%sp', SEG='', IDX=12)
  )
  
  fyp %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=FYP) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -FYP, -IDX) 
}
Case <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  baseproduct = get_base_productcd(dbfile)
  
  case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(AGCODE, CASE)
  
  
  mp_case = mp %>% 
    merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG) %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.)
  
  # create index
  mp_case['IDX']=0
  mp_case[mp_case$SEG=='GENLION',]$IDX = 1
  mp_case[mp_case$SEG=='Rookie in month',]$IDX = 2
  mp_case[mp_case$SEG=='Rookie last month',]$IDX = 3
  mp_case[mp_case$SEG=='2-3 months',]$IDX = 4
  mp_case[mp_case$SEG=='4-6 months',]$IDX = 5
  mp_case[mp_case$SEG=='7-12 months',]$IDX = 6
  mp_case[mp_case$SEG=='13+ months',]$IDX = 7
  mp_case[mp_case$SEG=='SA',]$IDX = 8
  
  # CREATE KPI
  mp_case['kpi']=''
  mp_case[mp_case$SEG=='GENLION',]$kpi = '# Case_by_rookie_mdrt:MDRT'
  mp_case[mp_case$SEG=='Rookie in month',]$kpi = '# Case_by_rookie_mdrt:Rookie in month'
  mp_case[mp_case$SEG=='Rookie last month',]$kpi = '# Case_by_rookie_mdrt:Rookie last month'
  mp_case[mp_case$SEG=='2-3 months',]$kpi = '# Case_by_rookie_mdrt:2-3 months'
  mp_case[mp_case$SEG=='4-6 months',]$kpi = '# Case_by_rookie_mdrt:4 - 6 mths'
  mp_case[mp_case$SEG=='7-12 months',]$kpi = '# Case_by_rookie_mdrt:7-12mth'
  mp_case[mp_case$SEG=='13+ months',]$kpi = '# Case_by_rookie_mdrt:13+mth'
  mp_case[mp_case$SEG=='SA',]$kpi = '# Case_by_rookie_mdrt:SA'
  
  case = rbind(
    mp_case
    , 
    mp_case %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY) %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(kpi='# Case_by_rookie_mdrt:Total', SEG='', IDX=10)
  )
  
  case %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=CASE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -CASE, -IDX) %>% 
    dplyr::mutate(level='TERRITORY')
}
Rider <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower_1.2(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) 
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  baseproduct = get_base_productcd(dbfile) %>% mutate(PRODUCTTYPE='BASEPRODUCT')
  
  case = kpi %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(AGCODE, RDOCNUM, CASE, COUNTRIDERS, PRODUCTTYPE)
  
  rider_attach = rbind(
    case %>% 
      dplyr::filter(COUNTRIDERS>=1) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='rider_attach', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory='', level='COUNTRY', region='', zone='', team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=1) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='rider_attach', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TERRITORY', region='', zone='', team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=1) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='rider_attach', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='REGION', region=REGIONCD, zone='', team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=1) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='rider_attach', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='ZONE', region='', zone=ZONECD, team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=1) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='rider_attach', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TEAM', region='', zone='', team=TEAMCD, fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
  )
  
  ratio_case4riders = rbind(
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory='', level='COUNTRY',  region='', zone='', team='', fmt='0.0%') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)/sum(case$CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TERRITORY',  region='', zone='', team='', fmt='0.0%') %>% 
      dplyr::summarise(value = sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.) %>% 
      merge(
        x = .
        ,
        y = case %>% 
          merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
          dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TERRITORY',  region='', zone='', team='', fmt='0.0%') %>% 
          dplyr::summarise(value = sum(CASE, na.rm = T))
        ,
        by.x=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
        ,
        by.y=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
      ) %>% 
      dplyr::mutate(value = ifelse(value.y==0, 0, value.x/ value.y)) %>% 
      dplyr::select(-value.x, -value.y)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='REGION',  region=REGIONCD, zone='', team='', fmt='0.0%') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.) %>% 
      merge(
        x = .
        ,
        y = case %>% 
          merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
          dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='REGION',  region=REGIONCD, zone='', team='', fmt='0.0%') %>% 
          dplyr::summarise(value=sum(CASE, na.rm = T))
        ,
        by.x=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
        ,
        by.y=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
      ) %>% 
      dplyr::mutate(value = ifelse(value.y==0, 0, value.x/ value.y)) %>% 
      dplyr::select(-value.x, -value.y)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='ZONE',  region='', zone=ZONECD, team='', fmt='0.0%') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.) %>% 
      merge(
        x = .
        ,
        y = case %>% 
          merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
          dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='ZONE',  region='', zone=ZONECD, team='', fmt='0.0%') %>% 
          dplyr::summarise(value=sum(CASE, na.rm = T))
        ,
        by.x=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
        ,
        by.y=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
      ) %>% 
      dplyr::mutate(value = ifelse(value.y==0, 0, value.x/ value.y)) %>% 
      dplyr::select(-value.x, -value.y)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TEAM',  region='', zone='', team=TEAMCD, fmt='0.0%') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.) %>% 
      merge(
        x = .
        ,
        y = case %>% 
          merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
          dplyr::group_by(kpi='ratio_case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TEAM',  region='', zone='', team=TEAMCD, fmt='0.0%') %>% 
          dplyr::summarise(value=sum(CASE, na.rm = T))
        ,
        by.x=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
        ,
        by.y=c( 'kpi', 'time_view', 'territory',     'level', 'region',  'zone',  'team',   'fmt')
      ) %>% 
      dplyr::mutate(value = ifelse(value.y==0, 0, value.x/ value.y)) %>% 
      dplyr::select(-value.x, -value.y)
    
    )
  
  case4riders = rbind(
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory='', level='COUNTRY',  region='', zone='', team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TERRITORY',  region='', zone='', team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='REGION',  region=REGIONCD, zone='', team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='ZONE',  region='', zone=ZONECD, team='', fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    ,
    case %>% 
      dplyr::filter(COUNTRIDERS>=4) %>% 
      merge(x=mp, y=., by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(kpi='case4riders', time_view=strftime(BUSSINESSDATE,'%Y%m'), territory=TERRITORY, level='TEAM',  region='', zone='', team=TEAMCD, fmt='#,##0') %>% 
      dplyr::summarise(value=sum(CASE, na.rm = T)) %>% 
      dplyr::ungroup(.)
    
  )
  
  re = rbind(
    rider_attach
    ,
    ratio_case4riders
    ,
    case4riders
    )
  re[is.na(re$value),] <- 0
  re
}
Rider_sheet <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  str = get_team_structure(dbfile)
  
  te = rbind(
    kpi_segmentation_1.1(criteria = "where kpi in ('rider_attach') and level='TEAM' ") %>% mutate(idx=1)
    ,
    kpi_segmentation_1.1(criteria = "where kpi in ('ratio_case4riders') and level='TEAM' ")  %>% mutate(idx=2)
    # ,
    # kpi_segmentation_1.1(criteria = "where kpi in ('case4riders') and level='TEAM' ")  %>% mutate(idx=3)
  ) 
  te_rider_attach_ytd = te %>% 
    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
    dplyr::filter(., kpi == 'rider_attach') %>% 
    dplyr::group_by(time_view=substr(time_view, 1, 4), kpi, territory, region, zone, team, level, idx, fmt) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  te_case4riders_ytd = te %>% 
    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
    dplyr::filter(., kpi == 'case4riders') %>% 
    dplyr::group_by(time_view=substr(time_view, 1, 4), kpi, territory, region, zone, team, level, idx, fmt) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  te_ratio_case4riders_ytd = merge(
    x = te_case4riders_ytd
    ,
    y = te_rider_attach_ytd
    ,
    by.x = c('time_view', 'territory', 'region', 'zone', 'team', 'level')
    ,
    by.y = c('time_view', 'territory', 'region', 'zone', 'team', 'level')
  ) %>% 
    dplyr::mutate(kpi='ratio_case4riders', fmt="0.0%", idx=2, value=value.x/value.y) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) # remove columns that have sufix is ".x" or '.y' (if any)
  
  te = rbind(
    te, te_rider_attach_ytd, te_case4riders_ytd, te_ratio_case4riders_ytd
  ) %>% 
    merge(
      x = .
      ,
      y = unique(select(str, c('region', 'zone', 'team'))),
      ,
      by.x = c('team')
      ,
      by.y = c('team')
      ,
      all.x = TRUE
    ) %>% 
    dplyr::mutate(region = region.y, zone = zone.y) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) %>% # remove columns that have sufix is ".x" or '.y' (if any)
    dplyr::mutate(name=team, color=color_team) 
    # tidyr::spread(time_view, value) %>% 
    # dplyr::arrange(territory, idx) 
  
  zo = rbind(
    kpi_segmentation_1.1(criteria = "where kpi in ('rider_attach') and level='ZONE' ") %>% mutate(idx=1)
    ,
    kpi_segmentation_1.1(criteria = "where kpi in ('ratio_case4riders') and level='ZONE' ")  %>% mutate(idx=2)
  ) %>% 
    merge(
      x = .
      ,
      y = unique(select(str, c('region', 'zone'))),
      ,
      by.x = c('zone')
      ,
      by.y = c('zone')
      ,
      all.x = TRUE
    ) %>% 
    dplyr::mutate(region = region.y) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) %>% # remove columns that have sufix is ".x" or '.y' (if any)
    dplyr::mutate(name=zone, color=color_zone) 
    # tidyr::spread(time_view, value) %>% 
    # dplyr::arrange(territory, idx)
  
  re = rbind(
    kpi_segmentation_1.1(criteria = "where kpi in ('rider_attach') and level='REGION' ") %>% mutate(idx=1)
    ,
    kpi_segmentation_1.1(criteria = "where kpi in ('ratio_case4riders') and level='REGION' ")  %>% mutate(idx=2)
  ) %>% 
    dplyr::mutate(name=region, color=color_region)  
    # tidyr::spread(time_view, value) %>% 
    # dplyr::arrange(territory, idx)
  
  ter = rbind(
    kpi_segmentation_1.1(criteria = "where kpi in ('rider_attach') and level='TERRITORY' ") %>% mutate(idx=1)
    ,
    kpi_segmentation_1.1(criteria = "where kpi in ('ratio_case4riders') and level='TERRITORY' ")  %>% mutate(idx=2)
  ) %>% 
    dplyr::mutate(name=territory, color=color_territory) 
    # tidyr::spread(time_view, value) %>% 
    # dplyr::arrange(territory, idx)
  
  con = rbind(
    kpi_segmentation_1.1(criteria = "where kpi in ('rider_attach') and level='COUNTRY' ") %>% mutate(idx=1)
    ,
    kpi_segmentation_1.1(criteria = "where kpi in ('ratio_case4riders') and level='COUNTRY' ")  %>% mutate(idx=2)
  ) %>% 
    dplyr::mutate(name='', color=color_country) 
    # tidyr::spread(time_view, value) %>% 
    # dplyr::arrange(territory, idx)
  
  rbind(
    te %>% 
      dplyr::arrange(level, territory, region, zone, name, idx), 
    zo %>% 
      dplyr::arrange(level, territory, region, zone, name, idx), 
    re %>% 
      dplyr::arrange(level, territory, region, zone, name, idx), 
    ter %>% 
      dplyr::arrange(level, territory, region, zone, name, idx), 
    con %>% 
      dplyr::arrange(level, territory, region, zone, name, idx)
  ) %>% 
    tidyr::spread(time_view, value)  
  
}
CaseSize <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  baseproduct = get_base_productcd(dbfile)
  
  # chỉ lấy case của base product
  case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(AGCODE, CASE)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% 
    dplyr::select(AGCODE, APE)
  
  mp_case = rbind(
    
    mp %>% 
    merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    #--
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_ape = rbind(
    mp %>% 
    merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    #---
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_casesize = mp_ape %>% 
    merge(x=., y=mp_case, 
                   by.x=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level'),
                   by.y=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level')) %>% 
    dplyr::mutate(CASE_SIZE = APE/CASE)
    
  
  # create index
  mp_casesize['IDX']=0
  mp_casesize[mp_casesize$SEG=='GENLION',]$IDX = 1
  mp_casesize[mp_casesize$SEG=='Rookie in month',]$IDX = 2
  mp_casesize[mp_casesize$SEG=='Rookie last month',]$IDX = 3
  mp_casesize[mp_casesize$SEG=='2-3 months',]$IDX = 4
  mp_casesize[mp_casesize$SEG=='4-6 months',]$IDX = 5
  mp_casesize[mp_casesize$SEG=='7-12 months',]$IDX = 6
  mp_casesize[mp_casesize$SEG=='13+ months',]$IDX = 7
  mp_casesize[mp_casesize$SEG=='SA',]$IDX = 8
  mp_casesize[mp_casesize$SEG=='TOTAL_EXCL_SA',]$IDX = 9
  mp_casesize[mp_casesize$SEG=='TOTAL',]$IDX = 10
  
  # CREATE KPI
  mp_casesize['kpi']=''
  mp_casesize[mp_casesize$SEG=='GENLION',]$kpi = 'CaseSize_by_rookie_mdrt:MDRT'
  mp_casesize[mp_casesize$SEG=='Rookie in month',]$kpi = 'CaseSize_by_rookie_mdrt:Rookie in month'
  mp_casesize[mp_casesize$SEG=='Rookie last month',]$kpi = 'CaseSize_by_rookie_mdrt:Rookie last month'
  mp_casesize[mp_casesize$SEG=='2-3 months',]$kpi = 'CaseSize_by_rookie_mdrt:2-3 months'
  mp_casesize[mp_casesize$SEG=='4-6 months',]$kpi = 'CaseSize_by_rookie_mdrt:4 - 6 mths'
  mp_casesize[mp_casesize$SEG=='7-12 months',]$kpi = 'CaseSize_by_rookie_mdrt:7-12mth'
  mp_casesize[mp_casesize$SEG=='13+ months',]$kpi = 'CaseSize_by_rookie_mdrt:13+mth'
  mp_casesize[mp_casesize$SEG=='SA',]$kpi = 'CaseSize_by_rookie_mdrt:SA'
  mp_casesize[mp_casesize$SEG=='TOTAL',]$kpi = 'CaseSize_by_rookie_mdrt:Total'
  mp_casesize[mp_casesize$SEG=='TOTAL_EXCL_SA',]$kpi = 'CaseSize_by_rookie_mdrt:Total_exclude_SA'
  
  mp_casesize %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=CASE_SIZE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -CASE, -APE, -IDX, -CASE_SIZE)
    
}
CaseSize1.1 <- function(bssdt, genlion_final_dt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, genlion_final_dt, dbfile)
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  baseproduct = get_base_productcd(dbfile)
  
  # chỉ lấy case của base product
  case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(time_view, AGCODE, CASE)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% 
    dplyr::select(time_view, AGCODE, APE)
  
  mp_case = rbind(
    
    mp %>% 
    merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(time_view, territory, SEG, level='TERRITORY') %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    #--
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(time_view, territory='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(time_view, territory='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(time_view, territory='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_ape = rbind(
    mp %>% 
    merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(time_view, territory, SEG, level='TERRITORY') %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(time_view, territory, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    #---
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(time_view, territory='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(time_view, territory='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(time_view, territory='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_casesize = mp_ape %>% 
    merge(x=., y=mp_case, 
                   by.x=c('time_view','territory', 'SEG', 'level'),
                   by.y=c('time_view','territory', 'SEG', 'level')) %>% 
    dplyr::mutate(CASE_SIZE = APE/CASE)
    
  
  # create index
  mp_casesize['IDX']=0
  mp_casesize[mp_casesize$SEG=='GENLION',]$IDX = 1
  mp_casesize[mp_casesize$SEG=='Rookie in month',]$IDX = 2
  mp_casesize[mp_casesize$SEG=='Rookie last month',]$IDX = 3
  mp_casesize[mp_casesize$SEG=='2-3 months',]$IDX = 4
  mp_casesize[mp_casesize$SEG=='4-6 months',]$IDX = 5
  mp_casesize[mp_casesize$SEG=='7-12 months',]$IDX = 6
  mp_casesize[mp_casesize$SEG=='13+ months',]$IDX = 7
  mp_casesize[mp_casesize$SEG=='SA',]$IDX = 8
  mp_casesize[mp_casesize$SEG=='TOTAL_EXCL_SA',]$IDX = 9
  mp_casesize[mp_casesize$SEG=='TOTAL',]$IDX = 10
  
  # CREATE KPI
  mp_casesize['kpi']=''
  mp_casesize[mp_casesize$SEG=='GENLION',]$kpi = 'CaseSize_by_rookie_mdrt:MDRT'
  mp_casesize[mp_casesize$SEG=='Rookie in month',]$kpi = 'CaseSize_by_rookie_mdrt:Rookie in month'
  mp_casesize[mp_casesize$SEG=='Rookie last month',]$kpi = 'CaseSize_by_rookie_mdrt:Rookie last month'
  mp_casesize[mp_casesize$SEG=='2-3 months',]$kpi = 'CaseSize_by_rookie_mdrt:2-3 months'
  mp_casesize[mp_casesize$SEG=='4-6 months',]$kpi = 'CaseSize_by_rookie_mdrt:4 - 6 mths'
  mp_casesize[mp_casesize$SEG=='7-12 months',]$kpi = 'CaseSize_by_rookie_mdrt:7-12mth'
  mp_casesize[mp_casesize$SEG=='13+ months',]$kpi = 'CaseSize_by_rookie_mdrt:13+mth'
  mp_casesize[mp_casesize$SEG=='SA',]$kpi = 'CaseSize_by_rookie_mdrt:SA'
  mp_casesize[mp_casesize$SEG=='TOTAL',]$kpi = 'CaseSize_by_rookie_mdrt:Total'
  mp_casesize[mp_casesize$SEG=='TOTAL_EXCL_SA',]$kpi = 'CaseSize_by_rookie_mdrt:Total_exclude_SA'
  
  mp_casesize %>% dplyr::arrange(territory, IDX) %>% 
    dplyr::mutate(value=CASE_SIZE) %>% 
    dplyr::select(-SEG, -CASE, -APE, -IDX, -CASE_SIZE)
    
}
CaseSize_exclude_2GA <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- "
    select OFFICECD from RAWDATA_GATotal where OFFICE_NAME in ('003 GA HCM 2', '010 GA TIỀN GIANG 1')
    "
  result <- dbSendQuery(my_database$con, SQL)
  excluded_GA = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  # genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
  #   dplyr::mutate(SEG='GENLION') %>% 
  #   dplyr::select(AGENT_CODE, SEG) %>% 
  #   dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  try((mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'), silent= T)
  try((mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'), silent= T)
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile) %>% dplyr::filter(!OFFICECD %in% excluded_GA$OFFICECD)
  baseproduct = get_base_productcd(dbfile)
  
  # chỉ lấy case của base product
  case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(AGCODE, CASE)
  ape = dplyr::filter(kpi, kpi$APE != 0) %>% 
    dplyr::select(AGCODE, APE)
  
  mp_case = rbind(
    
    mp %>% 
    merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    #--
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_ape = rbind(
    mp %>% 
    merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    #---
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=ape, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(APE=sum(APE)/10^6) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_casesize = mp_ape %>% 
    merge(x=., y=mp_case, 
                   by.x=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level'),
                   by.y=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level')) %>% 
    dplyr::mutate(CASE_SIZE = APE/CASE)
    
  
  # create index
  mp_casesize['IDX']=0
  try((mp_casesize[mp_casesize$SEG=='GENLION',]$IDX = 1), silent= T)
  try((mp_casesize[mp_casesize$SEG=='Rookie in month',]$IDX = 2), silent= T)
  try((mp_casesize[mp_casesize$SEG=='Rookie last month',]$IDX = 3), silent= T)
  try((mp_casesize[mp_casesize$SEG=='2-3 months',]$IDX = 4), silent= T)
  try((mp_casesize[mp_casesize$SEG=='4-6 months',]$IDX = 5), silent= T)
  try((mp_casesize[mp_casesize$SEG=='7-12 months',]$IDX = 6), silent= T)
  try((mp_casesize[mp_casesize$SEG=='13+ months',]$IDX = 7), silent= T)
  try((mp_casesize[mp_casesize$SEG=='SA',]$IDX = 8), silent= T)
  try((mp_casesize[mp_casesize$SEG=='TOTAL_EXCL_SA',]$IDX = 9), silent= T)
  try((mp_casesize[mp_casesize$SEG=='TOTAL',]$IDX = 10), silent= T)
  
  # CREATE KPI
  try((mp_casesize['kpi']=''), silent= T)
  try((mp_casesize[mp_casesize$SEG=='GENLION',]$kpi = 'CaseSize_by_rookie_mdrt:MDRT_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='Rookie in month',]$kpi = 'CaseSize_by_rookie_mdrt:Rookie in month_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='Rookie last month',]$kpi = 'CaseSize_by_rookie_mdrt:Rookie last month_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='2-3 months',]$kpi = 'CaseSize_by_rookie_mdrt:2-3 months_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='4-6 months',]$kpi = 'CaseSize_by_rookie_mdrt:4 - 6 mths_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='7-12 months',]$kpi = 'CaseSize_by_rookie_mdrt:7-12mth_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='13+ months',]$kpi = 'CaseSize_by_rookie_mdrt:13+mth_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='SA',]$kpi = 'CaseSize_by_rookie_mdrt:SA_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='TOTAL',]$kpi = 'CaseSize_by_rookie_mdrt:Total_excluded_2GA'), silent= T)
  try((mp_casesize[mp_casesize$SEG=='TOTAL_EXCL_SA',]$kpi = 'CaseSize_by_rookie_mdrt:Total_exclude_SA_excluded_2GA'), silent= T)
  
  mp_casesize %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=CASE_SIZE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -CASE, -APE, -IDX, -CASE_SIZE)
    
}
Case_per_Active <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
    dplyr::mutate(SEG='GENLION') %>% 
    dplyr::select(AGENT_CODE, SEG) %>% 
    dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  baseproduct = get_base_productcd(dbfile)
  
  case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(AGCODE, CASE)
  
  mp_case = rbind(
    mp %>% 
    merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    # country
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  active = rbind(
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    # country
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_case_per_active = mp_case %>% 
    merge(x=., y=active, 
                   by.x=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level'),
                   by.y=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level')) %>% 
    dplyr::mutate(CASE_PER_ACTIVE = CASE/ACTIVE)
    
  
  # create index
  mp_case_per_active['IDX']=0
  mp_case_per_active[mp_case_per_active$SEG=='GENLION',]$IDX = 1
  mp_case_per_active[mp_case_per_active$SEG=='Rookie in month',]$IDX = 2
  mp_case_per_active[mp_case_per_active$SEG=='Rookie last month',]$IDX = 3
  mp_case_per_active[mp_case_per_active$SEG=='2-3 months',]$IDX = 4
  mp_case_per_active[mp_case_per_active$SEG=='4-6 months',]$IDX = 5
  mp_case_per_active[mp_case_per_active$SEG=='7-12 months',]$IDX = 6
  mp_case_per_active[mp_case_per_active$SEG=='13+ months',]$IDX = 7
  mp_case_per_active[mp_case_per_active$SEG=='SA',]$IDX = 8
  mp_case_per_active[mp_case_per_active$SEG=='TOTAL_EXCL_SA',]$IDX = 9
  mp_case_per_active[mp_case_per_active$SEG=='TOTAL',]$IDX = 10
  
  # CREATE KPI
  mp_case_per_active['kpi']=''
  mp_case_per_active[mp_case_per_active$SEG=='GENLION',]$kpi = '# Case/Active_by_rookie_mdrt:MDRT'
  mp_case_per_active[mp_case_per_active$SEG=='Rookie in month',]$kpi = '# Case/Active_by_rookie_mdrt:Rookie in month'
  mp_case_per_active[mp_case_per_active$SEG=='Rookie last month',]$kpi = '# Case/Active_by_rookie_mdrt:Rookie last month'
  mp_case_per_active[mp_case_per_active$SEG=='2-3 months',]$kpi = '# Case/Active_by_rookie_mdrt:2-3 months'
  mp_case_per_active[mp_case_per_active$SEG=='4-6 months',]$kpi = '# Case/Active_by_rookie_mdrt:4 - 6 mths'
  mp_case_per_active[mp_case_per_active$SEG=='7-12 months',]$kpi = '# Case/Active_by_rookie_mdrt:7-12mth'
  mp_case_per_active[mp_case_per_active$SEG=='13+ months',]$kpi = '# Case/Active_by_rookie_mdrt:13+mth'
  mp_case_per_active[mp_case_per_active$SEG=='SA',]$kpi = '# Case/Active_by_rookie_mdrt:SA'
  mp_case_per_active[mp_case_per_active$SEG=='TOTAL',]$kpi = '# Case/Active_by_rookie_mdrt:Total'
  mp_case_per_active[mp_case_per_active$SEG=='TOTAL_EXCL_SA',]$kpi = '# Case/Active_by_rookie_mdrt:Total_exclude_SA'
  
  mp_case_per_active %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=CASE_PER_ACTIVE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -CASE, -ACTIVE, -IDX, -CASE_PER_ACTIVE) 
}
Case_per_Active_exclude_2GA <- function(bssdt, genlion_final_dt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf("
       SELECT AGENTCD, OFFICECD FROM GVL_AGENTLIST WHERE BUSSINESSDATE='%s' AND OFFICECD in (
   select OFFICECD from RAWDATA_GATotal where OFFICE_NAME in ('003 GA HCM 2', '010 GA TIỀN GIANG 1')
  )
    ",bssdt)
  result <- dbSendQuery(my_database$con, SQL)
  excluded_GA = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(genlion_final_dt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  # phân loại agent theo nhóm sa, genlion, rookie... theo thứ tự ưu tiên từ cao đến thấp.
  sa = get_Manpower_sa(bssdt, dbfile) %>% 
    dplyr::mutate(SEG='SA') %>% 
    dplyr::select(AGENT_CODE, SEG)
  
  if (bssdt >= as.Date('2017-04-30')) {
    genlion = Genlion(strftime(bssdt, '%Y%m%d'), genlion_final_dt, dbfile) %>% 
      dplyr::mutate(SEG = 'GENLION') %>% 
      dplyr::select(AGENT_CODE, SEG) %>% 
      dplyr::filter(., !AGENT_CODE %in% sa$AGENT_CODE)
  } else {
    genlion <- get_Manpower_MDRT(bssdt, dbfile) %>% 
      dplyr::select(AGENT_CODE) %>% 
      dplyr::mutate(SEG = 'GENLION') # TRUOC 20170430 THI DAY LA MDRT NHUNG DE GENLION TUONG TRUNG
  }
  if (nrow(genlion) == 0) {
    warning(sprintf('Genlion of business date: %s - has no data', bssdt))
    stop()
  }
  
  mp = get_Manpower(bssdt, included_ter_ag = T, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12))) %>% 
    merge(x=., y=rbind(genlion,sa), by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T) %>% 
    dplyr::filter(!AGENT_CODE %in% excluded_GA$AGENTCD)
  
  mp[is.na(mp$SEG) & mp$MDIFF==0,]$SEG='Rookie in month'
  mp[is.na(mp$SEG) & mp$MDIFF==1,]$SEG='Rookie last month'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(2:3),]$SEG='2-3 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(4:6),]$SEG='4-6 months'
  mp[is.na(mp$SEG) & mp$MDIFF %in% c(7:12),]$SEG='7-12 months'
  mp[is.na(mp$SEG) & mp$MDIFF >= 13,]$SEG='13+ months'
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile) %>% dplyr::filter(!OFFICECD %in% excluded_GA$OFFICECD)
  baseproduct = get_base_productcd(dbfile)
  
  case = dplyr::filter(kpi, kpi$CASE != 0) %>% 
         merge(x=., y=baseproduct, by.x='PRODUCTCODE', by.y='PRODUCTCODE') %>% 
         dplyr::select(AGCODE, CASE)
  
  mp_case = rbind(
    mp %>% 
    merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
    dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
    dplyr::summarise(CASE=sum(CASE)) %>% 
    dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    # country
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% 
      merge(x=., y=case, by.x='AGENT_CODE', by.y='AGCODE') %>% 
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(CASE=sum(CASE)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  active = rbind(
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY, SEG, level='TERRITORY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY, level='TERRITORY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
    ,
    # country
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', SEG, level='COUNTRY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.)
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL')
    ,
    mp %>% dplyr::filter(ACTIVESP=='Yes') %>%  
      dplyr::filter(SEG!='SA') %>% 
      dplyr::group_by(BUSSINESSDATE, TERRITORY='COUNTRY', level='COUNTRY') %>% 
      dplyr::summarise(ACTIVE=n()) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::mutate(SEG='TOTAL_EXCL_SA')
  )
  
  mp_case_per_active = mp_case %>% 
    merge(x=., y=active, 
                   by.x=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level'),
                   by.y=c('BUSSINESSDATE','TERRITORY', 'SEG', 'level')) %>% 
    dplyr::mutate(CASE_PER_ACTIVE = CASE/ACTIVE)
    
  
  # create index
  try((mp_case_per_active['IDX']=0), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='GENLION',]$IDX = 1), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='Rookie in month',]$IDX = 2), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='Rookie last month',]$IDX = 3), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='2-3 months',]$IDX = 4), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='4-6 months',]$IDX = 5), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='7-12 months',]$IDX = 6), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='13+ months',]$IDX = 7), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='SA',]$IDX = 8), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='TOTAL_EXCL_SA',]$IDX = 9), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='TOTAL',]$IDX = 10), silent= T)
  
  # CREATE KPI
  try((mp_case_per_active['kpi']=''), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='GENLION',]$kpi = '# Case/Active_by_rookie_mdrt:MDRT_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='Rookie in month',]$kpi = '# Case/Active_by_rookie_mdrt:Rookie in month_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='Rookie last month',]$kpi = '# Case/Active_by_rookie_mdrt:Rookie last month_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='2-3 months',]$kpi = '# Case/Active_by_rookie_mdrt:2-3 months_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='4-6 months',]$kpi = '# Case/Active_by_rookie_mdrt:4 - 6 mths_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='7-12 months',]$kpi = '# Case/Active_by_rookie_mdrt:7-12mth_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='13+ months',]$kpi = '# Case/Active_by_rookie_mdrt:13+mth_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='SA',]$kpi = '# Case/Active_by_rookie_mdrt:SA_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='TOTAL',]$kpi = '# Case/Active_by_rookie_mdrt:Total_excluded_2GA'), silent= T)
  try((mp_case_per_active[mp_case_per_active$SEG=='TOTAL_EXCL_SA',]$kpi = '# Case/Active_by_rookie_mdrt:Total_exclude_SA_excluded_2GA'), silent= T)
  
  mp_case_per_active %>% dplyr::arrange(TERRITORY, IDX) %>% 
    dplyr::mutate(time_view=strftime(BUSSINESSDATE, '%Y%m'), territory=TERRITORY, value=CASE_PER_ACTIVE) %>% 
    dplyr::select(-BUSSINESSDATE, -TERRITORY, -SEG, -CASE, -ACTIVE, -IDX, -CASE_PER_ACTIVE) 
}

AD_LIST = function(dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
    "
  SELECT DISTINCT 'TEAM' AS LEVEL, TEAM_CODE AS ADCODE, TEAM_NAME AS ADNAME, TEAM_HEAD_CODE AS ADHEADCODE, TEAM_HEAD_NAME AS ADHEADNAME FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL
  UNION ALL
  SELECT DISTINCT 'OFFICE' AS LEVEL, OFFICE_CODE AS ADCODE, OFFICE_NAME AS ADNAME, OFFICE_HEAD_CODE AS ADHEADCODE, OFFICE_HEAD_NAME AS ADHEADNAME FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL
  UNION ALL
  SELECT DISTINCT 'ZONE' AS LEVEL, ZONE_CODE AS ADCODE, ZONE_NAME AS ADNAME, ZONE_HEAD_CODE AS ADHEADCODE, ZONE_HEAD_NAME AS ADHEADNAME FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL
  UNION ALL
  SELECT DISTINCT 'REGION' AS LEVEL, REGION_CODE AS ADCODE, REGION_NAME AS ADNAME, REGION_HEAD_CODE AS ADHEADCODE, REGION_HEAD_NAME AS ADHEADNAME FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL

  "
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data %>% dplyr::filter(!is.na(ADCODE))
}
ad_list_full_structure = function(dbfile = 'KPI_PRODUCTION/main_database.db') {
  territory = territory_mapping(dbfile)
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- 
    "
  SELECT DISTINCT 'TEAM' AS LEVEL, TEAM_CODE AS ADCODE, TEAM_NAME AS ADNAME, TEAM_HEAD_CODE AS ADHEADCODE, TEAM_HEAD_NAME AS ADHEADNAME,
  TEAM_HEAD_CODE, OFFICE_HEAD_CODE, ZONE_HEAD_CODE, REGION_HEAD_CODE, REGION_CODE
  FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL AND CHANNEL_CODE = 'TIEDAGENCY'
  UNION ALL
  SELECT DISTINCT 'OFFICE' AS LEVEL, OFFICE_CODE AS ADCODE, OFFICE_NAME AS ADNAME, OFFICE_HEAD_CODE AS ADHEADCODE, OFFICE_HEAD_NAME AS ADHEADNAME,
  '' AS TEAM_HEAD_CODE, OFFICE_HEAD_CODE, ZONE_HEAD_CODE, REGION_HEAD_CODE, REGION_CODE
  FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL AND OFFICE_CODE IS NOT NULL AND CHANNEL_CODE = 'TIEDAGENCY'
  UNION ALL
  SELECT DISTINCT 'ZONE' AS LEVEL, ZONE_CODE AS ADCODE, ZONE_NAME AS ADNAME, ZONE_HEAD_CODE AS ADHEADCODE, ZONE_HEAD_NAME AS ADHEADNAME,
  '' AS TEAM_HEAD_CODE, '' AS OFFICE_HEAD_CODE, ZONE_HEAD_CODE, REGION_HEAD_CODE, REGION_CODE
  FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL AND ZONE_CODE IS NOT NULL AND CHANNEL_CODE = 'TIEDAGENCY'
  UNION ALL
  SELECT DISTINCT 'REGION' AS LEVEL, REGION_CODE AS ADCODE, REGION_NAME AS ADNAME, REGION_HEAD_CODE AS ADHEADCODE, REGION_HEAD_NAME AS ADHEADNAME,
  '' AS TEAM_HEAD_CODE, '' AS OFFICE_HEAD_CODE, '' AS ZONE_HEAD_CODE, REGION_HEAD_CODE, REGION_CODE
  FROM RAWDATA_ADLIST
  WHERE ACTIVE='YES' AND TEAM_HEAD_CODE IS NOT NULL AND REGION_CODE IS NOT NULL AND CHANNEL_CODE = 'TIEDAGENCY'
  "
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data %>% 
    dplyr::filter(!is.na(ADCODE)) %>% 
    merge(
      x = .,
      y = territory %>% dplyr::select(REGION_CODE, TERRITORY),
      by.x = 'REGION_CODE',
      by.y = 'REGION_CODE',
      all.x = T
    )
}
territory_mapping = function(dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- "SELECT * FROM RAWDATA_MAPPING_TERRITORY_REGION"
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data
}
sales_by_product = function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile)
  prds = all_product()
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
  "
  SELECT
  C.AGNTNUM AS AGENT_CODE,
  C.REPNUM AS REPNUM,
  C.CHDRNUM AS CHDRNUM,
  C.STATCODE AS STATCODE,
  C.BILLFREQ AS BILLFREQ,
  C.CNTTYPE AS CNTTYPE,
  H.HPRRCVDT AS HPRRCVDT,
  CVR.CRTABLE AS COVRPF_PRODUCTCODE,
  CVT.CRTABLE AS COVTPF_PRODUCTCODE,
  CVR.INSTPREM AS COVRPF_INSTPREM,
  CVT.APE AS COVTPF_INSTPREM
  FROM (
      SELECT * FROM CHDRPF WHERE VALIDFLAG <> '2'
  )C
  INNER JOIN HPADPF H ON C.CHDRNUM = H.CHDRNUM
  LEFT JOIN (
      SELECT
      CHDRNUM,
      CRTABLE,
      Sum(SINGP) AS SP,
      Sum(INSTPREM) AS Prem,
      Sum([INSTPREM]*[BILLFREQ]) AS APE,
      BILLFREQ
      FROM COVTPF
      GROUP BY CHDRNUM, CRTABLE, BILLFREQ

  ) CVT ON C.CHDRNUM = CVT.CHDRNUM
  LEFT JOIN (
      SELECT CHDRNUM, CRTABLE, STATCODE, INSTPREM
      FROM COVRPF
        WHERE VALIDFLAG='1'
  )CVR ON C.CHDRNUM = CVR.CHDRNUM
  WHERE H.HPRRCVDT LIKE '%s'
  "
  ,
  strftime(bssdt, '%Y%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding = "utf-8")
  dbClearResult(result)
  data = data %>% 
    dplyr::mutate(HPRRCVDT = as.Date(strptime(HPRRCVDT, '%Y%m%d'))) %>% 
    dplyr::mutate(PRODUCTCODE = ifelse(!is.na(COVRPF_PRODUCTCODE), COVRPF_PRODUCTCODE, COVTPF_PRODUCTCODE)) %>% 
    dplyr::mutate(ISSUED_APE = as.numeric(BILLFREQ)*COVRPF_INSTPREM) %>% 
    dplyr::mutate(PENDING_APE = ifelse(!is.na(COVTPF_INSTPREM), as.numeric(BILLFREQ)*COVTPF_INSTPREM, as.numeric(BILLFREQ)*COVRPF_INSTPREM)) 
    
  tied_agency_ape =  mp %>% 
    merge(x = ., y = data, by.x = 'AGENT_CODE', by.y = 'AGENT_CODE')
  
  # return
    tied_agency_ape %>% 
    dplyr::group_by(time_view = strftime(HPRRCVDT, '%Y%m'), PRODUCTCODE) %>%
    dplyr::summarise(
      Case = n()
      ,
      `Keyed-in APE` = sum(PENDING_APE, na.rm = T)/10^6
      ,
      `% Mix` = (sum(PENDING_APE, na.rm = T)/10^6)/(sum(tied_agency_ape$PENDING_APE)/10^6)
      ,
      `Issued APE` = sum(ISSUED_APE, na.rm = T)/10^6
      ) %>% 
    dplyr::ungroup(.) %>% 
      merge(x = ., y = prds, by.x='PRODUCTCODE', by.y = 'PRODUCTCODE', all.x = T) %>% 
      dplyr::mutate(`Product Code` = PRODUCTCODE, `Product Name`=VIETNAMESENAME, `Base Product` = ifelse(PRODUCT_TYPE == 'BASEPRODUCT', 'Yes', 'No'))

  
}

# 
# PERSISTENCY -------------------------------------------------------------
#
get_persistency_individual <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency 2Y
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  INDIVIDUAL AS value, 
                  TOTALAPECURRENT_INDI as current_ape, 
                  TOTALAPEORIGINAL_INDI as original_ape 
                FROM RAWDATA_Persistency
                WHERE DATE = '%s'
                AND INDIVIDUAL IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per2y
}
get_persistency_y1_individual <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency 2Y
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  INDIVIDUAL AS value, 
                  TOTALAPECURRENT_INDI as current_ape, 
                  TOTALAPEORIGINAL_INDI as original_ape 
                FROM RAWDATA_Persistency_Y1
                WHERE DATE = '%s'
                AND INDIVIDUAL IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per2y
}
get_persistency_y2_individual <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency 2Y
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  INDIVIDUAL AS value, 
                  TOTALAPECURRENT_INDI as current_ape, 
                  TOTALAPEORIGINAL_INDI as original_ape 
                FROM RAWDATA_Persistency_Y2
                WHERE DATE = '%s'
                AND INDIVIDUAL IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per2y
}
get_persistency_office <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency 2Y
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  OFFICE AS value, 
                  TOTALAPECURRENT_OFFICE as current_ape, 
                  TOTALAPEORIGINAL_OFFICE as original_ape 
                FROM RAWDATA_Persistency
                WHERE DATE = '%s'
                AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per2y
}
get_persistency_y1_office <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency Y1
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  OFFICE AS value, 
                  TOTALAPECURRENT_OFFICE as current_ape, 
                 TOTALAPEORIGINAL_OFFICE as original_ape 
                FROM RAWDATA_Persistency_Y1
                WHERE DATE = '%s'
                AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y1 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per_y1
}
get_persistency_y2_office <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency Y2
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  OFFICE AS value, 
                  TOTALAPECURRENT_OFFICE as current_ape, 
                  TOTALAPEORIGINAL_OFFICE as original_ape 
                FROM RAWDATA_Persistency_Y2
                WHERE DATE = '%s'
                AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y2 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per_y2
}
get_persistency_region <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency 2Y
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  REGION AS value, 
                  TOTALAPECURRENT_REGION as current_ape, 
                  TOTALAPEORIGINAL_REGION as original_ape 
                FROM RAWDATA_Persistency
                WHERE DATE = '%s'
                AND REGION IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per2y
}
get_persistency_y1_region <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency Y1
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  REGION AS value, 
                  TOTALAPECURRENT_REGION as current_ape, 
                  TOTALAPEORIGINAL_REGION as original_ape 
                FROM RAWDATA_Persistency_Y1
                WHERE DATE = '%s'
                AND REGION IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y1 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per_y1
}
get_persistency_y2_region <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  # persistency Y2
  SQL <- sprintf("
                SELECT 
                  DATE as time_view, 
                  AGENT_CODE AS segment, 
                  REGION AS value, 
                  TOTALAPECURRENT_REGION as current_ape, 
                  TOTALAPEORIGINAL_REGION as original_ape 
                FROM RAWDATA_Persistency_Y2
                WHERE DATE = '%s'
                AND REGION IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y2 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  per_y2
}

persistency_UNIT_Y1 <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT DATE AS BSSDT, U.UNIT_CODE, UNIT AS PERSISTENCY_Y1 
    FROM RAWDATA_Persistency_Y1 P
    LEFT JOIN RAWDATA_UnitList U ON P.AGENT_CODE = U.AGENT_CODE
    WHERE DATE = '%s' AND UNIT IS NOT NULL",
    paste(bssdt, collapse = "','")
  )
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding = "utf-8")
  dbClearResult(result)
  data
}
persistency_UNIT_Y2 <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT DATE AS BSSDT, U.UNIT_CODE, UNIT AS PERSISTENCY_Y2 
    FROM RAWDATA_Persistency_Y2 P
    LEFT JOIN RAWDATA_UnitList U ON P.AGENT_CODE = U.AGENT_CODE
    WHERE DATE = '%s' AND UNIT IS NOT NULL",
    paste(bssdt, collapse = "','")
  )
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding = "utf-8")
  dbClearResult(result)
  data
}
persistency_BRANCH_Y1 <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT DATE AS BSSDT, U.UNIT_CODE as BRANCHCD, BRANCH AS PERSISTENCY_Y1 
    FROM RAWDATA_Persistency_Y1 P
    LEFT JOIN RAWDATA_UnitList U ON P.AGENT_CODE = U.AGENT_CODE
    WHERE DATE = '%s' ",
    paste(bssdt, collapse = "','")
  )
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding = "utf-8")
  dbClearResult(result)
  data
}
persistency_BRANCH_Y2 <- function(bssdt, dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "SELECT DATE AS BSSDT, U.UNIT_CODE as BRANCHCD, BRANCH AS PERSISTENCY_Y2 
    FROM RAWDATA_Persistency_Y2 P
    LEFT JOIN RAWDATA_UnitList U ON P.AGENT_CODE = U.AGENT_CODE
    WHERE DATE = '%s' ",
    paste(bssdt, collapse = "','")
  )
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding = "utf-8")
  dbClearResult(result)
  data
}

#
# BD ----------------------------------------------------------------------
#
BD <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "
    SELECT RM.TERRITORY, AD.REGION_CODE, AD.REGION_NAME, AD.ZONE_CODE, AD.ZONE_NAME, AD.TEAM_CODE, AD.TEAM_NAME, AD.TEAM_HEAD_NAME
    FROM RAWDATA_ADLIST AD
    JOIN RAWDATA_MAPPING_TERRITORY_REGION RM ON AD.REGION_CODE=RM.REGION_CODE
    WHERE TEAM_NAME != 'DUMMY' 
    AND SUB_CHANNELCODE='TIEDAGENCY' AND ACTIVE='YES'
    ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  ad_list = fetch(result, encoding="utf-8")
  dbClearResult(result)

  SQL <- sprintf(
    "
  SELECT BUSSINESSDATE, TEAMCD, MONTHSTART, MONTHEND, SERVICING_AGENT, AGENT_DESIGNATION, TERMINATION_DATE, ACTIVESP,
    SUPERVISOR_CODE, SUPERVISOR_CODE_DESIGNATION, AGENT_CODE
  FROM RAWDATA_Manpower_ActiveRatio
  WHERE BUSSINESSDATE='%s'
    ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  data = data %>% 
    dplyr::mutate(Start=if_else(MONTHSTART=='Yes',1,0)) %>% 
    dplyr::mutate(End=if_else(MONTHEND=='Yes' & is.na(SERVICING_AGENT),1,0)) %>% 
    dplyr::mutate(SA=if_else(!is.na(SERVICING_AGENT) & SERVICING_AGENT=='Yes', 1, 0)) %>% 
    dplyr::mutate(AG=if_else(MONTHEND=='Yes' & AGENT_DESIGNATION=='AG',1,0)) %>% 
    dplyr::mutate(US=if_else(MONTHEND=='Yes' & AGENT_DESIGNATION=='US',1,0)) %>% 
    dplyr::mutate(AL=if_else(MONTHEND=='Yes' & AGENT_DESIGNATION %in% c('UM','BM'),1,0)) %>% 
    dplyr::mutate(Ter=if_else(MONTHEND=='No' & substr(BUSSINESSDATE, 1, 7)==substr(TERMINATION_DATE, 1, 7) ,1,0)) %>% 
    dplyr::mutate(Recruit=if_else(MONTHSTART=='No' & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(RecruitAGUS=if_else(MONTHSTART=='No' & MONTHEND=='Yes' & AGENT_DESIGNATION %in% c('AG','US'),1,0)) %>% 
    dplyr::mutate(RecruitAL=if_else(MONTHSTART=='No' & MONTHEND=='Yes' & AGENT_DESIGNATION %in% c('UM','BM'),1,0)) %>% 
    dplyr::mutate(Actv=if_else(ACTIVESP=='Yes', 1, 0))
  team = data %>%  
    dplyr::group_by(TEAMCD) %>% 
    dplyr::summarise(
      Start=sum(Start, na.rm = T), End=sum(End, na.rm = T), 
      SA=sum(SA, na.rm = T), AG=sum(AG, na.rm = T), US=sum(US, na.rm = T), 
      AL=sum(AL, na.rm = T), Ter=sum(Ter, na.rm = T), Recruit=sum(Recruit, na.rm = T), 
      RecruitAGUS=sum(RecruitAGUS, na.rm = T),
      RecruitAL=sum(RecruitAL, na.rm = T),
      Actv=sum(Actv, na.rm = T)
      ) %>% 
    dplyr::ungroup(.)
  
  team2 = data %>% 
    dplyr::filter(!is.na(MONTHSTART) & MONTHSTART=='No' & !is.na(MONTHEND) & MONTHEND=='Yes') %>% 
    dplyr::group_by(TEAMCD, SUPERVISOR_CODE, SUPERVISOR_CODE_DESIGNATION) %>% 
    dplyr::summarise(CountOfAgent=n()) %>% 
    dplyr::mutate(ActvUS=if_else(!is.na(SUPERVISOR_CODE_DESIGNATION) & SUPERVISOR_CODE_DESIGNATION=='US', 1, 0)) %>% 
    dplyr::mutate(ActvAL=if_else(!is.na(SUPERVISOR_CODE_DESIGNATION) & SUPERVISOR_CODE_DESIGNATION %in% c('UM','BM'), 1, 0)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::group_by(TEAMCD) %>% 
    dplyr::summarise(CountOfSupervisorCode=n(), ActvUS=sum(ActvUS, na.rm = T), ActvAL=sum(ActvAL, na.rm = T))
  
  SQL <- sprintf(
    "
    SELECT 
      kpi.BUSSINESSDATE, kpi.TEAMCD, kpi.APE, kpi.`CASE`, kpi.FYP, pr.BASEPRODUCT
    FROM
    RAWDATA_KPITotal kpi LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
    WHERE kpi.BUSSINESSDATE like '%s'
    ", strftime(bssdt, "%Y%"))
  result <- dbSendQuery(my_database$con, SQL)
  kpi = fetch(result, encoding="utf-8") %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d"))) %>% 
    dplyr::mutate(CASECOUNT = if_else(BASEPRODUCT=='Yes', CASE, 0)) 
  dbClearResult(result)
  kpi_mtd = kpi %>% 
    dplyr::group_by(TEAMCD, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(APE=sum(APE, na.rm = T)/10^6, FYP=sum(FYP, na.rm = T)/10^6, CASECOUNT=sum(CASECOUNT, na.rm = T))
  kpi_ytd = kpi %>%
    dplyr::group_by(TEAMCD, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y')) %>% 
    dplyr::summarise(APE=sum(APE, na.rm = T)/10^6)
  
  ape_mtd = tidyr::spread(select(kpi_mtd, TEAMCD, BUSSINESSDATE, APE), BUSSINESSDATE, APE)
  ape_ytd = tidyr::spread(select(kpi_ytd, TEAMCD, BUSSINESSDATE, APE), BUSSINESSDATE, APE)
  names(ape_mtd) = paste(names(ape_mtd), 'APE', sep = "_")
  names(ape_ytd) = paste(names(ape_ytd), 'APE', sep = "_")
  casecount_mtd = tidyr::spread(
    select(
      dplyr::filter(kpi_mtd, BUSSINESSDATE==strftime(bssdt, '%Y%m')), TEAMCD, BUSSINESSDATE, CASECOUNT),
    BUSSINESSDATE, CASECOUNT
    )
  names(casecount_mtd) = paste(names(casecount_mtd), 'CASECOUNT', sep="_")
  # result
  result = ad_list %>% 
    merge(x=., y=team, by.x='TEAM_CODE', by.y='TEAMCD', all.x=T) %>% 
    merge(x=., y=team2, by.x='TEAM_CODE', by.y='TEAMCD', all.x=T) %>% 
    merge(x=., y=ape_mtd, by.x='TEAM_CODE', by.y='TEAMCD_APE', all.x=T) %>% 
    merge(x=., y=casecount_mtd, by.x='TEAM_CODE', by.y='TEAMCD_CASECOUNT', all.x=T) %>% 
    merge(x=., y=ape_ytd, by.x='TEAM_CODE', by.y='TEAMCD_APE', all.x=T) %>% 
    dplyr::mutate(ActvALR=ActvAL/AL) %>% 
    dplyr::mutate(ActvUSR=ActvUS/ActvUS) %>% 
    dplyr::mutate(Ratio=Actv/((Start+End)/2)) 
  result[,'CSize'] = result[,sprintf('%s_APE',strftime(bssdt, '%Y%m'))]/result[,sprintf('%s_CASECOUNT',strftime(bssdt, '%Y%m'))]
  result
}
#
# GA ----------------------------------------------------------------------
#
GA <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "
    SELECT 
      GA.OFFICECD,
  		GA.REGION_NAME AS Region, 
  		GA.OFFICE_NAME AS Office, 
  		GA.GANAME AS GA_Name, 
  		GA.AGCODE AS GAD, 
  		GA.AGNAME AS GAD_Name, 
  		GA.CONTRACTDT AS Contract, 
  		GA.EFFROM AS Effective, 
  		GA.OPENINGDT AS Opening, 
  		mpa.MonthStart,
  		mpa.MonthEnd,
  		mpa.AGENT_DESIGNATION,
  		mpa.ActiveSP,
  		mpa.Active
  	FROM RAWDATA_GATotal GA 
  	LEFT JOIN RAWDATA_Manpower_ActiveRatio mpa ON GA.OFFICECD = mpa.OFFICECD
  	WHERE 
  		mpa.BussinessDate='%s'
    ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data = data %>% 
    dplyr::mutate(Start=if_else(MONTHSTART=='Yes',1,0)) %>% 
    dplyr::mutate(End=if_else(MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(Add=if_else(MONTHSTART=='No' & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(AL=if_else(AGENT_DESIGNATION %in% c('UM','BM') & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(Actv=if_else(ACTIVESP=='Yes',1,0)) %>% 
    dplyr::mutate(Actv1c=if_else(ACTIVE=='Yes',1,0)) %>% 
    dplyr::group_by(OFFICECD, Region, Office, GA_Name, GAD, GAD_Name, Contract, Effective, Opening) %>% 
    dplyr::summarise(Start=sum(Start), End=sum(End), Add=sum(Add), AL=sum(AL), Actv=sum(Actv), Actv1c=sum(Actv1c)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(Ratio=Actv/((Start+End)/2))

  # sum ape
  SQL<- sprintf(
  "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.APE
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%'))
  result <- dbSendQuery(my_database$con, SQL)
  ape = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  ape = ape %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d")))
  sumape = ape %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
  sumape_ytd = ape %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y')) %>% 
    dplyr::summarise(APE=sum(APE)/10^6) 
  sumape = tidyr::spread(sumape, BUSSINESSDATE, APE)
  sumape_ytd = tidyr::spread(sumape_ytd, BUSSINESSDATE, APE)
  names(sumape) = paste(names(sumape), 'APE', sep = "_")
  names(sumape_ytd) = paste(names(sumape_ytd), 'APE_YTD', sep = "_")
 
   # sum casecount
  SQL<- sprintf(
    "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.`CASE`
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  sumcase = fetch(result, encoding="utf-8")
  dbClearResult(result)
  sumcase = sumcase %>% 
    # dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d"))) %>% 
    dplyr::mutate(CASECOUNT = if_else(BASEPRODUCT=='Yes', CASE, 0)) %>% 
    # dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::group_by(OFFICECD) %>%
    dplyr::summarise(CASE_MTD=sum(CASECOUNT))
  # sumcase = tidyr::spread(sumcase, BUSSINESSDATE, CASECOUNT)
  # names(sumcase) = paste(names(sumcase), 'CASE', sep = "_")
  
  # persistency 2Y
  SQL <- sprintf("
                 SELECT AGENT_CODE AS OFFICECD, OFFICE AS PERSISTENCY2Y FROM RAWDATA_Persistency
                 WHERE DATE = '%s'
                 AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # persistency Y2
  SQL <- sprintf("
                 SELECT AGENT_CODE AS OFFICECD, OFFICE AS PERSISTENCY_Y2 FROM RAWDATA_Persistency_Y2
                 WHERE DATE = '%s'
                 AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y2 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  # return
  data %>% 
    merge(x=., y=sumape, by.x=c('OFFICECD'), by.y=c('OFFICECD_APE'), all.x=T) %>% 
    merge(x=., y=sumcase, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) %>% 
    merge(x=., y=sumape_ytd, by.x=c('OFFICECD'), by.y=c('OFFICECD_APE_YTD'), all.x=T) %>% 
    merge(x=., y=per_y2, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) %>% 
    merge(x=., y=per2y, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) 
}
GA_fyp <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "
    SELECT 
      GA.OFFICECD,
  		GA.REGION_NAME AS Region, 
  		GA.OFFICE_NAME AS Office, 
  		GA.GANAME AS GA_Name, 
  		GA.AGCODE AS GAD, 
  		GA.AGNAME AS GAD_Name, 
  		GA.CONTRACTDT AS Contract, 
  		GA.EFFROM AS Effective, 
  		GA.OPENINGDT AS Opening, 
  		mpa.MonthStart,
  		mpa.MonthEnd,
  		mpa.AGENT_DESIGNATION,
  		mpa.ActiveSP,
  		mpa.Active
  	FROM RAWDATA_GATotal GA 
  	LEFT JOIN RAWDATA_Manpower_ActiveRatio mpa ON GA.OFFICECD = mpa.OFFICECD
  	WHERE 
  		mpa.BussinessDate='%s'
    ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data = data %>% 
    dplyr::mutate(Start=if_else(MONTHSTART=='Yes',1,0)) %>% 
    dplyr::mutate(End=if_else(MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(Add=if_else(MONTHSTART=='No' & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(AL=if_else(AGENT_DESIGNATION %in% c('UM','BM') & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(Actv=if_else(ACTIVESP=='Yes',1,0)) %>% 
    dplyr::mutate(Actv1c=if_else(ACTIVE=='Yes',1,0)) %>% 
    dplyr::group_by(OFFICECD, Region, Office, GA_Name, GAD, GAD_Name, Contract, Effective, Opening) %>% 
    dplyr::summarise(Start=sum(Start), End=sum(End), Add=sum(Add), AL=sum(AL), Actv=sum(Actv), Actv1c=sum(Actv1c)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(Ratio=Actv/((Start+End)/2))

  # sum fyp
  SQL<- sprintf(
  "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.FYP
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%'))
  result <- dbSendQuery(my_database$con, SQL)
  fyp = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  fyp = fyp %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d")))
  sumfyp = fyp %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(FYP=sum(FYP)/10^6) %>% 
    dplyr::ungroup(.)
  sumfyp_ytd = fyp %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y')) %>% 
    dplyr::summarise(FYP=sum(FYP)/10^6) 
  sumfyp = tidyr::spread(sumfyp, BUSSINESSDATE, FYP)
  sumfyp_ytd = tidyr::spread(sumfyp_ytd, BUSSINESSDATE, FYP)
  names(sumfyp) = paste(names(sumfyp), 'FYP', sep = "_")
  names(sumfyp_ytd) = paste(names(sumfyp_ytd), 'FYP_YTD', sep = "_")
 
   # sum casecount
  SQL<- sprintf(
    "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.`CASE`
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  sumcase = fetch(result, encoding="utf-8")
  dbClearResult(result)
  sumcase = sumcase %>% 
    # dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d"))) %>% 
    dplyr::mutate(CASECOUNT = if_else(BASEPRODUCT=='Yes', CASE, 0)) %>% 
    # dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::group_by(OFFICECD) %>%
    dplyr::summarise(CASE_MTD=sum(CASECOUNT))
  # sumcase = tidyr::spread(sumcase, BUSSINESSDATE, CASECOUNT)
  # names(sumcase) = paste(names(sumcase), 'CASE', sep = "_")
  
  # persistency 2Y
  SQL <- sprintf("
                 SELECT AGENT_CODE AS OFFICECD, OFFICE AS PERSISTENCY2Y FROM RAWDATA_Persistency
                 WHERE DATE = '%s'
                 AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # persistency Y2
  SQL <- sprintf("
                 SELECT AGENT_CODE AS OFFICECD, OFFICE AS PERSISTENCY_Y2 FROM RAWDATA_Persistency_Y2
                 WHERE DATE = '%s'
                 AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y2 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  # return
  data %>% 
    merge(x=., y=sumfyp, by.x=c('OFFICECD'), by.y=c('OFFICECD_FYP'), all.x=T) %>% 
    merge(x=., y=sumcase, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) %>% 
    merge(x=., y=sumfyp_ytd, by.x=c('OFFICECD'), by.y=c('OFFICECD_FYP_YTD'), all.x=T) %>% 
    merge(x=., y=per_y2, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) %>% 
    merge(x=., y=per2y, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) 
}
GA_ryp <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  my_database <- src_sqlite(dbfile, create = TRUE)
  SQL <- sprintf(
    "
    SELECT 
      GA.OFFICECD,
  		GA.REGION_NAME AS Region, 
  		GA.OFFICE_NAME AS Office, 
  		GA.GANAME AS GA_Name, 
  		GA.AGCODE AS GAD, 
  		GA.AGNAME AS GAD_Name, 
  		GA.CONTRACTDT AS Contract, 
  		GA.EFFROM AS Effective, 
  		GA.OPENINGDT AS Opening, 
  		mpa.MonthStart,
  		mpa.MonthEnd,
  		mpa.AGENT_DESIGNATION,
  		mpa.ActiveSP,
  		mpa.Active
  	FROM RAWDATA_GATotal GA 
  	LEFT JOIN RAWDATA_Manpower_ActiveRatio mpa ON GA.OFFICECD = mpa.OFFICECD
  	WHERE 
  		mpa.BussinessDate='%s'
    ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  data = data %>% 
    dplyr::mutate(Start=if_else(MONTHSTART=='Yes',1,0)) %>% 
    dplyr::mutate(End=if_else(MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(Add=if_else(MONTHSTART=='No' & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(AL=if_else(AGENT_DESIGNATION %in% c('UM','BM') & MONTHEND=='Yes',1,0)) %>% 
    dplyr::mutate(Actv=if_else(ACTIVESP=='Yes',1,0)) %>% 
    dplyr::mutate(Actv1c=if_else(ACTIVE=='Yes',1,0)) %>% 
    dplyr::group_by(OFFICECD, Region, Office, GA_Name, GAD, GAD_Name, Contract, Effective, Opening) %>% 
    dplyr::summarise(Start=sum(Start), End=sum(End), Add=sum(Add), AL=sum(AL), Actv=sum(Actv), Actv1c=sum(Actv1c)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(Ratio=Actv/((Start+End)/2))

  # sum ryp
  SQL<- sprintf(
  "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.RYP
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%'))
  result <- dbSendQuery(my_database$con, SQL)
  ryp = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  ryp = ryp %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d")))
  sumryp = ryp %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(RYP=sum(RYP, na.rm = T)/10^6) %>% 
    dplyr::ungroup(.)
  sumryp_ytd = ryp %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y')) %>% 
    dplyr::summarise(RYP=sum(RYP)/10^6) 
  sumryp = tidyr::spread(sumryp, BUSSINESSDATE, RYP)
  sumryp_ytd = tidyr::spread(sumryp_ytd, BUSSINESSDATE, RYP)
  names(sumryp) = paste(names(sumryp), 'RYP', sep = "_")
  names(sumryp_ytd) = paste(names(sumryp_ytd), 'RYP_YTD', sep = "_")
 
   # sum casecount
  SQL<- sprintf(
    "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.`CASE`
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%m%'))
  result <- dbSendQuery(my_database$con, SQL)
  sumcase = fetch(result, encoding="utf-8")
  dbClearResult(result)
  sumcase = sumcase %>% 
    # dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d"))) %>% 
    dplyr::mutate(CASECOUNT = if_else(BASEPRODUCT=='Yes', CASE, 0)) %>% 
    # dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::group_by(OFFICECD) %>%
    dplyr::summarise(CASE_MTD=sum(CASECOUNT))
  # sumcase = tidyr::spread(sumcase, BUSSINESSDATE, CASECOUNT)
  # names(sumcase) = paste(names(sumcase), 'CASE', sep = "_")
  
  # persistency 2Y
  SQL <- sprintf("
                 SELECT AGENT_CODE AS OFFICECD, OFFICE AS PERSISTENCY2Y FROM RAWDATA_Persistency
                 WHERE DATE = '%s'
                 AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per2y = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # persistency Y2
  SQL <- sprintf("
                 SELECT AGENT_CODE AS OFFICECD, OFFICE AS PERSISTENCY_Y2 FROM RAWDATA_Persistency_Y2
                 WHERE DATE = '%s'
                 AND OFFICE IS NOT NULL
                 ", strftime(bssdt, "%Y-%m-%d"))
  result <- dbSendQuery(my_database$con, SQL)
  per_y2 = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  # return
  data %>% 
    merge(x=., y=sumryp, by.x=c('OFFICECD'), by.y=c('OFFICECD_RYP'), all.x=T) %>% 
    merge(x=., y=sumcase, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) %>% 
    merge(x=., y=sumryp_ytd, by.x=c('OFFICECD'), by.y=c('OFFICECD_RYP_YTD'), all.x=T) %>% 
    merge(x=., y=per_y2, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) %>% 
    merge(x=., y=per2y, by.x=c('OFFICECD'), by.y=c('OFFICECD'), all.x=T) 
}
GA_ape <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  my_database <- src_sqlite(dbfile, create = TRUE)
  # sum ape
  SQL<- sprintf(
  "
  SELECT 
  	GA.OFFICECD, 
  	kpi.BussinessDate,
  	pr.BASEPRODUCT,
  	kpi.APE
  FROM RAWDATA_GATotal GA 
  LEFT JOIN RAWDATA_KPITotal kpi ON GA.OFFICECD = kpi.OFFICECD
  LEFT JOIN RAWDATA_Product pr ON kpi.PRODUCTCODE = pr.PRUDUCTCODE
  WHERE kpi.BUSSINESSDATE LIKE '%s'
  ", strftime(bssdt, '%Y%'))
  result <- dbSendQuery(my_database$con, SQL)
  ape = fetch(result, encoding="utf-8")
  dbClearResult(result)
  
  ape = ape %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, "%Y%m%d")))
  sumape = ape %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y%m')) %>% 
    dplyr::summarise(APE=sum(APE)/10^6) %>% 
    dplyr::ungroup(.)
  sumape_ytd = ape %>% 
    dplyr::group_by(OFFICECD, BUSSINESSDATE = strftime(BUSSINESSDATE, '%Y')) %>% 
    dplyr::summarise(APE=sum(APE)/10^6) 
  sumape = tidyr::spread(sumape, BUSSINESSDATE, APE)
  sumape_ytd = tidyr::spread(sumape_ytd, BUSSINESSDATE, APE)
  names(sumape) = paste(names(sumape), 'APE', sep = "_")
  names(sumape_ytd) = paste(names(sumape_ytd), 'APE_YTD', sep = "_")
 
}


# Production_AD Structure -------------------------------------------------
Production_AD_Structure_REGION <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  idx = 0
  idx = idx + 1
  mp_total = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='MP') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  activity_ratio_total = kpi_segmentation(criteria = "where kpi='Activity Ratio_by_rookie_mdrt:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='AR') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  active_total = kpi_segmentation(criteria = "where kpi='# Active_by_rookie_mdrt:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Active') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  case_per_active_total = kpi_segmentation(criteria = "where kpi='# Case/Active_by_rookie_mdrt:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Case/Active') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  casesize_total = kpi_segmentation(criteria = "where kpi='CaseSize_by_rookie_mdrt:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Casesize') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  ape_total = kpi_segmentation(criteria = "where kpi='APE_by_rookie_mdrt:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='APE') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  case_total = kpi_segmentation(criteria = "where kpi='# Case_by_rookie_mdrt:Total' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Case') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  Active_excSA = kpi_segmentation(criteria = "where kpi='# Active_by_rookie_mdrt:Total (excl. SA)' and level='REGION' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Active_excSA') %>% 
    dplyr::mutate(fmt='#,##0')
  
  #
  # Production_AD Structure: territory
  #
  data_group_by_territory = rbind(
    # total man power 
    mp_total, mp_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    activity_ratio_total, activity_ratio_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    active_total, active_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    case_per_active_total, case_per_active_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    casesize_total, casesize_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    ape_total, ape_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    case_total, case_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    Active_excSA, Active_excSA %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
  )
  data_group_by_territory %>% dplyr::mutate(level='REGION')
}
Production_AD_Structure_TERRITORY <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  idx = 0
  idx = idx + 1
  mp_total = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='MP') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  activity_ratio_total = kpi_segmentation(criteria = "where kpi='Activity Ratio_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='AR') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  active_total = kpi_segmentation(criteria = "where kpi='# Active_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Active') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  case_per_active_total = kpi_segmentation(criteria = "where kpi='# Case/Active_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Case/Active') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  casesize_total = kpi_segmentation(criteria = "where kpi='CaseSize_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Casesize') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  ape_total = kpi_segmentation(criteria = "where kpi='APE_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='APE') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  case_total = kpi_segmentation(criteria = "where kpi='# Case_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Case') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  Active_excSA = kpi_segmentation(criteria = "where kpi='# Active_by_rookie_mdrt:Total (excl. SA)' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Active_excSA') %>% 
    dplyr::mutate(fmt='#,##0')
  
  #
  # Production_AD Structure: territory
  #
  data_group_by_territory = rbind(
    # total man power 
    mp_total, mp_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    activity_ratio_total, activity_ratio_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    active_total, active_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    case_per_active_total, case_per_active_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    casesize_total, casesize_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    ape_total, ape_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    case_total, case_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    Active_excSA, Active_excSA %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
  )
  data_group_by_territory
}
Production_AD_Structure_COUNTRY <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  idx = 0
  idx = idx + 1
  mp_total = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='MP') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  activity_ratio_total = kpi_segmentation(criteria = "where kpi='Activity Ratio_by_rookie_mdrt:Total' and level='COUNTRY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='AR') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  active_total = kpi_segmentation(criteria = "where kpi='# Active_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Active') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  case_per_active_total = kpi_segmentation(criteria = "where kpi='# Case/Active_by_rookie_mdrt:Total' and level='COUNTRY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Case/Active') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  casesize_total = kpi_segmentation(criteria = "where kpi='CaseSize_by_rookie_mdrt:Total' and level='COUNTRY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Casesize') %>% 
    dplyr::mutate(fmt='#,##0.0')
  idx = idx + 1
  ape_total = kpi_segmentation(criteria = "where kpi='APE_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='APE') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  case_total = kpi_segmentation(criteria = "where kpi='# Case_by_rookie_mdrt:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Case') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  Active_excSA = kpi_segmentation(criteria = "where kpi='# Active_by_rookie_mdrt:Total (excl. SA)' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Active_excSA') %>% 
    dplyr::mutate(fmt='#,##0')
  
  #
  # Production_AD Structure: territory
  #
  data_group_by_territory = rbind(
    # total man power 
    mp_total, mp_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    activity_ratio_total, activity_ratio_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    active_total, active_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    case_per_active_total, case_per_active_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    casesize_total, casesize_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    ape_total, ape_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    case_total, case_total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    Active_excSA, Active_excSA %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
  )
  data_group_by_territory
}

# Recruitment_Structure ---------------------------------------------------

Recruitment_Structure_REGION <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  idx = 0
  idx = idx + 1
  total = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:Total' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Total # New recruits') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  ag = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:AG' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='AG') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  al = kpi_segmentation(criteria = "where kpi='Recruit_AL' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='Total recruited AL') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  us = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:US' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='US') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  um = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:UM' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='UM') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  sum = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:SUM' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='SUM') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  bm = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:BM' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='BM') %>% 
    dplyr::mutate(fmt='#,##0')
  idx = idx + 1
  sbm = kpi_segmentation(criteria = "where kpi='Recruit_by_designation:SBM' and level='TERRITORY' ", dbfile) %>% 
    dplyr::mutate(idx=idx) %>% 
    dplyr::mutate(kpi='SBM') %>% 
    dplyr::mutate(fmt='#,##0')
  rbind(
    total, total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    ag, ag %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    al, al %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    us, us %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    um, um %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    sum, sum %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    bm, bm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.),
    sbm, sbm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx, fmt) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
  )
}


# Recruitment KPI_Structure -----------------------------------------------
Recruitment_KPI_Structure_detail  <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  us = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:US' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=5) %>% 
    dplyr::mutate(kpi='# US')
  um = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:UM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=6) %>% 
    dplyr::mutate(kpi='# UM')
  sum = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SUM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=7) %>% 
    dplyr::mutate(kpi='# SUM')
  bm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:BM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=8) %>% 
    dplyr::mutate(kpi='# BM')
  sbm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SBM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=9) %>% 
    dplyr::mutate(kpi='# SBM')
  active_recruit_leader = kpi_segmentation(criteria = "where kpi='active_recruit_leader' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=10) %>% 
    dplyr::mutate(kpi='active_leader')
  Recruit_AL = kpi_segmentation(criteria = "where kpi='Recruit_AL' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=10) %>% 
    dplyr::mutate(kpi='recruit')
  rookie_in_month = kpi_segmentation(criteria = "where kpi='# Manpower_by_rookie_mdrt:Rookie in month' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=10) %>% 
    dplyr::mutate(kpi='rookie_in_month')
  
  mp_group_by_territory = rbind(
    
    us, us %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    um, um %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sum, sum %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    ,
    bm, bm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sbm, sbm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
  )
  leader =  mp_group_by_territory %>% 
            dplyr::group_by(time_view, territory, kpi='Leader', level='TERRITORY', idx=1) %>% 
            dplyr::summarise(value=sum(value, na.rm=T)) %>% 
            dplyr::ungroup(.) %>% 
            dplyr::mutate(fmt='#,##0')
  
  active_leader = rbind(
                      active_recruit_leader, active_recruit_leader %>% 
                      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                      dplyr::summarise(value=sum(value)) %>% 
                      dplyr::ungroup(.)
                    ) %>% 
                      dplyr::group_by(time_view, territory, kpi, level='TERRITORY', idx=2) %>% 
                      dplyr::summarise(value=sum(value, na.rm=T)) %>% 
                      dplyr::ungroup(.) %>% 
                      dplyr::mutate(fmt='#,##0')
  recruit = rbind(
                Recruit_AL, Recruit_AL %>% 
                dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                dplyr::summarise(value=sum(value)) %>% 
                dplyr::ungroup(.)
                ) %>% 
                dplyr::group_by(time_view, territory, kpi, level='TERRITORY', idx=3) %>% 
                dplyr::summarise(value=sum(value, na.rm=T)) %>% 
                dplyr::ungroup(.) %>% 
                dplyr::mutate(fmt='#,##0')
  
  rookie_in_month = rbind(
                rookie_in_month, rookie_in_month %>% 
                dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                dplyr::summarise(value=sum(value)) %>% 
                dplyr::ungroup(.)
                ) %>% 
                dplyr::group_by(time_view, territory, kpi, level='TERRITORY', idx=0) %>% 
                dplyr::summarise(value=sum(value, na.rm=T)) %>% 
                dplyr::ungroup(.) %>% 
                dplyr::mutate(fmt='#,##0')
    
  
  # result return
  rbind(
    leader
      ,
    active_leader
      ,
    recruit
    ,
    rookie_in_month
  )
}
Recruitment_KPI_Structure_TERRITORY  <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  us = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:US' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=5) %>% 
    dplyr::mutate(kpi='# US')
  um = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:UM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=6) %>% 
    dplyr::mutate(kpi='# UM')
  sum = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SUM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=7) %>% 
    dplyr::mutate(kpi='# SUM')
  bm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:BM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=8) %>% 
    dplyr::mutate(kpi='# BM')
  sbm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SBM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=9) %>% 
    dplyr::mutate(kpi='# SBM')
  active_recruit_leader = kpi_segmentation(criteria = "where kpi='active_recruit_leader' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=10) %>% 
    dplyr::mutate(kpi='active_leader')
  Recruit_AL = kpi_segmentation(criteria = "where kpi='Recruit_AL' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=10) %>% 
    dplyr::mutate(kpi='recruit')
  rookie_in_month = kpi_segmentation(criteria = "where kpi='# Manpower_by_rookie_mdrt:Rookie in month' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=10) %>% 
    dplyr::mutate(kpi='rookie_in_month')
  
  mp_group_by_territory = rbind(
    
    us, us %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    um, um %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sum, sum %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    ,
    bm, bm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sbm, sbm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
  )
  leader =  mp_group_by_territory %>% 
            dplyr::group_by(time_view, territory, kpi='Leader', level='TERRITORY', idx=1) %>% 
            dplyr::summarise(value=sum(value, na.rm=T)) %>% 
            dplyr::ungroup(.) %>% 
            dplyr::mutate(fmt='#,##0')
  
  active_leader = rbind(
                      active_recruit_leader, active_recruit_leader %>% 
                      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                      dplyr::summarise(value=sum(value)) %>% 
                      dplyr::ungroup(.)
                    ) %>% 
                      dplyr::group_by(time_view, territory, kpi, level='TERRITORY', idx=2) %>% 
                      dplyr::summarise(value=sum(value, na.rm=T)) %>% 
                      dplyr::ungroup(.) %>% 
                      dplyr::mutate(fmt='#,##0')
  recruit = rbind(
                Recruit_AL, Recruit_AL %>% 
                dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                dplyr::summarise(value=sum(value)) %>% 
                dplyr::ungroup(.)
                ) %>% 
                dplyr::group_by(time_view, territory, kpi, level='TERRITORY', idx=3) %>% 
                dplyr::summarise(value=sum(value, na.rm=T)) %>% 
                dplyr::ungroup(.) %>% 
                dplyr::mutate(fmt='#,##0')
  
  rookie_in_month = rbind(
                rookie_in_month, rookie_in_month %>% 
                dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                dplyr::summarise(value=sum(value)) %>% 
                dplyr::ungroup(.)
                ) %>% 
                dplyr::group_by(time_view, territory, kpi, level='TERRITORY', idx=0) %>% 
                dplyr::summarise(value=sum(value, na.rm=T)) %>% 
                dplyr::ungroup(.) %>% 
                dplyr::mutate(fmt='#,##0')
    
  active_leader_percentage = leader %>% 
                              merge(x=., y=active_leader, by.x=c('time_view','territory','level'), by.y=c('time_view','territory','level') ) %>% 
                              dplyr::mutate(value=value.y/value.x) %>% 
                              dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
                              dplyr::select(., -grep('*\\.y$',colnames(.))) %>% 
                              dplyr::mutate(kpi='%a.leader', idx=4, fmt='0.0%')
  recruit_per_active_leader = rookie_in_month %>% 
                              merge(x=., y=active_leader, by.x=c('time_view','territory','level'), by.y=c('time_view','territory','level') ) %>% 
                              dplyr::mutate(value=value.x/value.y) %>% 
                              dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
                              dplyr::select(., -grep('*\\.y$',colnames(.))) %>% 
                              dplyr::mutate(kpi=' recruit/a.leader', idx=5, fmt='#,###.0')
  
  # result return
  rbind(
    leader
      ,
    active_leader
      ,
    recruit
    ,
    active_leader_percentage
    ,
    recruit_per_active_leader
  )
}
Recruitment_KPI_Structure_COUNTRY <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  dtl = Recruitment_KPI_Structure_detail(bssdt, dbfile) %>% 
    dplyr::group_by(time_view, level='COUNTRY', kpi, fmt) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  
  leader = dtl %>% dplyr::filter(kpi=='Leader') %>% dplyr::mutate(idx=1)
  active_leader = dtl %>% dplyr::filter(kpi=='active_leader') %>% dplyr::mutate(idx=2)
  recruit = dtl %>% dplyr::filter(kpi=='recruit') %>% dplyr::mutate(idx=3)
  rookie_in_month = dtl %>% dplyr::filter(kpi=='rookie_in_month')
  

  
  active_leader_percentage = leader %>% 
    merge(x=., y=active_leader, by.x=c('time_view','level'), by.y=c('time_view','level') ) %>% 
    dplyr::mutate(value=value.y/value.x) %>% 
    dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
    dplyr::select(., -grep('*\\.y$',colnames(.))) %>% 
    dplyr::mutate(kpi='%a.leader', idx=4, fmt='0.0%')
  recruit_per_active_leader = rookie_in_month %>% 
    merge(x=., y=active_leader, by.x=c('time_view','level'), by.y=c('time_view','level') ) %>% 
    dplyr::mutate(value=value.x/value.y) %>% 
    dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
    dplyr::select(., -grep('*\\.y$',colnames(.))) %>% 
    dplyr::mutate(kpi=' recruit/a.leader', idx=5, fmt='#,###.0')
  rbind(
    leader, active_leader,recruit,active_leader_percentage,recruit_per_active_leader
  )
  
}
# Ending_MP_Structure -----------------------------------------------------


Ending_MP_Structure <- function(bssdt, dbfile = 'KPI_PRODUCTION/main_database.db'){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  total = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=1) %>% 
    dplyr::mutate(kpi='Ending Manpower_Total')
  total_excl_sa = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total (excl. SA)' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=2) %>% 
    dplyr::mutate(kpi='Ending Manpower_ExSA')
  sa = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SA' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=3) %>% 
    dplyr::mutate(kpi='# SA')
  ag = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:AG' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=4) %>% 
    dplyr::mutate(kpi='# AG')
  us = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:US' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=5) %>% 
    dplyr::mutate(kpi='# US')
  um = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:UM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=6) %>% 
    dplyr::mutate(kpi='# UM')
  sum = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SUM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=7) %>% 
    dplyr::mutate(kpi='# SUM')
  bm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:BM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=8) %>% 
    dplyr::mutate(kpi='# BM')
  sbm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SBM' and level='TERRITORY' ") %>% 
    dplyr::mutate(idx=9) %>% 
    dplyr::mutate(kpi='# SBM')
  
  mp_group_by_territory = rbind(
    total, total %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    total_excl_sa, total_excl_sa %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sa, sa %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    ,
    ag, ag %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    us, us %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    um, um %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sum, sum %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    ,
    bm, bm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
    ,
    sbm, sbm %>% 
      dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
      dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.)
    
  )
  mp_group_by_territory
}

# end of segment ----------------------------------------------------------
#



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

termination_movement <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = as.Date('2017-07-31')
  if(!inherits(business_dates, "Date")) {
    message ("the param should be a Date type")
    stop()
  }
  # my_database <- src_sqlite(dbfile, create = TRUE)
  # SQL <- sprintf(
  #   "SELECT A.AGENT_CODE, A.NEW_AGENT_DESIGNATION, B.TERRITORY, A.EFFECTIVE_FROM 
  #   FROM RAWDATA_AGENTMOVEMENT A
  #   JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.OLD_REGION_CODE = B.REGION_CODE 
  #   WHERE 
  #   EFFECTIVE_FROM LIKE '%s'
  #   AND UPPER(MOVEMENT_TYPE) = 'TERMINATION'
  #   ", 
  #   strftime(business_dates, '%Y-%m%'))
  # result <- dbSendQuery(my_database$con, SQL)
  # data = fetch(result, encoding="utf-8")
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
  # data
  
  get_Manpower(business_dates, T, dbfile) %>% 
    dplyr::filter(AGENT_STATUS=='Terminated') %>% 
    dplyr::filter(substr(BUSSINESSDATE, 1, 7)==substr(TERMINATION_DATE, 1, 7) )
    
}

reinstatement_movement <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  # business_dates = as.Date('2017-05-31')
  if(!inherits(business_dates, "Date")) {
    message ("the param should be a Date type")
    stop()
  }
  # my_database <- src_sqlite(dbfile, create = TRUE)
  # SQL <- sprintf(
  #   "SELECT A.AGENT_CODE, A.NEW_AGENT_DESIGNATION, B.TERRITORY, A.EFFECTIVE_FROM 
  #   FROM RAWDATA_AGENTMOVEMENT A
  #   JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.OLD_REGION_CODE = B.REGION_CODE 
  #   WHERE 
  #   EFFECTIVE_FROM LIKE '%s'
  #   AND UPPER(MOVEMENT_TYPE) = 'TERMINATION'
  #   ", 
  #   strftime(business_dates, '%Y-%m%'))
  # result <- dbSendQuery(my_database$con, SQL)
  # data = fetch(result, encoding="utf-8")
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
  # data
  
  get_Manpower(business_dates, T, dbfile) %>% 
    dplyr::filter(substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7) )
    
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

terminated_agents <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db'){
  p1=termination_movement(business_dates, dbfile) %>% 
    dplyr::group_by(territory = TERRITORY) %>% 
    dplyr::summarise(value=n()) %>% 
    dplyr::ungroup(.) 
  
  p1 %>% 
    dplyr::group_by(territory) %>% 
    dplyr::summarise(value=sum(value))%>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(time_view = strftime(business_dates, '%Y%m')) %>% 
    dplyr::mutate(kpi='terminated_agents') 
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
active_recruit_leader <- function(business_dates, dbfile = 'KPI_PRODUCTION/main_database.db') {
  get_Manpower(business_dates, included_ter_ag = T, dbfile) %>% 
  # filter agent whose joining dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  # chỉ đếm những leader nào tuyển AG mới, còn leader tuyển um, us... thì không đếm
  # dplyr::filter(AGENT_DESIGNATION=='AG') %>% 
  # tuy nhiên theo Châu Huỳnh thì trường hợp leader tuyển leader vẫn đếm luôn nên không lọc điều kiện này
  convert_string_to_date('BUSSINESSDATE', "%Y-%m-%d") %>% 
  # tìm các leader có tuyển dụng trong tháng
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y%m'), SUPERVISOR_CODE) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.) %>% 
  dplyr::filter(value>=1) %>% # leader recruited at least 1 agent is counted as active recruit leader
  dplyr::filter(!is.na(SUPERVISOR_CODE) & SUPERVISOR_CODE != '') %>% 
  group_by(territory=TERRITORY, time_view=BUSSINESSDATE) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.) %>% 
  dplyr::mutate(kpi='active_recruit_leader')
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
    convert_string_to_date('JOINING_DATE', '%Y-%m-%d') %>% 
    convert_string_to_date('BUSSINESSDATE', '%Y-%m-%d') %>% 
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
           template, result_file, format_string='#,##0', color="white") {
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
                    dataFormat =  xlsx::DataFormat(format_string) 
                    cs = CellStyle.default(wb) + dataFormat + xlsx::Fill(foregroundColor=color, backgroundColor=color, pattern="SOLID_FOREGROUND")  
                    xlsx::setCellStyle(c, cs)
                  })
                } else {
                  lapply(names(cells), function(x) { 
                    c = cells[[x]]
                    xlsx::setCellValue(c, v)
                    # cách này replace luôn fomula trong cell bằng v
                    # c[["Value"]] <- v
                    dataFormat =  xlsx::DataFormat(format_string) 
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
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
}

fill_excel_column1 <- function(df, sheetname, rowNameColIndex, headerRowIndex, start_writing_from_row, template, result_file) {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  for (c in names(columnNameValues)) {
    cv=columnNameValues[[c]]
    j = match(cv, colnames(df))
    if (!is.na(j)) {
      for (i in seq(1:nrow(df))) {
        v = df[i, j]
        if (!is.na(v)){
          # get digits before the dot
          rowIndex = start_writing_from_row+i-1
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", c))
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
  # set vay
  # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
  # xlsx::setCellValue(cell11, "tao day ky")
  
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  
  # autosize column widths for all the columns in the sheet
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
}

fill_excel_column1.1 <- function(df, sheetname, rowNameColIndex, headerRowIndex, start_writing_from_row, template, result_file) {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  for (c in names(columnNameValues)) {
    cv=columnNameValues[[c]]
    j = match(cv, colnames(df))
    if (!is.na(j)) {
      for (i in seq(1:nrow(df))) {
        v = df[i, j]
        if (!is.na(v)){
          # get digits before the dot
          rowIndex = start_writing_from_row+i-1
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", c))
          arow = getRows(selectedSheet, rowIndex = rowIndex)
          if (length(arow) == 0) {
            xlsx::createRow(selectedSheet, rowIndex = rowIndex)
            arow = getRows(selectedSheet, rowIndex = rowIndex)
          }
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
  # set vay
  # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
  # xlsx::setCellValue(cell11, "tao day ky")
  
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  
  # autosize column widths for all the columns in the sheet
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
}

fill_excel_column1.2 <- function(df, sheetname, rowNameColIndex, headerRowIndex, start_writing_from_row, template, result_file, color="white") {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  for (c in names(columnNameValues)) {
    cv=columnNameValues[[c]]
    j = match(cv, colnames(df))
    if (!is.na(j)) {
      for (i in seq(1:nrow(df))) {
        v = df[i, j]
        if (!is.na(v)){
          # get digits before the dot
          rowIndex = start_writing_from_row+i-1
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", c))
          arow = getRows(selectedSheet, rowIndex = rowIndex)
          if (length(arow) == 0) {
            xlsx::createRow(selectedSheet, rowIndex = rowIndex)
            arow = getRows(selectedSheet, rowIndex = rowIndex)
          }
          cells <- getCells(arow, colIndex = colIndex)
          if (is.null(cells)) {
            xlsx::createCell(arow, colIndex = colIndex)
            cells <- getCells(arow, colIndex = colIndex)
          } 
            lapply(names(cells), function(x) { 
              c = cells[[x]]
              
              # format cell value
              format_string='#,##0'
              if (endsWith(as.character(v), '%')) {
                format_string='0%'
              }
              dataFormat =  xlsx::DataFormat(format_string) 
              cs = CellStyle.default(wb) + dataFormat + xlsx::Fill(foregroundColor=color, backgroundColor=color, pattern="SOLID_FOREGROUND")  
              xlsx::setCellStyle(c, cs)
              
              ### set cell value
              if (suppressWarnings(!is.na(as.numeric(v)))) {
                v = as.numeric(v)
              } else if (endsWith(as.character(v), '%')) {
                v = gsub('%', '', v)
                if (suppressWarnings(!is.na(as.numeric(v)))) {
                  v = as.numeric(v)/100
                }
              } 
              xlsx::setCellValue(c, v)
              # c[["Value"]] <- v
            })
          
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
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
}

fill_excel_column1.3 <- function(df, sheetname, rowNameColIndex, headerRowIndex, start_writing_from_row, template, result_file, color="white") {
  if (!'fmt' %in% names(df)) {
    warning('The dataframe must have fmt column')
    stop()
  }
  # df MUST HAVE fmt column which is format string for each row
  
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  for (c in names(columnNameValues)) {
    cv=columnNameValues[[c]]
    j = match(cv, colnames(df))
    if (!is.na(j)) {
      for (i in seq(1:nrow(df))) {
        v = df[i, j]
        
          # get digits before the dot
          rowIndex = start_writing_from_row+i-1
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", c))
          arow = getRows(selectedSheet, rowIndex = rowIndex)
          if (length(arow) == 0) {
            xlsx::createRow(selectedSheet, rowIndex = rowIndex)
            arow = getRows(selectedSheet, rowIndex = rowIndex)
          }
          cells <- getCells(arow, colIndex = colIndex)
          if (is.null(cells)) {
            xlsx::createCell(arow, colIndex = colIndex)
            cells <- getCells(arow, colIndex = colIndex)
          } 
            lapply(names(cells), function(x) { 
              c = cells[[x]]
              
              # format cell value
              format_string=df[i,'fmt']
              dataFormat =  xlsx::DataFormat(format_string) 
              cs = CellStyle.default(wb) + dataFormat + xlsx::Fill(foregroundColor=color, backgroundColor=color, pattern="SOLID_FOREGROUND")  
              xlsx::setCellStyle(c, cs)
              if (!is.na(v)){
              ### set cell value
              if (suppressWarnings(!is.na(as.numeric(v)))) {
                v = as.numeric(v)
              } else if (endsWith(as.character(v), '%')) {
                v = gsub('%', '', v)
                if (suppressWarnings(!is.na(as.numeric(v)))) {
                  v = as.numeric(v)/100
                }
              } 
              xlsx::setCellValue(c, v)
              # c[["Value"]] <- v
              }
            })
          
        # }
      }
    }
  }
  # set vay
  # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
  # xlsx::setCellValue(cell11, "tao day ky")
  
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  
  # autosize column widths for all the columns in the sheet
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
  start_writing_from_row = start_writing_from_row + nrow(df)
}
fill_excel_column1.4 <- function(df, sheetname, rowNameColIndex, headerRowIndex, start_writing_from_row, template, result_file) {
  if (!'fmt' %in% names(df)) {
    warning('The dataframe must have fmt column')
    stop()
  }
  if (!'color' %in% names(df)) {
    warning('The dataframe must have color column')
    stop()
  }
  # df MUST HAVE fmt column which is format string for each row
  
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  for (c in names(columnNameValues)) {
    cv=columnNameValues[[c]]
    j = match(cv, colnames(df))
    if (!is.na(j)) {
      for (i in seq(1:nrow(df))) {
        v = df[i, j]
        
          # get digits before the dot
          rowIndex = start_writing_from_row+i-1
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", c))
          arow = getRows(selectedSheet, rowIndex = rowIndex)
          if (length(arow) == 0) {
            xlsx::createRow(selectedSheet, rowIndex = rowIndex)
            arow = getRows(selectedSheet, rowIndex = rowIndex)
          }
          cells <- getCells(arow, colIndex = colIndex)
          if (is.null(cells)) {
            xlsx::createCell(arow, colIndex = colIndex)
            cells <- getCells(arow, colIndex = colIndex)
          } 
            lapply(names(cells), function(x) { 
              c = cells[[x]]
              
              # format cell value
              format_string=df[i,'fmt']
              color=df[i,'color']
              dataFormat =  xlsx::DataFormat(format_string) 
              cs = CellStyle.default(wb) + dataFormat + xlsx::Fill(foregroundColor=color, backgroundColor=color, pattern="SOLID_FOREGROUND")  
              xlsx::setCellStyle(c, cs)
              if (!is.na(v)){
              ### set cell value
              if (suppressWarnings(!is.na(as.numeric(v)))) {
                v = as.numeric(v)
              } else if (endsWith(as.character(v), '%')) {
                v = gsub('%', '', v)
                if (suppressWarnings(!is.na(as.numeric(v)))) {
                  v = as.numeric(v)/100
                }
              } 
              xlsx::setCellValue(c, v)
              # c[["Value"]] <- v
              }
            })
          
        # }
      }
    }
  }
  # set vay
  # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
  # xlsx::setCellValue(cell11, "tao day ky")
  
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  
  # autosize column widths for all the columns in the sheet
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
  start_writing_from_row = start_writing_from_row + nrow(df)
}
fill_excel_column1.4_no_fmt <- function(df, sheetname, rowNameColIndex, headerRowIndex, start_writing_from_row, template, result_file) {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  columnNameRows <- xlsx::getRows(selectedSheet, rowIndex = headerRowIndex)  
  columnNameCells <- xlsx::getCells(columnNameRows)
  columnNameValues <- lapply(columnNameCells, getCellValue) # extract the cell values
  
  for (c in names(columnNameValues)) {
    cv=columnNameValues[[c]]
    j = match(cv, colnames(df))
    if (!is.na(j)) {
      for (i in seq(1:nrow(df))) {
        v = df[i, j]
        
          # get digits before the dot
          rowIndex = start_writing_from_row+i-1
          # get digits following by the dot
          colIndex = as.numeric(sub(".*\\.", "", c))
          arow = getRows(selectedSheet, rowIndex = rowIndex)
          if (length(arow) == 0) {
            xlsx::createRow(selectedSheet, rowIndex = rowIndex)
            arow = getRows(selectedSheet, rowIndex = rowIndex)
          }
          cells <- getCells(arow, colIndex = colIndex)
          if (is.null(cells)) {
            xlsx::createCell(arow, colIndex = colIndex)
            cells <- getCells(arow, colIndex = colIndex)
          } 
            lapply(names(cells), function(x) { 
              c = cells[[x]]
              
              if (!is.na(v)){
              ### set cell value
              if (suppressWarnings(!is.na(as.numeric(as.character(v))))) {
                v = as.numeric(v)
              } else if (endsWith(as.character(v), '%')) {
                v = gsub('%', '', v)
                if (suppressWarnings(!is.na(as.numeric(v)))) {
                  v = as.numeric(v)/100
                }
              } 
              xlsx::setCellValue(c, v)
              # c[["Value"]] <- v
              }
            })
          
        # }
      }
    }
  }
  # set vay
  # cell11 <- getCells(getRows(selectedSheet,rowIndex=4),colIndex = 5)[[1]]
  # xlsx::setCellValue(cell11, "tao day ky")
  
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
  
  # autosize column widths for all the columns in the sheet
  # xlsx::autoSizeColumn(selectedSheet, colIndex=3:(ncol(df)))
  # xlsx::setColumnWidth(selectedSheet, colIndex=3:ncol(df), colWidth=8)
  xlsx::saveWorkbook(wb, result_file)
  start_writing_from_row = start_writing_from_row + nrow(df)
}
fill_excel_cell <- function(v, sheetname, rowIndex, colIndex, template, result_file) {
  # each element in the df will be written to the excel sheet 
  # based on it colname and rowname
  wb = xlsx::loadWorkbook(template)
  sheets = xlsx::getSheets(wb)
  selectedSheet = sheets[[sheetname]]
  
  arow = getRows(selectedSheet, rowIndex = rowIndex)
  if (length(arow) == 0) {
    xlsx::createRow(selectedSheet, rowIndex = rowIndex)
    arow = getRows(selectedSheet, rowIndex = rowIndex)
  }
  cells <- getCells(arow, colIndex = colIndex)
  if (is.null(cells)) {
    xlsx::createCell(arow, colIndex = colIndex)
    cells <- getCells(arow, colIndex = colIndex)
  } 
  lapply(names(cells), function(x) { 
    c = cells[[x]]
    xlsx::setCellValue(c, v)
  })
  
  #Exporting data from R to Excel: formulas do not recalculate
  wb$setForceFormulaRecalculation(T)
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
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
  data
}
kpi_segmentation <- function(criteria='', dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = sprintf("select time_view, territory, trim(kpi) as kpi, value from kpi_segmentation %s", criteria)
  result <- dbSendQuery(my_database$con, sql)
  data = fetch(result, encoding="utf-8")
  # dbClearResult(result)
  # dbDisconnect(my_database$con)
  data
}
kpi_segmentation_1.1 <- function(criteria='', dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = sprintf("select time_view, trim(kpi) as kpi, value, territory, region, zone, team, level, fmt from kpi_segmentation %s", criteria)
  result <- dbSendQuery(my_database$con, sql)
  data = fetch(result, encoding="utf-8")
  dbClearResult(result)
  # dbDisconnect(my_database$con)
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

product_mix_calculation <- function(fromdt, bssdt, dbfile = 'KPI_PRODUCTION/main_database.db') {
  if(!inherits(fromdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  
  kpi = get_kpi(strftime(bssdt, '%Y%m%'), dbfile)
  kpi_study_period = get_kpi_in_a_period(strftime(fromdt, '%Y%m%d'), strftime(bssdt, '%Y%m%d'), dbfile)
  
  ape_by_product = kpi %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y%m%d'))) %>% 
    dplyr::group_by(time_view = strftime(BUSSINESSDATE, '%Y%m'), PRODUCTCODE) %>% 
    dplyr::summarise(APE=sum(APE, na.rm = T)/10^6)
  total_ape_by_product_in_period = kpi_study_period %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y%m%d'))) %>% 
    dplyr::group_by(time_view = strftime(bssdt, '%Y'), PRODUCTCODE) %>% 
    dplyr::summarise(APE=sum(APE, na.rm = T)/10^6)
  
  products = all_product(dbfile)
  
  products_ape = products %>% 
    merge(x=., y=ape_by_product, by.x='PRODUCTCODE', by.y='PRODUCTCODE') 
  products_ape$TOTAL_APE = sum(products_ape$APE)
  products_ape$value=products_ape$APE/products_ape$TOTAL_APE
  
  products_ape_in_period = products %>% 
    merge(x=., y=total_ape_by_product_in_period, by.x='PRODUCTCODE', by.y='PRODUCTCODE') 
  products_ape_in_period$TOTAL_APE = sum(products_ape_in_period$APE)
  products_ape_in_period$value=products_ape_in_period$APE/products_ape_in_period$TOTAL_APE
  
  
  rbind(
    products_ape %>% 
      dplyr::filter(PRODUCT_TYPE=='BASEPRODUCT') %>% 
      dplyr::mutate(product_code=PRODUCTCODE, product_name=PRODUCTNAME) %>% 
      dplyr::select(-VIETNAMESENAME, -PRODUCTCODE, -PRODUCTNAME, -PRODUCT_TYPE, -APE, -TOTAL_APE)
    ,
    products_ape %>% dplyr::filter(PRODUCT_TYPE=='RIDER') %>% 
      dplyr::group_by(time_view, PRODUCT_TYPE) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::select(-PRODUCT_TYPE) %>% 
      dplyr::mutate(product_code='RIDER', product_name='')
    ,
    products_ape %>% dplyr::filter(PRODUCT_TYPE=='SME') %>% 
      dplyr::group_by(time_view, PRODUCT_TYPE) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::select(-PRODUCT_TYPE) %>% 
      dplyr::mutate(product_code='SME', product_name='')
    ,
    #---total col
    products_ape_in_period %>% 
      dplyr::filter(PRODUCT_TYPE=='BASEPRODUCT') %>% 
      dplyr::mutate(product_code=PRODUCTCODE, product_name=PRODUCTNAME) %>% 
      dplyr::select(-VIETNAMESENAME, -PRODUCTCODE, -PRODUCTNAME, -PRODUCT_TYPE, -APE, -TOTAL_APE)
    ,
    products_ape_in_period %>% dplyr::filter(PRODUCT_TYPE=='RIDER') %>% 
      dplyr::group_by(time_view, PRODUCT_TYPE) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::select(-PRODUCT_TYPE) %>% 
      dplyr::mutate(product_code='RIDER', product_name='')
    ,
    products_ape_in_period %>% dplyr::filter(PRODUCT_TYPE=='SME') %>% 
      dplyr::group_by(time_view, PRODUCT_TYPE) %>% 
      dplyr::summarise(value=sum(value)) %>% 
      dplyr::ungroup(.) %>% 
      dplyr::select(-PRODUCT_TYPE) %>% 
      dplyr::mutate(product_code='SME', product_name='')
  )
}


product_mix <- function(criteria='', orderby, addTotalRow=F, dbfile = 'KPI_PRODUCTION/main_database.db') {
  my_database <- src_sqlite(dbfile, create = TRUE)
  sql = sprintf("select * from product_mix %s", criteria)
  result <- dbSendQuery(my_database$con, sql)
  t2 = fetch(result, encoding="utf-8") %>% dplyr::mutate(idx=0)
  dbClearResult(result)
  
  if (addTotalRow) {
    t2 = rbind(
      t2,
      t2 %>% 
        dplyr::group_by(time_view) %>% 
        dplyr::summarise(value=sum(value)) %>% 
        dplyr::mutate(product_code='Basic Total', product_name='', idx=1) %>% 
        dplyr::ungroup(.)
      
    )
  }
  try((t2['NA'==(t2$product_name),]$product_name <- ''), silent=T)
  try((t2['(blank)'==(t2$product_name),]$product_name <- ''), silent=T)
  t3=tidyr::spread(t2, time_view, value) 
  t3=dplyr::arrange(t3, t3$idx, desc(t3[orderby])) %>% dplyr::select(-idx)
  # t3=t3[order(t3$Total, decreasing = T),]
  # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'product_code'])
  
  t3 %>% dplyr::mutate(fmt='0.0%')
  
}
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

get_genlion_final_date <- function(bssdt){
  bssdt %>% quarters()
}
# DATE UTIL ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
generate_last_day_of_quarter <- function(yyyy) {
  fr <- as.Date(sprintf('%s-01-01', yyyy))
  to <- as.Date(sprintf('%s-12-31', yyyy))
  last_day_of_quarters <- seq(from=seq(fr, length=2, by='1 quarter')[2], to=seq(to, length=2, by='1 quarter')[2], by="1 quarter") - 1
  last_day_of_quarters
}

generate_last_day_of_q1 <- function(bssdt) {
  as.Date(cut(bssdt, "quarter")) - 1
}

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

get_last_day_of_next_m1 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=6, by='1 month')-1
  re[3]
}
get_last_day_of_next_m2 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=6, by='1 month')-1
  re[4]
}
get_last_day_of_next_m3 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=6, by='1 month')-1
  re[5]
}
get_last_day_of_next_m4 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=6, by='1 month')-1
  re[6]
}
get_last_day_of_next_m5 <- function(yyyy, mm) {
  dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
  re <- seq(dt, length=7, by='1 month')-1
  re[7]
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
get_last_day_of_month_x <- function(yyyy, mm, x) {
  if (x == 0) {
    get_last_day_of_m0(yyyy, mm)
  } else {
    dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
    re <- seq(dt, length=x, by='-1 month')-1
    re[x]
  }
}
get_last_day_of_month_x_v1 <- function(yyyy, mm, x) {
  if (x == 0) {
    time_view = get_last_day_of_m0(yyyy, mm)
    data.frame(time_view, M='M0')
  } else {
    dt <- as.Date(sprintf('%s-%s-01', yyyy, mm))
    re <- seq(dt, length=x, by='-1 month')-1
    time_view = re[x]
    data.frame(time_view, M=sprintf('M%s', x))
  }
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
      # db_insert_into(con = my_database$con, table = tablename, values = result) # insert data to the table
      insert_or_replace_bulk(result, tablename, dbfile, bulksize = 1000)
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
import_kpiproduction_msaccess_drop_then_create_new_table <- function(tablenames=NA, accessdb="KPI_PRODUCTION/KPI_PRODUCTION.accdb", dbfile = 'KPI_PRODUCTION/main_database.db') {
  # tablenames=c('KPITotal','Manpower_Active Ratio')
  # accessdb="d:/Data/DA_201711/KPI_PRODUCTION.accdb"
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
      # results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
      # dbClearResult(results)
      # db_insert_into(con = my_database$con, table = tablename, values = result)
      results <- dbSendQuery(my_database$con, sprintf("drop table %s", tablename))
      dbClearResult(results)
      copy_to(my_database, result, name=tablename, temporary = FALSE)
    }
  }
  close(accessConn) 
  dbDisconnect(my_database$con)
}

import_lifeasia_data <- function(folder, dbfile) {
  library(readr)
  HPADPF <- read_csv(sprintf("%s//HPADPF.csv", folder))
  insert_or_replaceall(HPADPF, 'HPADPF', dbfile)
  CHDRPF <- read_csv(sprintf("%s//CHDRPF.csv", folder))
  insert_or_replaceall(CHDRPF, 'CHDRPF', dbfile)
  COVTPF <- read_csv(sprintf("%s//COVTPF.csv", folder))
  insert_or_replaceall(COVTPF, 'COVTPF', dbfile)
  COVRPF <- read_csv(sprintf("%s//COVRPF.csv", folder))
  insert_or_replaceall(COVRPF, 'COVRPF', dbfile)
}

import_kpiproduction_msaccess_GENLION_REPORT <- function(BUSSINESSDATE, accessdb="KPI_PRODUCTION/KPI_PRODUCTION.accdb", dbfile = 'KPI_PRODUCTION/main_database.db') {
  tablenames='Genlion_Report'
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
    result['BUSSINESSDATE']=BUSSINESSDATE
    
    tablename <- gsub(' ', '', tablename) # replace all special characters from the table name 
    tablename <- gsub('&', '', tablename) # replace all special characters from the table name 
    tablename <- gsub('-', '', tablename) # replace all special characters from the table name
    tablename <- gsub('%', '', tablename) # replace all special characters from the table name
    tablename <- sprintf('RAWDATA_%s', tablename)
    if (!(tablename %in% dbListTables(my_database$con))) {
      copy_to(my_database, result, name=tablename, temporary = FALSE)
    } else {
      results <- dbSendQuery(my_database$con, sprintf("delete FROM %s WHERE BUSSINESSDATE= '%s'", tablename, BUSSINESSDATE))
      dbClearResult(results)
      db_insert_into(con = my_database$con, table = tablename, values = result)
    }
  }
  close(accessConn) 
  dbDisconnect(my_database$con)
}
import_kpiproduction_msaccess_Manpower_Active_Ratio_2015_forward <- function(BUSSINESSDATE, accessdb="KPI_PRODUCTION/KPI_PRODUCTION.accdb", dbfile = 'KPI_PRODUCTION/main_database.db') {
  tablenames='Manpower_Active Ratio'
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
    accessSQL = sprintf("SELECT * from [%s] WHERE right(BUSSINESSDATE,4)=%s", tablename, BUSSINESSDATE)
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
      results <- dbSendQuery(my_database$con, sprintf("delete FROM %s WHERE BUSSINESSDATE LIKE '%s%s'", tablename, BUSSINESSDATE, '%'))
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
  # df=df
  # excelfile=excelFile
  # newexcelfile = excelFile
  # sheetname="Agency North"
  # visible=F
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
    stop()
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

RDCOMClient_fill_excel_column <- function(df, excelfile, newexcelfile, sheetname, visible, rowNameColIndex, headerRowIndex, start_writing_from_row) {
  ############################
  # df=df
  # excelfile=excelFile
  # newexcelfile = excelFile
  # sheetname="Agency North"
  # visible=F
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
        for (colidx in seq(colcount)) {
          colidx_val = sheet$Cells(headerRowIndex, colidx)$Value()
          if (!is.null(colidx_val)) {
            
            j = match(colidx_val, colnames(df))
            
            if (!is.na(j)) {
              for (i in seq(nrow(df))) {
                v = df[i, j]
                if (!is.na(v)){
                  rowidx = start_writing_from_row+i-1
                  cell  <- sheet$Cells(rowidx,colidx)
                  cell[["Value"]] <- v
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
    stop()
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

count_group_by <- function(df, f1 = NA, f2 = NA, f3 = NA, f4 = NA, f5 = NA, f6 = NA, f7 = NA, f8 = NA, f9 = NA, f10 = NA, summarise_field_name){
  segs =data.frame(
    segment = c("'COUNTRY'", 'TERRITORY', 'REGIONCD', 'ZONECD', 'OFFICECD', 'TEAMCD'),
    group_level = c("'COUNTRY'", "'TERRITORY'", "'REGIONCD'", "'ZONECD'", "'OFFICECD'", "'TEAMCD'"),
    order_idx = c(0, 1, 2, 3, 4, 5)
  )
  
  re = do.call(rbind, apply(segs, 1, function(seg){
    
    n1 =  names(segs)[1]
    b1 = seg[1]
    
    n2 =  names(segs)[2]
    b2 = seg[2]
    
    n3 =  names(segs)[3]
    b3 = seg[3]
    
    tem = df %>% dplyr::group_by_(
      n1 = b1,
      n2 = b2,
      n3 = b3,
      'f1' = ifelse(is.na(f1), "'fx'", f1),
      'f2' = ifelse(is.na(f2), "'fx'", f2),
      'f3' = ifelse(is.na(f3), "'fx'", f3),
      'f4' = ifelse(is.na(f4), "'fx'", f4),
      'f5' = ifelse(is.na(f5), "'fx'", f5),
      'f6' = ifelse(is.na(f6), "'fx'", f6),
      'f7' = ifelse(is.na(f7), "'fx'", f7),
      'f8' = ifelse(is.na(f8), "'fx'", f8),
      'f9' = ifelse(is.na(f9), "'fx'", f9),
      'f10' = ifelse(is.na(f10), "'fx'", f10)
    ) %>%
      dplyr::summarise_(v = 'n()') %>%
      dplyr::ungroup(.)
    tem[, n1] = tem$n1
    tem[, n2] = tem$n2
    tem[, n3] = tem$n3
    tem %>% dplyr::select(-n1, -n2, -n3)
  }))
  
  re[,summarise_field_name] = re$v
  if (!is.na(f1)) {
    re[,f1] = re$f1
  }
  if (!is.na(f2)) {
    re[,f2] = re$f2
  }
  if (!is.na(f3)) {
    re[,f3] = re$f3
  }
  if (!is.na(f4)) {
    re[,f4] = re$f4
  }
  if (!is.na(f5)) {
    re[,f5] = re$f5
  }
  if (!is.na(f6)) {
    re[,f6] = re$f6
  }
  if (!is.na(f7)) {
    re[,f7] = re$f7
  }
  if (!is.na(f8)) {
    re[,f8] = re$f8
  }
  if (!is.na(f9)) {
    re[,f9] = re$f9
  }
  if (!is.na(f10)) {
    re[,f10] = re$f10
  }
  re %>% dplyr::select(-v, -f1, -f2, -f3, -f4, -f5, -f6, -f7, -f8, -f9, -f10)
}

