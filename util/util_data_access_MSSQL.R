library(RJDBC)
#---- connection string ----
COMPANY_EDM_VN <-
  "jdbc:sqlserver://10.10.2.6;databaseName=COMPANY_EDM_VN"

COMPANY_EDM_UAT_VN <-
  "jdbc:sqlserver://10.10.1.148:1433;databaseName=COMPANY_EDM_UAT_VN"

COMPANY_EDM_SIT <-
  "jdbc:sqlserver://10.10.1.147:1433;databaseName=COMPANY_EDM_SIT"

COMPANY_EDM_DAY2_22May2PM <-
  "jdbc:sqlserver://10.10.1.147:1433;databaseName=COMPANY_EDM_DAY2_22May2PM"

COMPANY_EDM_UAT_4 <-
  "jdbc:sqlserver://10.10.1.147:1433;databaseName=COMPANY_EDM_UAT_4"

COMPANY_EDM_VN_UAT6 <-
  "jdbc:sqlserver://10.10.2.20\\EDMDB02;databaseName=COMPANY_EDM_VN_UAT6"

COMPANY_EDM_VN_DR <-
  "jdbc:sqlserver://GVLHCMHOFIS01\\DR_EDM_01:1433;databaseName=COMPANY_EDM_VN_DR"

SIS_PRD <-
  "jdbc:sqlserver://10.10.2.16;databaseName=sis_prd"


#---- connect to sqlserver database ----
connectdb <- function(env) {
  driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  sqljdbc_path = paste(getwd(), 'util', "sqljdbc4-4.0.2206.100.jar", sep =
                         '/')
  drv <-
    JDBC(driverClass = driver_class, sqljdbc_path)
  if (env %in% c("COMPANY_EDM_VN")) {
    conn <- dbConnect(drv, COMPANY_EDM_VN, "vreport0101", "3dmR3p0rt@")
    
  } else if (env %in% c("COMPANY_EDM_UAT_VN")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_UAT_VN, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_SIT")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_SIT, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_DAY2_22May2PM")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_DAY2_22May2PM, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_UAT_4")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_UAT_4, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_VN_UAT6")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_VN_UAT6, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_VN_DR")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_VN_DR, "vsql0103", "Life123456@")
  
  } else if (env %in% c("SIS_PRD")) {
    conn <-
      dbConnect(drv, SIS_PRD, "vsis", "vsis#gvl#2016")
  }
  conn
}

##---- execute sql statement ----
execute_sql <- function(sql, env) {
  options(java.parameters = "-Xmx2048m") # extend java memory
  conn = connectdb(env)
  queryResults <- dbGetQuery(conn, sql)
  dbDisconnect(conn)
  queryResults
}

execute_update <- function(sql, env) {
  conn = connectdb(env)
  queryResults <- RJDBC::dbSendUpdate(conn, sql)
  dbDisconnect(conn)
  queryResults
}

execute_store_procedure <- function(sql, env) {
  options(java.parameters = "-Xmx1024m") # extend java memory
  conn = connectdb(env)
  sql <- paste("set nocount on \n", sql, sep = '')
  queryResults <- RJDBC::dbGetQuery(conn, sql)
  dbDisconnect(conn)
  queryResults
}

#TODO: CHUA TEST
execute_sqlfile <- function(sql_file, env) {
  con <- connectdb(env)
  queries <- readLines(sql_file, skipNul = TRUE, encoding = 'UTF-8')
  sapply(queries, function(x) RJDBC::dbSendUpdate(con,x))
  dbDisconnect(con)
}