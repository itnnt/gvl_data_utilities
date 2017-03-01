require(RJDBC)

COMPANY_EDM_VN <-
  "jdbc:sqlserver://10.10.2.6:1433;databaseName=COMPANY_EDM_VN;SelectMethod=cursor;useUnicode=true;characterEncoding=UTF-8"

COMPANY_EDM_UAT_VN <-
  "jdbc:sqlserver://10.10.1.148:1433;databaseName=COMPANY_EDM_UAT_VN;SelectMethod=cursor;useUnicode=true;characterEncoding=UTF-8"

COMPANY_EDM_DAY2_22May2PM <-
  "jdbc:sqlserver://10.10.1.148:1433;databaseName=COMPANY_EDM_DAY2_22May2PM;SelectMethod=cursor;useUnicode=true;characterEncoding=UTF-8"

COMPANY_EDM_VN_UAT6 <-
  "jdbc:sqlserver://10.10.2.20/EDMDB02:1433;databaseName=COMPANY_EDM_VN_UAT6;SelectMethod=cursor;useUnicode=true;characterEncoding=UTF-8"

COMPANY_EDM_VN_DR <-
  "jdbc:sqlserver://GVLHCMHOFIS01\\DR_EDM_01:1433;databaseName=COMPANY_EDM_VN_DR"

##########################################################################################

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
    
  } else if (env %in% c("COMPANY_EDM_DAY2_22May2PM")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_DAY2_22May2PM, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_VN_UAT6")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_VN_UAT6, "vsql0103", "Life123456@")
    
  } else if (env %in% c("COMPANY_EDM_VN_DR")) {
    conn <-
      dbConnect(drv, COMPANY_EDM_VN_DR, "vsql0103", "Life123456@")
  }
  conn
}


execute_sql <- function(sql, env) {
  conn = connectdb(env)
  queryResults <- dbGetQuery(conn, sql)
  dbDisconnect(conn)
  queryResults
}
