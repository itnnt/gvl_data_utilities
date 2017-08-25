source("KPI_PRODUCTION/set_environment.r")
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import_agentlist_from_msaccess <- function(BUSSINESSDATE, accessdb, dbfile) {
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
    
    tablename <- 'GVL_AGENTLIST_new'
    
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

import_agent_list <- function(range='A6:DA', dbfile) {
  my_database <- src_sqlite(dbfile, create = TRUE)
  files <- list.files(path = './KPI_PRODUCTION/input/AgentList/',pattern = "\\.xls$") # get files' name in the folder
  for (f in files) {
    print(f)
    # AgentList <- read_excel(sprintf("./KPI_PRODUCTION/input/AgentList/%s",'AgentList-test.xls'), col_names = TRUE, sheet = 1 )
    BUSSINESSDATE <- substr(gsub('AgentList-','',f), 1, 8)
    
    conn = odbcConnectExcel(sprintf("./KPI_PRODUCTION/input/AgentList/%s",f), readOnly = T) # open a connection to the Excel file
    result = sqlQuery(conn, sprintf("select * from [%s%s]", sqlTables(conn)$TABLE_NAME[1], range)) # read a sheet (alternative SQL sintax)
    
    fieldnames <- names(result) # get all field names in the result
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
    if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
      copy_to(my_database, result, name=tablename, temporary = FALSE) # save data to a new table
    } else { 
      # the table has been created before
      # results <- dbSendQuery(my_database$con, sprintf("SELECT * FROM %s WHERE BUSSINESSDATE='%s'",
      #                                                 tablename,
      #                                                 strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')),'%Y-%m-%d'))) # clear data from the table
      # agents <- fetch(results, encoding="utf-8")
      # dbClearResult(results) # clear result set
      # # if number of agents in excel file differs from one in the database
      # if (nrow(agents) != nrow(result)) {
        results <- dbSendQuery(my_database$con, sprintf("delete FROM %s WHERE BUSSINESSDATE='%s'",
                                                        tablename,
                                                        strftime(as.Date(strptime(BUSSINESSDATE, '%Y%m%d')),'%Y-%m-%d')
        )) # clear data from the table
        dbClearResult(results) # clear result set
        result <- dplyr::filter(result, !(is.na(AGENTCD)) & AGENTCD!='' )
        db_insert_into(con = my_database$con, table = tablename, values = result) # insert data to the table
      # }
    }
  }
  dbDisconnect(my_database$con)
  RODBC::odbcCloseAll()
}

import_kpiproduction_msaccess <- function(tablenames=NA, accessdb, dbfile) {
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


# MAIN PROCESS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(interactive()) {
  import_agentlist_from_msaccess("20170731", accessdb="d:/Data/DA_201706/KPI_BANCA.accdb", dbfile="KPI_PRODUCTION/main_database_BANCASSURANCE.db")
  import_kpiproduction_msaccess(c('KPITotal','Manpower_Active Ratio'), accessdb="d:/Data/DA_201706/KPI_BANCA.accdb", dbfile="KPI_PRODUCTION/main_database_BANCASSURANCE.db")
  import_kpiproduction_msaccess(c('Product'), accessdb="d:/Data/DA_201706/KPI_PRODUCTION.accdb", dbfile="KPI_PRODUCTION/main_database_BANCASSURANCE.db")
}
