source("KPI_PRODUCTION/set_environment.r")
source('KPI_PRODUCTION/Common functions.R')
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


import_agent_list <- function(range='A6:DA', dbfile = 'KPI_PRODUCTION/main_database.db') {
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

import_mdrt <- function() {
  dta <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/MDRT.xlsx")
  # convert all POSIXct columns to string columns
  dta[] <- lapply(dta, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) 
  tablename <- 'RAWDATA_MDRT'
    if (!(tablename %in% dbListTables(my_database$con))) {
      copy_to(my_database, dta, name=tablename, temporary = FALSE)
    } else {
      results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
      dbClearResult(results)
      db_insert_into(con = my_database$con, table = tablename, values = dta)
    }
}
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Connect to the main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)



# chi lay du lieu cua nhung bang dang dung cho report 
# test --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
accessSQL = "SELECT * FROM AgentList"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns
insert_or_replaceall(result, 'AgentList')

accessSQL = "SELECT * FROM GenLion"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns
insert_or_replaceall(result, 'RAWDATA_GenLion')

accessSQL = "SELECT * from KPITotal"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns

accessSQL = "SELECT * from MDRT"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns


accessSQL = "SELECT * from Product"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns


accessSQL = "SELECT * from tblArea_Region"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns


### qrySA ####
### ##########
accessSQL = "SELECT BussinessDate, MAR.[Agent Code], MAR.[Agent Name], MAR.[AGENT DESIGNATION], MAR.MonthStart, MAR.MonthEnd, MAR.Active, MAR.ActiveEX, MAR.ActiveSP, MAR.[Servicing agent]
FROM [Manpower_Active Ratio] MAR
WHERE MAR.[Servicing agent]='Yes' ;"
result <- RODBC::sqlQuery(accessConn, accessSQL)
result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) # convert all POSIXct columns to string columns


# test ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------





# Area_Region -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
library(readxl)
tblArea_Region <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/tblArea_Region.xlsx")
tablename <- 'tblArea_Region'
if (!(tablename %in% excluded_tables)) {
  if (!(tablename %in% dbListTables(my_database$con))) {
    copy_to(my_database, tblArea_Region, name=tablename, temporary = FALSE)
  } else {
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
    dbClearResult(results)
    db_insert_into(con = my_database$con, table = tablename, values = tblArea_Region)
  }
}

# qrySA -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
qrySA <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/qrySA.xlsx")
tablename <- 'qrySA'
if (!(tablename %in% excluded_tables)) {
  if (!(tablename %in% dbListTables(my_database$con))) {
    copy_to(my_database, qrySA, name=tablename, temporary = FALSE)
  } else {
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
    dbClearResult(results)
    db_insert_into(con = my_database$con, table = tablename, values = qrySA)
  }
}


# GenLion -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
GenLion <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/GenLion.xlsx")
tablename <- 'GenLion'
if (!(tablename %in% excluded_tables)) {
  if (!(tablename %in% dbListTables(my_database$con))) {
    copy_to(my_database, GenLion, name=tablename, temporary = FALSE)
  } else {
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
    dbClearResult(results)
    db_insert_into(con = my_database$con, table = tablename, values = GenLion)
  }
}


























# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import GenClubs_Q12017.xlsx ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
GenClubs_Q12017 <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/GenClubs_Q12017.xlsx")
fieldnames <- names(GenClubs_Q12017) # get all field names in the result
fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
names(GenClubs_Q12017) <- toupper(fieldnames) # rename all fields in the result
# convert all POSIXct columns to string columns
GenClubs_Q12017[] <- lapply(GenClubs_Q12017, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x) 
tablename <- sprintf('%s', 'GENCLUB')
  if (!(tablename %in% dbListTables(my_database$con))) {
    copy_to(my_database, GenClubs_Q12017, name=tablename, temporary = FALSE)
  } else {
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
    dbClearResult(results)
    db_insert_into(con = my_database$con, table = tablename, values = GenClubs_Q12017)
  }

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Mapping_Territory_Region.xlsx -------------------------------------------------------------------------------------------------------------------------------------------------------------------
result <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/Mapping_Territory_Region.xlsx")
fieldnames <- names(result) # get all fields' name
fieldnames <- gsub(' ', '_', fieldnames) # replace all space characters by _
names(result) <- toupper(fieldnames) # reset fields' name by their upper cases
tablename <- 'MAPPING_TERRITORY_REGION'
if (!(tablename %in% excluded_tables)) { # some tables will be excluded from importing data
  if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
    copy_to(my_database, result, name=tablename, temporary = FALSE) # save data to a new table
  } else { # the table has been created before
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename)) # clear data from the table
    dbClearResult(results) # clear result set
    db_insert_into(con = my_database$con, table = tablename, values = result) # insert data to the table
  }
}

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Product -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
result <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/Product.xlsx")
fieldnames <- names(result) # get all fields' name
fieldnames <- gsub(' ', '_', fieldnames) # replace all space characters by _
names(result) <- toupper(fieldnames) # reset fields' name by their upper cases
tablename <- 'GVL_PRODUCT'
if (!(tablename %in% excluded_tables)) { # some tables will be excluded from importing data
  if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
    copy_to(my_database, result, name=tablename, temporary = FALSE) # save data to a new table
  } else { # the table has been created before
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename)) # clear data from the table
    dbClearResult(results) # clear result set
    db_insert_into(con = my_database$con, table = tablename, values = result) # insert data to the table
  }
}


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import REF_TRANSACTION_CODE.xlsx ----------------------------------------------------------------------------------------------------------------------------------------------------------------
REF_TRANSACTION_CODE <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/REF_TRANSACTION_CODE.xlsx")



# MAIN PROCESS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(interactive()) {
  # import_agent_list('A6:DA')
  # import_agent_list('A1:DA')
  # import_mdrt()
  # 
  import_agentlist_from_msaccess("20160131", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201601/KPI_PRODUCTION_20160131.accdb")
  import_agentlist_from_msaccess("20160229", accessdb="d:/Data/DA_201602/KPI_PRODUCTION_20160229.accdb")
  import_agentlist_from_msaccess("20160331", accessdb="d:/Data/DA_201603/KPI_PRODUCTION_20160331.accdb")
  import_agentlist_from_msaccess("20160430", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201604/KPI_PRODUCTION_20160429-30.accdb")
  import_agentlist_from_msaccess("20160531", accessdb="d:/Data/DA_2016/KPI_PRODUCTION_20160531.accdb")
  import_agentlist_from_msaccess("20160630", accessdb="d:/Data/DA_2016/KPI_PRODUCTION_20160630.accdb")
  import_agentlist_from_msaccess("20160731", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201607/KPI_PRODUCTION_20160729-31.accdb")
  import_agentlist_from_msaccess("20160831", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201608/KPI_PRODUCTION_20160831.accdb")
  import_agentlist_from_msaccess("20160930", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201609/KPI_PRODUCTION_20160929.accdb")
  import_agentlist_from_msaccess("20161031", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201610/KPI_PRODUCTION_20161031.accdb")
  import_agentlist_from_msaccess("20161130", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201611/KPI_PRODUCTION_20161130.accdb")
  import_agentlist_from_msaccess("20161231", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201612/KPI_PRODUCTION_20161231.accdb")
  
  import_agentlist_from_msaccess("20170131", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201701/KPI_PRODUCTION_20170131.accdb")
  import_agentlist_from_msaccess("20170228", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201702/KPI_PRODUCTION_20170228.accdb")
  import_agentlist_from_msaccess("20170331", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201703/KPI_PRODUCTION_20170331.accdb")
  import_agentlist_from_msaccess("20170430", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201704/KPI_PRODUCTION_20170430.accdb")
  import_agentlist_from_msaccess("20170531", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201705/KPI_PRODUCTION_20170531.accdb")
  import_agentlist_from_msaccess("20170630", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201706/KPI_PRODUCTION_20170630.accdb")
  import_agentlist_from_msaccess("20170731", accessdb="d:/Data/DA_201707/KPI_PRODUCTION.accdb")
  import_agentlist_from_msaccess("20170831", accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  
  import_kpiproduction_msaccess_GENLION_REPORT("20170430", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201704/KPI_PRODUCTION_20170430.accdb")
  import_kpiproduction_msaccess_GENLION_REPORT("20170531", accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201705/KPI_PRODUCTION_20170531.accdb")
  import_kpiproduction_msaccess_GENLION_REPORT('20170630', accessdb="t:/AGY/AA/Van/EDM/KPI_PRODUCTION/KPI_PRODUCTION_201706/KPI_PRODUCTION_20170630.accdb")
  
  import_kpiproduction_msaccess(c('KPITotal','Manpower_Active Ratio'), accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  import_kpiproduction_msaccess(c('AgentMovement'), accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  import_kpiproduction_msaccess(c('Promotion_Demotion_SBM_SUM'), accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  import_kpiproduction_msaccess(c('Persistency'), accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  import_kpiproduction_msaccess(c('Persistency_Y2'), accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  import_kpiproduction_msaccess_GENLION_REPORT('20170831', accessdb="d:/Data/DA_201708/KPI_PRODUCTION.accdb")
  
  # import_kpiproduction_msaccess(c('GATotal'), accessdb="d:/Data/DA_201707/KPI_PRODUCTION.accdb")
  GATotal <- read_excel("d:\\Data\\DA_201708\\GATotal.xlsx")
  fieldnames <- names(GATotal) # get all field names in the result
  fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
  names(GATotal) <- toupper(fieldnames) # rename all fields in the result
  insert_or_replace_bulk(GATotal, 'RAWDATA_GATotal', dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db') # save to database

  ADLIST <- read_excel("d:\\Data\\DA_201708\\ADLIST.xlsx")
  fieldnames <- names(ADLIST) # get all field names in the result
  fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
  names(ADLIST) <- toupper(fieldnames) # rename all fields in the result
  insert_or_replaceall(ADLIST, 'RAWDATA_ADLIST', dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db') # save to database
  
}
RODBC::odbcCloseAll()

#import lai agent list cac ngay nay do bi trung lap du lieu
#'2015-05-31',
# '2015-06-30',
# '2015-07-31',
# '2015-09-30',
# '2015-11-30',
# '2017-04-30'


# 
# # test ####
# Sys.setlocale(category="LC_ALL", locale = "vietnamese")
# result <- sqlFetch(accessConn, 'Manpower_Active Ratio', stringsAsFactors = F)
# 
# Product <- sqlFetch(accessConn, 'Product', stringsAsFactors = F)
# # # __ FETCH TABLE ADLIST FROM KPI_PRODUCTION.accdb ####
# sqlFetch(accessConn, 'Product', fetch = "SET NAMES 'utf8';")
# 
# fieldnames <- names(result) # get all field names in the result
# fieldnames <- gsub(' ', '_', fieldnames) # replace all ' ' characters in the fieldnames by '_'
# names(result) <- fieldnames # rename all fields in the result
# results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", 'Manpower_ActiveRatio'))
# dbClearResult(results)
# result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x)
# db_insert_into(con = my_database$con, table = 'Manpower_ActiveRatio', values = result)
# # dbWriteTable(conn = my_database$con, name = 'Manpower_ActiveRatio', value = result, append=TRUE)
# 



