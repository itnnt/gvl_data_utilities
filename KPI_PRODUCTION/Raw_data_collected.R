library(RODBC)
library(dplyr)
library(RSQLite)

# CONNECT TO KPI_PRODUCTION.accdb ####
accessConn<-odbcConnectAccess2007("KPI_PRODUCTION/KPI_PRODUCTION.accdb", "utf-8")
#accessConn<-odbcConnectAccess2007("KPI_PRODUCTION/KPI_PRODUCTION.accdb", DBMSencoding='UTF-8')
# View(sqlTables(db))
# # __ FETCH TABLE KPITotal FROM KPI_PRODUCTION.accdb ####
# KPITotal <- sqlFetch(db, 'KPITotal')
# # __ FETCH TABLE Manpower_Active Ratio FROM KPI_PRODUCTION.accdb ####
# Manpower_Active_Ratio <- sqlFetch(db, 'Manpower_Active Ratio')
# # __ FETCH TABLE Product FROM KPI_PRODUCTION.accdb ####
# Product <- sqlFetch(accessConn, 'Product')
# # __ FETCH TABLE ADLIST FROM KPI_PRODUCTION.accdb ####
# sqlFetch(db, fetch = "SET NAMES 'utf8';")
# ADLIST <- sqlFetch(db, 'ActiveList')

all_tables <- subset(sqlTables(accessConn), 
                     TABLE_TYPE == "TABLE", 
                     TABLE_NAME) # get all tables' name from the data base

# CONNECT TO KPI_PRODUCTION.db ####
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)
excluded_tables <- c('RAWDATA_TeamAL_RecruitAL')
for (i in 1:nrow(all_tables)) {
  tablename <- (all_tables[i,'TABLE_NAME'])
  print(sprintf('Data copying for %s', tablename))
  result <- sqlFetch(accessConn, tablename, stringas) # fetch data from tables
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
  if (!(tablename %in% excluded_tables)) {
    if (!(tablename %in% dbListTables(my_database$con))) {
      copy_to(my_database, result, name=tablename, temporary = FALSE)
    } else {
      results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename))
      dbClearResult(results)
      db_insert_into(con = my_database$con, table = tablename, values = result)
    }
  }
}
close(accessConn) 




# Import GenClubs_Q12017.xlsx ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
library(readxl)
GenClubs_Q12017 <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/GenClubs_Q12017.xlsx")
# Import GenClubs_Q12017.xlsx ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Mapping_Territory_Region.xlsx -------------------------------------------------------------------------------------------------------------------------------------------------------------------
result <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/Mapping_Territory_Region.xlsx")
fieldnames <- names(result) # get all fields' name
fieldnames <- gsub(' ', '_', fieldnames) # replace all space characters by _
names(result) <- toupper(fieldnames) # reset fields' name by their upper cases
tablename <- 'RAWDATA_MAPPING_TERRITORY_REGION'
if (!(tablename %in% excluded_tables)) { # some tables will be excluded from importing data
  if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
    copy_to(my_database, result, name=tablename, temporary = FALSE) # save data to a new table
  } else { # the table has been created before
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename)) # clear data from the table
    dbClearResult(results) # clear result set
    db_insert_into(con = my_database$con, table = tablename, values = result) # insert data to the table
  }
}
# Mapping_Territory_Region.xlsx -------------------------------------------------------------------------------------------------------------------------------------------------------------------


# Import REF_TRANSACTION_CODE.xlsx ----------------------------------------------------------------------------------------------------------------------------------------------------------------
REF_TRANSACTION_CODE <- read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/input/REF_TRANSACTION_CODE.xlsx")
# Import REF_TRANSACTION_CODE.xlsx ----------------------------------------------------------------------------------------------------------------------------------------------------------------




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
# results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", 'RAWDATA_Manpower_ActiveRatio'))
# dbClearResult(results)
# result[] <- lapply(result, function(x) if(inherits(x, "POSIXct")) strftime(x, '%Y-%m-%d') else x)
# db_insert_into(con = my_database$con, table = 'RAWDATA_Manpower_ActiveRatio', values = result)
# # dbWriteTable(conn = my_database$con, name = 'RAWDATA_Manpower_ActiveRatio', value = result, append=TRUE)
# 



