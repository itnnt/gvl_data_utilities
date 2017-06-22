
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Required libraries ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
library(dplyr)
library(RSQLite)
library(xlsx)
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# RAWDATA_CSCNTTotal ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
RAWDATA_CSCNTTotal <- "SELECT * FROM RAWDATA_CSCNTTotal"
result <- dbSendQuery(my_database$con, RAWDATA_CSCNTTotal)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

# select valued columns
data <- data %>% 
  dplyr::select(-c(UNIT, BRANCH, TEAM, OFFICE, ZONE, REGION, CHANNEL, SUBCHANNEL_NAME)) %>% 
  dplyr::tbl_df() 

data$BUSSINESSDATE <- as.character(strptime(data$BUSSINESSDATE, '%Y%m%d'))

tablename <- 'GVL_CSCNT'
if (!(tablename %in% excluded_tables)) { # some tables will be excluded from importing data
  if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
    copy_to(my_database, data, name=tablename, temporary = FALSE) # save data to a new table
  } else { # the table has been created before
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename)) # clear data from the table
    dbClearResult(results) # clear result set
    db_insert_into(con = my_database$con, table = tablename, values = data) # insert data to the table
  }
}

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# RAWDATA_KPITotal --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
RAWDATA_KPITotal <- "SELECT * FROM RAWDATA_KPITotal"
result <- dbSendQuery(my_database$con, RAWDATA_KPITotal)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

# select valued columns
data <- data %>% 
  dplyr::select(-c(UNIT, BRANCH, TEAM, OFFICE, ZONE, REGION, CHANNEL, SUBCHANNEL_NAME)) %>% 
  dplyr::tbl_df() 

data$BUSSINESSDATE <- as.character(strptime(data$BUSSINESSDATE, '%Y%m%d'))

tablename <- 'GVL_KPITOTAL'
if (!(tablename %in% excluded_tables)) { # some tables will be excluded from importing data
  if (!(tablename %in% dbListTables(my_database$con))) { # check if the table has been aldready created before
    copy_to(my_database, data, name=tablename, temporary = FALSE) # save data to a new table
  } else { # the table has been created before
    results <- dbSendQuery(my_database$con, sprintf("delete FROM %s", tablename)) # clear data from the table
    dbClearResult(results) # clear result set
    db_insert_into(con = my_database$con, table = tablename, values = data) # insert data to the table
  }
}

