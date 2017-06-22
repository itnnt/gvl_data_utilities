source('EDM_set_common_input.R')
source('util/util_data_access_sqlite.R')
library(dplyr)

# PARAMETER SETUP ####
env = 'COMPANY_EDM_VN'
repdt = "2017-06-07"
dbfile = 'D:/workspace_r/gvl_data_utilities/data_feeding/EDM.db'
my_database<- src_sqlite(dbfile, create = FALSE)
#sqlite_retrieve("SELECT * FROM CMS_REP_DAILYPRODNSUMM", dbfile)

# CMS_REP_AGTPRODN ####
datafr <- execute_sql(sprintf("SELECT * FROM CMS_REP_AGTPRODN WHERE REPORTINGDT='%s'", repdt), env)
if (!("CMS_REP_AGTPRODN" %in% dbListTables(my_database$con))) {
  copy_to(my_database, datafr, name='CMS_REP_AGTPRODN', temporary = FALSE)
} else {
  results <- dbSendQuery(my_database$con, sprintf("delete FROM CMS_REP_AGTPRODN WHERE REPORTINGDT='%s'", repdt))
  dbClearResult(results)
  db_insert_into(con = my_database$con, table = "CMS_REP_AGTPRODN", values = datafr)
}

# CMS_REP_DAILYPRODNSUMM ####
cms_rep_dailyprodnsumm <- execute_sql(sprintf("SELECT * FROM CMS_REP_DAILYPRODNSUMM WHERE REPORTINGDT='%s'", repdt), env)
if (!("CMS_REP_DAILYPRODNSUMM" %in% dbListTables(my_database$con))) {
  copy_to(my_database, cms_rep_dailyprodnsumm, name='CMS_REP_DAILYPRODNSUMM', temporary = FALSE)
} else {
  results <- dbSendQuery(my_database$con, sprintf("delete FROM CMS_REP_DAILYPRODNSUMM WHERE REPORTINGDT='%s'", repdt))
  dbClearResult(results)
  db_insert_into(con = my_database$con, table = "CMS_REP_DAILYPRODNSUMM", values = cms_rep_dailyprodnsumm)
}

dbDisconnect(my_database$con)
