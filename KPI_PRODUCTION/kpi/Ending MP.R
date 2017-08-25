# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# Ending Manpower_Total ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ending_manpower_total <- sprintf(
  "SELECT REGIONCD, AGENTCD, BUSSINESSDATE, B.TERRITORY FROM GVL_AGENTLIST A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE BUSSINESSDATE IN ('%s') 
  AND AGENT_DESIGNATION NOT IN ('AO')
  AND AGENTCD IS NOT NULL
  AND AGENT_STATUS IN ('Active','Suspended')
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, ending_manpower_total)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
ending_manpower_total_group_by_month <- data %>% 
  na.omit() %>% # omit na values
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  # count number of rows
  summarise(`Ending Manpower_Total` = n()) 

ending_manpower_total_group_by_quarter <- ending_manpower_total_group_by_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Ending Manpower_Total` = sum(`Ending Manpower_Total`)) 

ending_manpower_total_group_by_ytd <- ending_manpower_total_group_by_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Ending Manpower_Total` = sum(`Ending Manpower_Total`)) 

# Ending Manpower_SA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SQL <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND SERVICING_AGENT='Yes'", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, SQL)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

sa_agents <- data %>% 
  na.omit # omit na values

ending_manpower_sa <- sa_agents %>% 
  # convert_string_to_date('BUSSINESSDATE','%y-%m-%d') %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# SA` = n()) 

ending_manpower_sa_quarter <- ending_manpower_sa %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# SA` = sum(`# SA`)) 

ending_manpower_sa_ytd <- ending_manpower_sa %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# SA` = sum(`# SA`)) 

# Ending Manpower_ExSA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ending_manpower_exsa <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, ending_manpower_exsa)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
ending_manpower_exsa <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise('Ending Manpower_ExSA' = n()) 
ending_manpower_exsa_quarter <- ending_manpower_exsa %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Ending Manpower_ExSA` = sum(`Ending Manpower_ExSA`)) 
ending_manpower_exsa_ytd <- ending_manpower_exsa %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Ending Manpower_ExSA` = sum(`Ending Manpower_ExSA`)) 

# AG ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
AG_month <- get_Manpower_AG(last_day_of_months) %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# AG` = n()) 

AG_quarter <- AG_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# AG` = sum(`# AG`)) 

AG_ytd <- AG_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# AG` = sum(`# AG`)) 

# US ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
US <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_DESIGNATION='US'
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, US)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
US_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# US` = n()) 

US_quarter <- US_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# US` = sum(`# US`)) 

US_ytd <- US_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# US` = sum(`# US`)) 

# UM ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UM <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_DESIGNATION='UM'
  AND (A1.STAFFCD NOT IN ('SUM') or STAFFCD is null)
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, UM)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
UM_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# UM` = n()) 

UM_quarter <- UM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# UM` = sum(`# UM`)) 

UM_ytd <- UM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# UM` = sum(`# UM`)) 

# SUM ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SUM <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_DESIGNATION='UM'
  AND (A1.STAFFCD IN ('SUM'))
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, SUM)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
SUM_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# SUM` = n()) 

SUM_quarter <- SUM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# SUM` = sum(`# SUM`)) 

SUM_ytd <- SUM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# SUM` = sum(`# SUM`)) 

# BM ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BM <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_DESIGNATION='BM'
  AND (A1.STAFFCD NOT IN ('SBM') or STAFFCD is null)
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, BM)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
BM_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# BM` = n()) 
BM_quarter <- BM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# BM` = sum(`# BM`)) 
BM_ytd <- BM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# BM` = sum(`# BM`)) 

# SBM ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SBM <- sprintf(
  "SELECT A.AGENT_CODE, A.REGIONCD, B.TERRITORY, A.JOINING_DATE, A.BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  JOIN GVL_AGENTLIST A1 ON (A.BUSSINESSDATE=A1.BUSSINESSDATE AND A.AGENT_CODE=A1.AGENTCD)
  JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE IN ('%s') 
  AND A1.AGENT_STATUS IN ('Active','Suspended')
  AND (A.SERVICING_AGENT is NULL or A.SERVICING_AGENT ='No')
  AND A.AGENT_DESIGNATION='BM'
  AND (A1.STAFFCD IN ('SBM'))
  ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, SBM)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
SBM_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# SBM` = n())  
SBM_quarter <- SBM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# SBM` = sum(`# SBM`))

SBM_ytd <- SBM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# SBM` = sum(`# SBM`)) 

final_ending_manpower_sa <-
  rbind(
    ending_manpower_sa_ytd,
    ending_manpower_sa_quarter,
    ending_manpower_sa
  )

final_ending_manpower_exsa <-
  rbind(
    ending_manpower_exsa_ytd,
    ending_manpower_exsa_quarter,
    ending_manpower_exsa
  )

final_ending_manpower_total <-
  rbind(
    ending_manpower_total_group_by_ytd,
    ending_manpower_total_group_by_quarter,
    ending_manpower_total_group_by_month
  )

final_AG <-  rbind(AG_ytd, AG_quarter, AG_month)
final_US <-  rbind(US_ytd, US_quarter, US_month)
final_UM <-  rbind(UM_ytd, UM_quarter, UM_month)
final_SUM <- rbind(SUM_ytd, SUM_quarter, SUM_month)
final_BM <-  rbind(BM_ytd, BM_quarter, BM_month)
final_SBM <- rbind(SBM_ytd, SBM_quarter, SBM_month)

dbDisconnect(my_database$con)