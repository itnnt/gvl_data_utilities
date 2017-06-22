# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Set study peiod ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
fr <- as.Date("2015-01-01")
to <- as.Date("2017-12-31")

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
# Generate a sequence of the fisrt day and last day of the month over 5 years ---------------------------------------------------------------------------------------------------------------------
first_day_of_months <- seq(from=fr, to=to, by="1 month")
last_day_of_months <- seq(from=seq(fr, length=2, by='1 month')[2], to=seq(to, length=2, by='1 month')[2], by="1 month") - 1

# Generate a sequence of the last day of the quarter over 5 years ---------------------------------------------------------------------------------------------------------------------------------
last_day_of_quarters <- seq(from=seq(fr, length=2, by='1 quarter')[2], to=seq(to, length=2, by='1 quarter')[2], by="1 quarter") - 1

# dplyr::tbl_df(last_day_of_months)

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# CSCNT -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
cscnt <- sprintf("SELECT A.*, B.TERRITORY FROM GVL_CSCNT A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE >= '%s'
  ", strftime(fr, '%Y-%m-%d'))
result <- dbSendQuery(my_database$con, cscnt)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

cscnt_grouped_month <- data %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise(CSCNT = sum(CASE)) %>% print # count number of rows
cscnt_grouped_quarter <- cscnt_grouped_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(CSCNT = sum(CSCNT)) %>% # SUM number of CSCNT
  print

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Percentage of case with 4 riders ----------------------------------------------------------------------------------------------------------------------------------------------------------------
case_count_4riders <- sprintf("SELECT A.*, B.TERRITORY FROM GVL_CSCNT A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE A.BUSSINESSDATE >= '%s'
  AND A.RDOCNUM IN (
											SELECT DISTINCT RDOCNUM FROM GVL_KPITOTAL 
											WHERE COUNTRIDERS >= 4 
									  )", strftime(fr, '%Y-%m-%d'))
result <- dbSendQuery(my_database$con, case_count_4riders)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

case_count_4riders_grouped_month <- data %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise(CSCNT = sum(CASE)) %>% print # count number of rows
case_count_4riders_grouped_quarter <- case_count_4riders_grouped_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(CSCNT = sum(CSCNT)) %>% # SUM number of CSCNT
  print
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# APE ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ape <- sprintf("SELECT BUSSINESSDATE, AGCODE, REGIONCD, APE, B.TERRITORY FROM GVL_KPITOTAL A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE BUSSINESSDATE >= '%s'", strftime(fr, '%Y-%m-%d'))
result <- dbSendQuery(my_database$con, ape)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

ape_grouped_month <- data %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  na.omit %>% # omit na values
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise(APE = sum(APE)) %>% # sum ape
  print # 

ape_grouped_quarter <- ape_grouped_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(APE = sum(APE)) %>% # SUM number of CSCNT
  print
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Active agents -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
active_agents <- sprintf(
  "SELECT AGENT_CODE, REGIONCD, B.TERRITORY, BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
  WHERE BUSSINESSDATE IN ('%s') 
  AND SERVICING_AGENT IS NULL
  AND ACTIVE='Yes'", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, active_agents)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

active_agents <- data %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% 
  summarise(NUMBER_OF_ACTIVE_AGENTS = n()) 

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Active agents EX --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
active_agents_ex <- sprintf(
  "SELECT AGENT_CODE, REGIONCD, B.TERRITORY, BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
  WHERE BUSSINESSDATE IN ('%s') 
  AND SERVICING_AGENT IS NULL
  AND ACTIVEEX='Yes'", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, active_agents_ex)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

active_agents_ex <- data %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% 
  summarise(NUMBER_OF_ACTIVE_AGENTSEX = n()) 

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Number of agents at the first day of months -----------------------------------------------------------------------------------------------------------------------------------------------------
active_agents_month_start <- sprintf(
  "SELECT AGENT_CODE, REGIONCD, B.TERRITORY, BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
  WHERE BUSSINESSDATE IN ('%s') 
  AND SERVICING_AGENT IS NULL
  AND MONTHSTART='Yes'", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, active_agents_month_start)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

active_agents_month_start <- data %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% 
  summarise(NUMBER_OF_ACTIVE_AGENTS_MONTHSTART = n()) 


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Number of agents at the last day of months ------------------------------------------------------------------------------------------------------------------------------------------------------
active_agents_month_end <- sprintf(
  "SELECT AGENT_CODE, REGIONCD, B.TERRITORY, BUSSINESSDATE 
  FROM RAWDATA_MANPOWER_ACTIVERATIO A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE 
  WHERE BUSSINESSDATE IN ('%s') 
  AND SERVICING_AGENT IS NULL
  AND MONTHEND='Yes'", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, active_agents_month_end)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

active_agents_month_end <- data %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% 
  summarise(NUMBER_OF_ACTIVE_AGENTS_MONTHEND = n()) 

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Activity Ratio ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# left join
activity_ratio <- merge(
  x = active_agents,
  y = active_agents_month_start,
  by.x = c('TERRITORY',	'BUSSINESSDATE'),
  by.y = c('TERRITORY',	'BUSSINESSDATE' ),
  all.x = TRUE
)
activity_ratio <- merge(
  x = activity_ratio,
  y = active_agents_month_end,
  by.x = c('TERRITORY',	'BUSSINESSDATE'),
  by.y = c('TERRITORY',	'BUSSINESSDATE' ),
  all.x = TRUE
)
activity_ratio <- activity_ratio %>% 
  dplyr::mutate(ACTIVITY_RATIO = NUMBER_OF_ACTIVE_AGENTS * 2 / (NUMBER_OF_ACTIVE_AGENTS_MONTHSTART + NUMBER_OF_ACTIVE_AGENTS_MONTHEND))







# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# save output to excel file -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
wb <- xlsx::createWorkbook()         # create blank workbook
sheet1 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTS") # create different sheets
sheet2 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTS_MONTH_START")
sheet3 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTS_MONTH_END")
sheet4 <- xlsx::createSheet(wb, sheetName="ACTIVITY_RATIO")
sheet5 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTSEX")
sheet6 <- xlsx::createSheet(wb, sheetName="CSCNT")
sheet7 <- xlsx::createSheet(wb, sheetName="APE")
sheet8 <- xlsx::createSheet(wb, sheetName="CSCNT_4RIDERS")
xlsx::addDataFrame(as.data.frame(active_agents), sheet1)  # add data to the sheets
xlsx::addDataFrame(as.data.frame(active_agents_month_start), sheet2)
xlsx::addDataFrame(as.data.frame(active_agents_month_end), sheet3)
xlsx::addDataFrame(as.data.frame(activity_ratio), sheet4)
xlsx::addDataFrame(as.data.frame(active_agents_ex), sheet5)
xlsx::addDataFrame(as.data.frame(rbind(cscnt_grouped_quarter,cscnt_grouped_month)), sheet6)
xlsx::addDataFrame(as.data.frame(rbind(ape_grouped_quarter,ape_grouped_month)), sheet7)
xlsx::addDataFrame(as.data.frame(rbind(case_count_4riders_grouped_quarter,case_count_4riders_grouped_month)), sheet8)

xlsx::saveWorkbook(wb, "KPI_PRODUCTION/output/count_active_agent.xlsx")  # write the file with multiple sheets
