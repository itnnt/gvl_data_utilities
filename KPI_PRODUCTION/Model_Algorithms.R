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


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Create table to save final result ---------------------------------------------------------------------------------------------------------------------------------------------------------------
Q <- last_day_of_quarters %>% 
  dplyr::tbl_df() %>% # convert to data table
  stats::setNames(.,c('BUSSINESSDATE')) %>% # names the column
  dplyr::mutate(BUSSINESSDATE_FM1=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE_FM1=paste(strftime(BUSSINESSDATE,'%Y'),BUSSINESSDATE_FM1,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  print

M <- last_day_of_months %>% 
  dplyr::tbl_df() %>% # convert a vector to data table
  stats::setNames(.,c('BUSSINESSDATE')) %>% 
  dplyr::mutate(BUSSINESSDATE_FM1=strftime(BUSSINESSDATE, '%Y-%b')) %>% 
  print

North <- rbind(Q, M)  %>%  mutate(id = row_number()) # mutates a new order col

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Ending Manpower_SA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ending_manpower_sa <- sprintf(
  "SELECT REGIONCD, AGENTCD, BUSSINESSDATE, B.TERRITORY FROM GVL_AGENTLIST A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE BUSSINESSDATE IN ('%s') 
  AND STAFFCD = 'SA'
  AND AGENTCD IS NOT NULL
  AND AGENT_STATUS IN ('Active','Suspended')
                 ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, ending_manpower_sa)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
ending_manpower_sa <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise(SA = n()) %>% # count number of rows
  print 
ending_manpower_sa_quarter <- ending_manpower_sa %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(SA = sum(SA)) %>% # SUM man power
  print
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Ending Manpower_ExSA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ending_manpower_exsa <- sprintf(
"SELECT REGIONCD, AGENTCD, BUSSINESSDATE, B.TERRITORY FROM GVL_AGENTLIST A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
  WHERE BUSSINESSDATE IN ('%s') 
  AND STAFFCD != 'SA'
  AND AGENTCD IS NOT NULL
  AND AGENT_STATUS IN ('Active','Suspended')
                 ", paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, ending_manpower_exsa)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
ending_manpower_exsa <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise('Ending Manpower_ExSA' = n()) %>% # count number of rows
  print 
ending_manpower_exsa_quarter <- ending_manpower_exsa %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('Ending Manpower_ExSA' = sum(`Ending Manpower_ExSA`)) %>% # SUM man power
  print
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
  summarise('#cases' = sum(CASE)) %>% print # total cases
cscnt_grouped_quarter <- cscnt_grouped_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('#cases' = sum(`#cases`)) %>% # SUM number of CSCNT
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
  summarise('% case with 4 riders' = sum(CASE)) %>% print # count number of rows
case_count_4riders_grouped_quarter <- case_count_4riders_grouped_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('% case with 4 riders' = sum(`% case with 4 riders`)) %>% # SUM number of CSCNT
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
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise('# Active'=n()) # count active agents
active_agents_grouped_quarter <- active_agents %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('# Active'=sum(`# Active`)) %>% # SUM number of ACTIVE_AGENTS
  print
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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month 
  summarise('# Active_ExSA' = n()) 
active_agents_ex_grouped_quarter <- active_agents_ex %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('# Active_ExSA'=sum(`# Active_ExSA`)) %>% # SUM number of ACTIVE_AGENTSEX
  print
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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month 
  summarise(ACTIVE_AGENTS_MONTHSTART = n()) 
active_agents_month_start_grouped_quarter <- active_agents_month_start %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(ACTIVE_AGENTS_MONTHSTART=sum(ACTIVE_AGENTS_MONTHSTART)) %>% # SUM number of ACTIVE_AGENTS_MONTHSTART
  print

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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month 
  summarise(ACTIVE_AGENTS_MONTHEND = n()) 
active_agents_month_end_grouped_quarter <- active_agents_month_end %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(ACTIVE_AGENTS_MONTHEND=sum(ACTIVE_AGENTS_MONTHEND)) %>% # SUM number of ACTIVE_AGENTS_MONTHEND
  print
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Persistency K1 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
persistency_k1 <- sprintf(
  "SELECT AGENT_CODE AS REGIONCD, TOTALAPECURRENT_REGION, TOTALAPEORIGINAL_REGION, DATE AS BUSSINESSDATE, B.TERRITORY 
  FROM RAWDATA_Persistency_Y1 A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.AGENT_CODE = B.REGION_CODE
  WHERE DATE in ('%s')
  AND TOTALAPECURRENT_REGION IS NOT NULL AND TOTALAPEORIGINAL_REGION IS NOT NULL", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, persistency_k1)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
persistency_k1_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K1'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) %>% 
  print 
persistency_k1_quarter <- persistency_k1_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K1'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) %>% 
  print
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Persistency K2 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
persistency_k2 <- sprintf(
  "SELECT AGENT_CODE AS REGIONCD, TOTALAPECURRENT_REGION, TOTALAPEORIGINAL_REGION, DATE AS BUSSINESSDATE, B.TERRITORY 
  FROM RAWDATA_Persistency_Y2 A
  LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.AGENT_CODE = B.REGION_CODE
  WHERE DATE in ('%s')
  AND TOTALAPECURRENT_REGION IS NOT NULL AND TOTALAPEORIGINAL_REGION IS NOT NULL", 
  paste(last_day_of_months, collapse = "','"))
result <- dbSendQuery(my_database$con, persistency_k2)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
persistency_k2_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, '%Y-%b')) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K2'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) %>% 
  print 
persistency_k2_quarter <- persistency_k2_month %>% 
  dplyr::mutate(D='01') %>% # adds a column named D, equals to 01
  dplyr::mutate(BUSSINESSDATE=paste(BUSSINESSDATE,D,sep='-')) %>% # adds 01 to the bussiness date
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%b-%d'))) %>% # converts the bussiness date to date datatype
  dplyr::mutate(Q=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  dplyr::mutate(BUSSINESSDATE=paste(strftime(BUSSINESSDATE,'%Y'),Q,sep='-')) %>% # converts the bussiness date to YYYY-QQ format
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K2'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) %>% 
  print

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
  dplyr::mutate('Activity Ratio'=`# Active`*2/(ACTIVE_AGENTS_MONTHSTART+ACTIVE_AGENTS_MONTHEND))

activity_ratio_grouped_quarter <- merge(
  x = active_agents_grouped_quarter,
  y = active_agents_month_start_grouped_quarter,
  by.x = c('TERRITORY',	'BUSSINESSDATE'),
  by.y = c('TERRITORY',	'BUSSINESSDATE' ),
  all.x = TRUE
)
activity_ratio_grouped_quarter <- merge(
  x = activity_ratio_grouped_quarter,
  y = active_agents_month_end_grouped_quarter,
  by.x = c('TERRITORY',	'BUSSINESSDATE'),
  by.y = c('TERRITORY',	'BUSSINESSDATE' ),
  all.x = TRUE
)
activity_ratio_grouped_quarter <- activity_ratio_grouped_quarter %>% 
  dplyr::mutate('Activity Ratio'=`# Active`*2/(ACTIVE_AGENTS_MONTHSTART+ACTIVE_AGENTS_MONTHEND))


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Build the last results --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
North <- North %>% 
  merge(x=., y=dplyr::filter(rbind(ending_manpower_exsa_quarter,ending_manpower_exsa), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(ending_manpower_sa_quarter,ending_manpower_sa), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(activity_ratio_grouped_quarter, activity_ratio), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  dplyr::select(-c(`# Active`, ACTIVE_AGENTS_MONTHSTART, ACTIVE_AGENTS_MONTHEND)) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(active_agents_ex_grouped_quarter, active_agents_ex), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(active_agents_grouped_quarter, active_agents), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(cscnt_grouped_quarter,cscnt_grouped_month), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(ape_grouped_quarter,ape_grouped_month), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(case_count_4riders_grouped_quarter,case_count_4riders_grouped_month), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(persistency_k1_quarter,persistency_k1_month), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  dplyr::select(-c(TOTALAPEORIGINAL_REGION, TOTALAPECURRENT_REGION)) %>% # remove TERRITORY col
  merge(x=., y=dplyr::filter(rbind(persistency_k2_quarter,persistency_k2_month), TERRITORY=='NORTH'), 
        by.x = c('BUSSINESSDATE_FM1'),
        by.y = c('BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::select(-TERRITORY) %>% # remove TERRITORY col
  dplyr::select(-c(TOTALAPEORIGINAL_REGION, TOTALAPECURRENT_REGION)) %>% # remove TERRITORY col
  dplyr::arrange(id) %>% # sort by id
  dplyr::select(-id) %>% 
  print
  
# "Transposing a dataframe maintaining the first column as heading
# North_results <- setNames(data.frame(t(North_results[,-1]), stringsAsFactors = F), North_results[,1])
North_results <- North %>% 
  dplyr::mutate('# Case/active'=`#cases`/`# Active`) %>% 
  dplyr::mutate('Casesize'=APE/`#cases`) %>% 
  select(-c(BUSSINESSDATE_FM1,BUSSINESSDATE))%>% # remove unnecessary columns so that all the cells' datatype will be kept
  t() %>% 
  data.frame(stringsAsFactors = F) %>% 
  setNames(North[,1]) # set columns' name by the first row's values

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Save output to excel file -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
wb <- xlsx::createWorkbook(type="xlsx")         # create blank workbook

# Styles for the data table row/column names
NUMERIC_STYLE <- CellStyle(wb, dataFormat =  DataFormat('#,##0.0')) #+ Font(wb, isBold=TRUE)
TABLE_ROWNAMES_STYLE <- CellStyle(wb) + Font(wb, isBold=TRUE)
TABLE_COLNAMES_STYLE <- CellStyle(wb) + Font(wb, isBold=TRUE) +
  Alignment(wrapText=TRUE, horizontal="ALIGN_CENTER") +
  Border(color="black", position=c("TOP", "BOTTOM"), 
         pen=c("BORDER_THIN", "BORDER_THICK")) 
TABLE_COLNAMES_STYLE_BLUE <- CellStyle(wb) + Font(wb, isBold=TRUE, color = 'blue') +
  Alignment(wrapText=TRUE, horizontal="ALIGN_CENTER") +
  Border(color="blue", position=c("TOP", "BOTTOM"), 
         pen=c("BORDER_THIN", "BORDER_THICK")) 
# add sheets to the workbook
sheet0 <- xlsx::createSheet(wb, sheetName="North") # create different sheets
sheet1 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTS") # create different sheets
sheet2 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTS_MONTH_START")
sheet3 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTS_MONTH_END")
sheet4 <- xlsx::createSheet(wb, sheetName="ACTIVITY_RATIO")
sheet5 <- xlsx::createSheet(wb, sheetName="ACTIVE_AGENTSEX")
sheet6 <- xlsx::createSheet(wb, sheetName="CSCNT")
sheet7 <- xlsx::createSheet(wb, sheetName="APE")
sheet8 <- xlsx::createSheet(wb, sheetName="CSCNT_4RIDERS")
sheet9 <- xlsx::createSheet(wb, sheetName="PERSISTENCY_K1")
sheet10 <- xlsx::createSheet(wb, sheetName="PERSISTENCY_K2")
sheet11 <- xlsx::createSheet(wb, sheetName="ENDING MANPOWER EXSA")
sheet12 <- xlsx::createSheet(wb, sheetName="ENDING MANPOWER SA")
# add data frame to the sheets
xlsx::addDataFrame(as.data.frame(North_results), 
                   sheet0, 
                   row.names=T, 
                   colnamesStyle = TABLE_COLNAMES_STYLE,
                   rownamesStyle = TABLE_ROWNAMES_STYLE
                   )  # add data to the sheet
# Create a freeze pane, fix first row and column
xlsx::createFreezePane(sheet0, 2, 2)
rows  <- getRows(sheet0, rowIndex=2:(nrow(North_results)+1))   # get all the rows excluded 1st row is headers
# Set cell stype
cells <- xlsx::getCells(rows, colIndex = 2:ncol(North_results)) # get all the cells excluded the 1st col
# xlsx::setCellStyle(cells[[500]], NUMERIC_STYLE)
# apply the NUMERIC_STYLE for all the cells
lapply(names(cells), function(ii) setCellStyle(cells[[ii]], NUMERIC_STYLE))
# autosize column widths
xlsx::autoSizeColumn(sheet0, colIndex=1:(1+ncol(North_results)))
cell11 <- getCells(getRows(sheet0,rowIndex=1:2),colIndex = 1:2)[[1]]
xlsx::setCellValue(cell11, "Overall KPI performance")
xlsx::setCellStyle(cell11, TABLE_COLNAMES_STYLE_BLUE)

xlsx::addDataFrame(as.data.frame(rbind(active_agents_grouped_quarter, active_agents)), sheet1, row.names=F)  # add data to the sheet
xlsx::addDataFrame(as.data.frame(rbind(active_agents_month_start_grouped_quarter, active_agents_month_start)), sheet2, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(active_agents_month_end_grouped_quarter, active_agents_month_end)), sheet3, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(activity_ratio_grouped_quarter, activity_ratio)), sheet4, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(active_agents_ex_grouped_quarter, active_agents_ex)), sheet5, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(cscnt_grouped_quarter,cscnt_grouped_month)), sheet6, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(ape_grouped_quarter,ape_grouped_month)), sheet7, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(case_count_4riders_grouped_quarter,case_count_4riders_grouped_month)), sheet8, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(persistency_k1_quarter,persistency_k1_month)), sheet9, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(persistency_k2_quarter,persistency_k2_month)), sheet10, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(ending_manpower_exsa_quarter,ending_manpower_exsa)), sheet11, row.names=F)
xlsx::addDataFrame(as.data.frame(rbind(ending_manpower_sa_quarter,ending_manpower_sa)), sheet12, row.names=F)

xlsx::saveWorkbook(wb, "KPI_PRODUCTION/output/GVL_Agency_reports.xlsx")  # write the file with multiple sheets





