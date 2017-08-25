# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# cases -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
cscnt <- sprintf("SELECT A.*, B.TERRITORY FROM GVL_CSCNT A
                 LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
                 WHERE A.BUSSINESSDATE >= '%s'
                 ", strftime(fr, FORMAT_BUSSINESSDATE))
result <- dbSendQuery(my_database$con, cscnt)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

cscnt_grouped_month <- data %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise('#cases' = sum(CASE)) 

cscnt_grouped_quarter <- cscnt_grouped_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('#cases' = sum(`#cases`)) 
cscnt_grouped_ytd <- cscnt_grouped_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('#cases' = sum(`#cases`)) 

# Percentage of case with 4 riders ----------------------------------------------------------------------------------------------------------------------------------------------------------------
case_count_4riders <- sprintf("SELECT A.*, B.TERRITORY FROM GVL_CSCNT A
                              LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
                              WHERE A.BUSSINESSDATE >= '%s'
                              AND A.RDOCNUM IN (
                              SELECT DISTINCT RDOCNUM FROM GVL_KPITOTAL 
                              WHERE COUNTRIDERS >= 4 
                              )", strftime(fr, FORMAT_BUSSINESSDATE))
result <- dbSendQuery(my_database$con, case_count_4riders)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

case_count_4riders_grouped_month <- data %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise('% case with 4 riders' = sum(CASE)) 

case_count_4riders_grouped_quarter <- case_count_4riders_grouped_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('% case with 4 riders' = sum(`% case with 4 riders`)) 

case_count_4riders_ytd <- case_count_4riders_grouped_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('% case with 4 riders' = sum(`% case with 4 riders`)) 


# case_count_4riders_mom <- filter_data_for_mom(case_count_4riders_grouped_month) 
# case_count_4riders_yoy <- filter_data_for_yoy(case_count_4riders_grouped_month) %>% 
#   summarise(`% case with 4 riders` = sum(`% case with 4 riders`)) 

# APE ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ape <- sprintf("SELECT BUSSINESSDATE, AGCODE, REGIONCD, APE, B.TERRITORY FROM GVL_KPITOTAL A
               LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
               WHERE BUSSINESSDATE >= '%s'", strftime(fr, FORMAT_BUSSINESSDATE))
result <- dbSendQuery(my_database$con, ape)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

ape <- data %>% na.omit
ape_grouped_month <- data %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(APE = sum(APE)) 


ape_grouped_quarter <- ape_grouped_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(APE = sum(APE)) 

ape_grouped_ytd <- ape_grouped_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(APE = sum(APE)) 

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

active_agents <- data

active_agents_month <- active_agents %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise('# Active'=n()) # count active agents
active_agents_grouped_quarter <- active_agents_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('# Active'=sum(`# Active`)) 

active_agents_grouped_ytd <- active_agents_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('# Active'=sum(`# Active`)) 

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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month 
  summarise('# Active_ExSA' = n()) 
active_agents_ex_grouped_quarter <- active_agents_ex %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('# Active_ExSA'=sum(`# Active_ExSA`)) 

active_agents_ex_ytd <- active_agents_ex %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise('# Active_ExSA'=sum(`# Active_ExSA`)) 

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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month 
  summarise(ACTIVE_AGENTS_MONTHSTART = n()) 
active_agents_month_start_grouped_quarter <- active_agents_month_start %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(ACTIVE_AGENTS_MONTHSTART=sum(ACTIVE_AGENTS_MONTHSTART)) 

active_agents_month_start_ytd <- active_agents_month_start %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(ACTIVE_AGENTS_MONTHSTART=sum(ACTIVE_AGENTS_MONTHSTART)) 

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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month 
  summarise(ACTIVE_AGENTS_MONTHEND = n()) 
active_agents_month_end_grouped_quarter <- active_agents_month_end %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(ACTIVE_AGENTS_MONTHEND=sum(ACTIVE_AGENTS_MONTHEND)) 

active_agents_month_end_ytd <- active_agents_month_end %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(ACTIVE_AGENTS_MONTHEND=sum(ACTIVE_AGENTS_MONTHEND)) 

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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K1'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) 

persistency_k1_quarter <- persistency_k1_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K1'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) 

persistency_k1_ytd <- persistency_k1_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K1'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) 

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
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K2'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) 

persistency_k2_quarter <- persistency_k2_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K2'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) 

persistency_k2_ytd <- persistency_k2_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(TOTALAPECURRENT_REGION=sum(TOTALAPECURRENT_REGION), TOTALAPEORIGINAL_REGION=sum(TOTALAPEORIGINAL_REGION)) %>% 
  dplyr::mutate('Persistency K2'=TOTALAPECURRENT_REGION/TOTALAPEORIGINAL_REGION) 

#  Activity Ratio ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# left join
activity_ratio <- merge(
  x = active_agents_month,
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
# ---
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
# ---
activity_ratio_ytd <- active_agents_grouped_ytd %>% 
  merge(x = ., y = active_agents_month_start_ytd,
        by.x = c('TERRITORY',	'BUSSINESSDATE'),
        by.y = c('TERRITORY',	'BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  merge(x = ., y = active_agents_month_end_ytd,
        by.x = c('TERRITORY',	'BUSSINESSDATE'),
        by.y = c('TERRITORY',	'BUSSINESSDATE' ),
        all.x = TRUE) %>% 
  dplyr::mutate('Activity Ratio'=`# Active`*2/(ACTIVE_AGENTS_MONTHSTART+ACTIVE_AGENTS_MONTHEND)) 
# ---

final_activity_ratio <-
  rbind(
    activity_ratio_ytd,
    activity_ratio_grouped_quarter,
    activity_ratio
  )
final_active_agents_ex <-
  rbind(
    active_agents_ex_ytd,
    active_agents_ex_grouped_quarter,
    active_agents_ex
  )
final_active_agents <-
  rbind(
    active_agents_grouped_ytd,
    active_agents_grouped_quarter,
    active_agents_month
  )
final_cscnt <-
  rbind(
    cscnt_grouped_ytd,
    cscnt_grouped_quarter,
    cscnt_grouped_month
  )
final_ape <-
  rbind(
    ape_grouped_ytd,
    ape_grouped_quarter,
    ape_grouped_month
  )
final_case_count_4riders <-
  rbind(
    case_count_4riders_ytd,
    case_count_4riders_grouped_quarter,
    case_count_4riders_grouped_month
  )
final_persistency_k1 <-
  rbind(
    persistency_k1_ytd,
    persistency_k1_quarter,
    persistency_k1_month
  )
final_persistency_k2 <-
  rbind(
    persistency_k2_ytd,
    persistency_k2_quarter,
    persistency_k2_month
  )

dbDisconnect(my_database$con)