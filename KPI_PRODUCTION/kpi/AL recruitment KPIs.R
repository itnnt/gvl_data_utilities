# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# AL recruitment KPIs -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# leader ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
agents <- get_Manpower(last_day_of_months)
experience_leader <- sprintf(
  "SELECT AGENT_CODE, EFFDATEFROM, TYPESTART 
  FROM RAWDATA_ExperienceLeaders 
  WHERE EFFDATEFROM >= '%s' AND EFFDATEFROM <= '%s'
  ", first_day_of_months[1], last_day_of_months[length(last_day_of_months)])
result <- dbSendQuery(my_database$con, experience_leader)
data = fetch(result, encoding="utf-8")
dbClearResult(result)
experience_leader <- data %>% 
  convert_string_to_date('EFFDATEFROM', FORMAT_BUSSINESSDATE) %>% 
  dplyr::mutate(EFFDATEFROM=strftime(EFFDATEFROM, FORMAT_MONTH_YEAR_y_b))

leader_month <- dplyr::filter(agents, (AGENT_DESIGNATION %in% c('US','UM','BM'))) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# leader` = n()) 

leader_quarter <- leader_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# leader` = sum(`# leader`)) 

leader_ytd <- leader_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# leader` = sum(`# leader`)) 

# active leader ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
active_leader_month <- dplyr::filter(agents, (AGENT_DESIGNATION %in% c('US','UM','BM'))) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# active leader` = n()) 

active_leader_quarter <- active_leader_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# active leader` = sum(`# active leader`)) 

active_leader_ytd <- active_leader_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# active leader` = sum(`# active leader`)) 

# recruite leader ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
recruite_leader_month <- dplyr::filter(agents, (AGENT_DESIGNATION %in% c('US','UM','BM'))) %>% 
  dplyr::mutate(EFFDATEFROM=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% 
  merge(x = ., y = experience_leader,
        by.x = c('AGENT_CODE'),
        by.y = c('AGENT_CODE' )) %>% 
  na.omit %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`# recruit` = n()) 

recruite_leader_quarter <- recruite_leader_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# recruit` = sum(`# recruit`)) 

recruite_leader_ytd <- recruite_leader_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`# recruit` = sum(`# recruit`)) 

final_leader <-
  rbind(
    leader_ytd,
    leader_quarter,
    leader_month
  )
final_active_leader <-
  rbind(
    active_leader_ytd,
    active_leader_quarter,
    active_leader_month
  )
final_recruite_leader <-
  rbind(
    recruite_leader_ytd,
    recruite_leader_quarter,
    recruite_leader_month
  )

dbDisconnect(my_database$con)