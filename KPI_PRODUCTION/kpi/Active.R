# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# Active MDRT -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_MDRT <- get_active_MDRT(last_day_of_months)
Active_MDRT_in_month <- Active_MDRT %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`MDRT` = n()) 

Active_MDRT_in_quarter <- Active_MDRT_in_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`MDRT` = sum(`MDRT`)) 
Active_MDRT_in_ytd <- Active_MDRT_in_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`MDRT` = sum(`MDRT`)) 

# Active_GenLion ------------------------------------------------------------
Active_GenLion = get_active_genlion(last_day_of_months)

Active_GenLion_month <- Active_GenLion %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(GenLion = n())

Active_GenLion_quarter <- Active_GenLion_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(GenLion = sum(GenLion)) 
Active_GenLion_ytd <- Active_GenLion_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(GenLion = sum(GenLion)) 

# Active_Rookie_in_month -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
active_excluded_sa <- get_active_excluded_SA(last_day_of_months)

# exclude genlion from 2017-04, exclude mdrt before 2017-04
data_excluded_mdrt <-  merge(x=dplyr::filter(active_excluded_sa, BUSSINESSDATE < '2017-04-30'),
                             y=dplyr::select(Active_MDRT, c(AGENT_CODE,BUSSINESSDATE,MDRT)),
                             by.x=c('BUSSINESSDATE','AGENT_CODE'),
                             by.y=c('BUSSINESSDATE','AGENT_CODE'),
                             all.x=T) %>% 
  dplyr::filter(is.na(MDRT)) %>% 
  dplyr::select(-MDRT)
data_excluded_genlion <-  merge(x=dplyr::filter(active_excluded_sa, BUSSINESSDATE >= '2017-04-30'),
                                y=dplyr::select(Active_GenLion, c(AGENT_CODE,BUSSINESSDATE,GENLION)),
                                by.x=c('BUSSINESSDATE','AGENT_CODE'),
                                by.y=c('BUSSINESSDATE','AGENT_CODE'),
                                all.x=T) %>% 
  dplyr::filter(is.na(GENLION)) %>% 
  dplyr::select(-GENLION)
# active agents excluded sa, mdrt before 2017-04, genlion from 2017-04
data <- rbind(data_excluded_mdrt,data_excluded_genlion) %>% 
  convert_string_to_date('JOINING_DATE', FORMAT_BUSSINESSDATE) %>%
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%
  # calculate different months between joining date and business date
  dplyr::mutate(MDIFF = as.integer(round((
    as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
  ) * 12)))

Active_Rookie_in_month <-
  data  %>%
  filter(MDIFF==0)

Active_Rookie_in_month_month <- Active_Rookie_in_month %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Rookie in month` = n()) 

Active_Rookie_in_month_quarter <- Active_Rookie_in_month_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie in month` = sum(`Rookie in month`)) 
Active_Rookie_in_month_ytd <- Active_Rookie_in_month_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie in month` = sum(`Rookie in month`)) 

# Active Rookie last month -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_Rookie_last_month <- 
  data %>%
  filter(MDIFF==1) 

Active_Rookie_last_month_month <- Active_Rookie_last_month %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Rookie last month` = n()) 

Active_Rookie_last_month_quarter <- Active_Rookie_last_month_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie last month` = sum(`Rookie last month`))
Active_Rookie_last_month_ytd <- Active_Rookie_last_month_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie last month` = sum(`Rookie last month`)) 

# Active Rookie in 2, 3 months ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_Rookie_2_3_months <- 
  data %>%
  filter(MDIFF %in% (2:3)) 

Active_Rookie_2_3_months_month <- Active_Rookie_2_3_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`2-3 months` = n()) 

Active_Rookie_2_3_months_quarter <- Active_Rookie_2_3_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`2-3 months` = sum(`2-3 months`)) 
Active_Rookie_2_3_months_ytd <- Active_Rookie_2_3_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`2-3 months` = sum(`2-3 months`)) 

# Active Rookie 4,6 months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_Rookie_4_6_months <- 
  data %>%
  filter(MDIFF %in% (4:6)) 

Active_Rookie_4_6_months_month <- Active_Rookie_4_6_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`4 - 6 mths` = n()) 

Active_Rookie_4_6_months_quarter <- Active_Rookie_4_6_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`4 - 6 mths` = sum(`4 - 6 mths`)) 
Active_Rookie_4_6_months_ytd <- Active_Rookie_4_6_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`4 - 6 mths` = sum(`4 - 6 mths`)) 

# Active Rookie 7,12 months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_Rookie_7_12_months <- 
  data %>%
  filter(MDIFF %in% (7:12))  

Active_Rookie_7_12_months_month <- Active_Rookie_7_12_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`7-12mth` = n()) 

Active_Rookie_7_12_months_quarter <- Active_Rookie_7_12_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`7-12mth` = sum(`7-12mth`)) 
Active_Rookie_7_12_months_ytd <- Active_Rookie_7_12_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`7-12mth` = sum(`7-12mth`)) 

# Active Rookie 13+ months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_Rookie_13_and_more_months <- 
  data %>%
  filter(MDIFF >= 13) 

Active_Rookie_13_and_more_months_month <- Active_Rookie_13_and_more_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`13+mth` = n()) 

Active_Rookie_13_and_more_months_quarter <- Active_Rookie_13_and_more_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`13+mth` = sum(`13+mth`)) 
Active_Rookie_13_and_more_months_ytd <- Active_Rookie_13_and_more_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`13+mth` = sum(`13+mth`)) 

# Active SA -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_SA_agents <- get_active_sa(last_day_of_months)

Active_SA_agents_month <- Active_SA_agents %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`SA` = n()) 

Active_SA_agents_quarter <- Active_SA_agents_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SA` = sum(`SA`)) 
Active_SA_agents_ytd <- Active_SA_agents_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SA` = sum(`SA`)) 

# Active Total -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active_total_agents <- get_active(last_day_of_months)

Active_total_agents_month <- Active_total_agents %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Total` = n()) 

Active_total_agents_quarter <- Active_total_agents_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total` = sum(`Total`)) 
Active_total_agents_ytd <- Active_total_agents_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total` = sum(`Total`)) 

final_Active_GenLion <-
  rbind(
    Active_GenLion_ytd,
    Active_GenLion_quarter,
    Active_GenLion_month
  )
final_Active_Rookie_in_month <-
  rbind(
    Active_Rookie_in_month_ytd,
    Active_Rookie_in_month_quarter,
    Active_Rookie_in_month_month
  )
final_Active_Rookie_last_month <-
  rbind(
    Active_Rookie_last_month_ytd,
    Active_Rookie_last_month_quarter,
    Active_Rookie_last_month_month
  )
final_Active_Rookie_2_3_months <-
  rbind(
    Active_Rookie_2_3_months_ytd,
    Active_Rookie_2_3_months_quarter,
    Active_Rookie_2_3_months_month
  )
final_Active_Rookie_4_6_months <-
  rbind(
    Active_Rookie_4_6_months_ytd,
    Active_Rookie_4_6_months_quarter,
    Active_Rookie_4_6_months_month
  )
final_Active_Rookie_7_12_months <-
  rbind(
    Active_Rookie_7_12_months_ytd,
    Active_Rookie_7_12_months_quarter,
    Active_Rookie_7_12_months_month
  )
final_Active_Rookie_13_and_more_months <-
  rbind(
    Active_Rookie_13_and_more_months_ytd,
    Active_Rookie_13_and_more_months_quarter,
    Active_Rookie_13_and_more_months_month
  )
final_Active_SA_agents <-
  rbind(
    Active_SA_agents_ytd,
    Active_SA_agents_quarter,
    Active_SA_agents_month
  )
final_Active_GenLion <-
  rbind(
    Active_GenLion_ytd,
    Active_GenLion_quarter,
    Active_GenLion_month
  )
final_Active_total_agents <-
  rbind(
    Active_total_agents_ytd,
    Active_total_agents_quarter,
    Active_total_agents_month
  )
final_Active_MDRT <-
  rbind(
    Active_MDRT_in_ytd,
    Active_MDRT_in_quarter,
    Active_MDRT_in_month
  )

dbDisconnect(my_database$con)