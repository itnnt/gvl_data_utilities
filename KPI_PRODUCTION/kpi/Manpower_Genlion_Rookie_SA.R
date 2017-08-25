# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# Manpower MDRT -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_MDRT <- get_Manpower_MDRT(last_day_of_months[last_day_of_months<'2017-04-30'])
Manpower_MDRT_in_month <- Manpower_MDRT %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`MDRT` = n()) 

Manpower_MDRT_in_quarter <- Manpower_MDRT_in_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`MDRT` = sum(`MDRT`)) 
Manpower_MDRT_in_ytd <- Manpower_MDRT_in_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`MDRT` = sum(`MDRT`)) 

# Manpower_GenLion ------------------------------------------------------------
Manpower_GenLion = get_Manpower_genlion(last_day_of_months)

Manpower_GenLion_month <- Manpower_GenLion %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(GenLion = n())

Manpower_GenLion_quarter <- Manpower_GenLion_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(GenLion = sum(GenLion)) 
Manpower_GenLion_ytd <- Manpower_GenLion_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(GenLion = sum(GenLion)) 

# Manpower_Rookie_in_month -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_excluded_sa <- get_Manpower_excluded_SA(last_day_of_months)

# exclude genlion from 2017-04, exclude mdrt before 2017-04
data_excluded_mdrt <-  merge(x=dplyr::filter(Manpower_excluded_sa, BUSSINESSDATE < '2017-04-30'),
                             y=dplyr::select(Manpower_MDRT, c(AGENT_CODE,BUSSINESSDATE,MDRT)),
                             by.x=c('BUSSINESSDATE','AGENT_CODE'),
                             by.y=c('BUSSINESSDATE','AGENT_CODE'),
                             all.x=T) %>% 
  dplyr::filter(is.na(MDRT)) %>% 
  dplyr::select(-MDRT)
data_excluded_genlion <-  merge(x=dplyr::filter(Manpower_excluded_sa, BUSSINESSDATE >= '2017-04-30'),
                                y=dplyr::select(Manpower_GenLion, c(AGENT_CODE,BUSSINESSDATE,GENLION)),
                                by.x=c('BUSSINESSDATE','AGENT_CODE'),
                                by.y=c('BUSSINESSDATE','AGENT_CODE'),
                                all.x=T) %>% 
  dplyr::filter(is.na(GENLION)) %>% 
  dplyr::select(-GENLION)
# Manpower agents excluded sa, mdrt before 2017-04, genlion from 2017-04
data <- rbind(data_excluded_mdrt,data_excluded_genlion) %>% 
  convert_string_to_date('JOINING_DATE', FORMAT_BUSSINESSDATE) %>%
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%
  # calculate different months between joining date and business date
  dplyr::mutate(MDIFF = as.integer(round((
    as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
  ) * 12)))

Manpower_Rookie_in_month <-
  data  %>%
  filter(MDIFF==0)

Manpower_Rookie_in_month_month <- Manpower_Rookie_in_month %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Rookie in month` = n()) 

Manpower_Rookie_in_month_quarter <- Manpower_Rookie_in_month_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie in month` = sum(`Rookie in month`)) 
Manpower_Rookie_in_month_ytd <- Manpower_Rookie_in_month_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie in month` = sum(`Rookie in month`)) 

# Manpower Rookie last month -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_Rookie_last_month <- 
  data %>%
  filter(MDIFF==1) 

Manpower_Rookie_last_month_month <- Manpower_Rookie_last_month %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Rookie last month` = n()) 

Manpower_Rookie_last_month_quarter <- Manpower_Rookie_last_month_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie last month` = sum(`Rookie last month`))
Manpower_Rookie_last_month_ytd <- Manpower_Rookie_last_month_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie last month` = sum(`Rookie last month`)) 

# Manpower Rookie in 2, 3 months ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_Rookie_2_3_months <- 
  data %>%
  filter(MDIFF %in% (2:3)) 

Manpower_Rookie_2_3_months_month <- Manpower_Rookie_2_3_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`2-3 months` = n()) 

Manpower_Rookie_2_3_months_quarter <- Manpower_Rookie_2_3_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`2-3 months` = sum(`2-3 months`)) 
Manpower_Rookie_2_3_months_ytd <- Manpower_Rookie_2_3_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`2-3 months` = sum(`2-3 months`)) 

# Manpower Rookie 4,6 months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_Rookie_4_6_months <- 
  data %>%
  filter(MDIFF %in% (4:6)) 

Manpower_Rookie_4_6_months_month <- Manpower_Rookie_4_6_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`4 - 6 mths` = n()) 

Manpower_Rookie_4_6_months_quarter <- Manpower_Rookie_4_6_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`4 - 6 mths` = sum(`4 - 6 mths`)) 
Manpower_Rookie_4_6_months_ytd <- Manpower_Rookie_4_6_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`4 - 6 mths` = sum(`4 - 6 mths`)) 

# Manpower Rookie 7,12 months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_Rookie_7_12_months <- 
  data %>%
  filter(MDIFF %in% (7:12))  

Manpower_Rookie_7_12_months_month <- Manpower_Rookie_7_12_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`7-12mth` = n()) 

Manpower_Rookie_7_12_months_quarter <- Manpower_Rookie_7_12_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`7-12mth` = sum(`7-12mth`)) 
Manpower_Rookie_7_12_months_ytd <- Manpower_Rookie_7_12_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`7-12mth` = sum(`7-12mth`)) 

# Manpower Rookie 13+ months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_Rookie_13_and_more_months <- 
  data %>%
  filter(MDIFF >= 13) 

Manpower_Rookie_13_and_more_months_month <- Manpower_Rookie_13_and_more_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`13+mth` = n()) 

Manpower_Rookie_13_and_more_months_quarter <- Manpower_Rookie_13_and_more_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`13+mth` = sum(`13+mth`)) 
Manpower_Rookie_13_and_more_months_ytd <- Manpower_Rookie_13_and_more_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`13+mth` = sum(`13+mth`)) 

# Manpower SA -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_SA_agents <- get_Manpower_sa(last_day_of_months)

Manpower_SA_agents_month <- Manpower_SA_agents %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`SA` = n()) 

Manpower_SA_agents_quarter <- Manpower_SA_agents_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SA` = sum(`SA`)) 
Manpower_SA_agents_ytd <- Manpower_SA_agents_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SA` = sum(`SA`)) 

# Manpower Total -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_total_agents <- get_Manpower(last_day_of_months)

Manpower_total_agents_month <- Manpower_total_agents %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Total` = n()) 

Manpower_total_agents_quarter <- Manpower_total_agents_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total` = sum(`Total`)) 
Manpower_total_agents_ytd <- Manpower_total_agents_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total` = sum(`Total`)) 

final_Manpower_GenLion <-
  rbind(
    Manpower_GenLion_ytd,
    Manpower_GenLion_quarter,
    Manpower_GenLion_month
  )
final_Manpower_Rookie_in_month <-
  rbind(
    Manpower_Rookie_in_month_ytd,
    Manpower_Rookie_in_month_quarter,
    Manpower_Rookie_in_month_month
  )
final_Manpower_Rookie_last_month <-
  rbind(
    Manpower_Rookie_last_month_ytd,
    Manpower_Rookie_last_month_quarter,
    Manpower_Rookie_last_month_month
  )
final_Manpower_Rookie_2_3_months <-
  rbind(
    Manpower_Rookie_2_3_months_ytd,
    Manpower_Rookie_2_3_months_quarter,
    Manpower_Rookie_2_3_months_month
  )
final_Manpower_Rookie_4_6_months <-
  rbind(
    Manpower_Rookie_4_6_months_ytd,
    Manpower_Rookie_4_6_months_quarter,
    Manpower_Rookie_4_6_months_month
  )
final_Manpower_Rookie_7_12_months <-
  rbind(
    Manpower_Rookie_7_12_months_ytd,
    Manpower_Rookie_7_12_months_quarter,
    Manpower_Rookie_7_12_months_month
  )
final_Manpower_Rookie_13_and_more_months <-
  rbind(
    Manpower_Rookie_13_and_more_months_ytd,
    Manpower_Rookie_13_and_more_months_quarter,
    Manpower_Rookie_13_and_more_months_month
  )
final_Manpower_SA_agents <-
  rbind(
    Manpower_SA_agents_ytd,
    Manpower_SA_agents_quarter,
    Manpower_SA_agents_month
  )
final_Manpower_GenLion <-
  rbind(
    Manpower_GenLion_ytd,
    Manpower_GenLion_quarter,
    Manpower_GenLion_month
  )
final_Manpower_total_agents <-
  rbind(
    Manpower_total_agents_ytd,
    Manpower_total_agents_quarter,
    Manpower_total_agents_month
  )
final_Manpower_MDRT <-
  rbind(
    Manpower_MDRT_in_ytd,
    Manpower_MDRT_in_quarter,
    Manpower_MDRT_in_month
  )

dbDisconnect(my_database$con)