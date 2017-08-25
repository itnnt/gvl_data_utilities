# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

#### Individual_APE ####
agent_ape_sum_bymonth = get_ape(last_day_of_months)

SQL <- sprintf("SELECT BUSSINESSDATE, AGCODE, REGIONCD, APE, B.TERRITORY FROM RAWDATA_KPITotal A
               LEFT JOIN RAWDATA_MAPPING_TERRITORY_REGION B ON A.REGIONCD = B.REGION_CODE
               WHERE BUSSINESSDATE >= '%s'", strftime(fr, '%Y%m%d'))
result <- dbSendQuery(my_database$con, SQL)
data = fetch(result, encoding="utf-8")
dbClearResult(result)

Individual_APE <- data

# Manpower MDRT -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_MDRT <- get_Manpower_MDRT(last_day_of_months[last_day_of_months<'2017-04-30'])

# APE_MDRT ------------------------------------------------------------
Active_MDRT <- get_active_MDRT(last_day_of_months)

APE_MDRT <- Manpower_MDRT %>% 
  merge(x=., 
        y=agent_ape_sum_bymonth, 
        by.x=c('BUSSINESSDATE','AGENT_CODE'),
        by.y=c('BUSSINESSDATE','AGCODE'))


APE_MDRT_month <- APE_MDRT %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(MDRT = sum(APE)/10^6) 


APE_MDRT_quarter <- APE_MDRT_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(MDRT = sum(MDRT)) 

APE_MDRT_ytd <- APE_MDRT_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(MDRT = sum(MDRT)) 


#### APE_total
APE_total <- Individual_APE %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y%m%d'))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(APE = sum(APE))  

# Manpower_GenLion ------------------------------------------------------------
Manpower_GenLion = get_Manpower_genlion(last_day_of_months)

# APE_GenLion ------------------------------------------------------------
APE_GenLion <- Manpower_GenLion %>% 
  merge(x=., 
        y=agent_ape_sum_bymonth, 
        by.x=c('BUSSINESSDATE','AGENT_CODE'),
        by.y=c('BUSSINESSDATE','AGCODE'))


APE_GenLion_month <- APE_GenLion %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(GenLion = sum(APE)/10^6) 


APE_GenLion_quarter <- APE_GenLion_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(GenLion = sum(GenLion)) 

APE_GenLion_ytd <- APE_GenLion_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(GenLion = sum(GenLion)) 


# APE_Rookie_in_month -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Manpower_excluded_sa <- get_Manpower_excluded_SA(last_day_of_months, included_ter_ag = T)

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
  merge(x=., 
        y=agent_ape_sum_bymonth, 
        by.x=c('BUSSINESSDATE','AGENT_CODE'),
        by.y=c('BUSSINESSDATE','AGCODE')) %>% 
  convert_string_to_date('JOINING_DATE', FORMAT_BUSSINESSDATE) %>%
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%
  # calculate different months between joining date and business date
  dplyr::mutate(MDIFF = as.integer(round((
    as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
  ) * 12)))

APE_Rookie_in_month <- data %>%
  filter(MDIFF==0)


APE_Rookie_in_month_month <- APE_Rookie_in_month %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Rookie in month` = sum(APE)/1000000) 


APE_Rookie_in_month_quarter <- APE_Rookie_in_month_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie in month` = sum(`Rookie in month`)) 

APE_Rookie_in_month_ytd <- APE_Rookie_in_month_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie in month` = sum(`Rookie in month`)) 

# APE Rookie last month -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
APE_Rookie_last_month <- data %>%
  filter(MDIFF ==1)

APE_Rookie_last_month_month <- APE_Rookie_last_month %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Rookie last month` = sum(APE)/1000000) 

APE_Rookie_last_month_quarter <- APE_Rookie_last_month_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie last month` = sum(`Rookie last month`)) 

APE_Rookie_last_month_ytd <- APE_Rookie_last_month_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Rookie last month` = sum(`Rookie last month`)) 

# APE Rookie in 2, 3 months ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
APE_Rookie_2_3_months <- data %>%
  filter(MDIFF %in% c(2:3))

APE_Rookie_2_3_months_month <- APE_Rookie_2_3_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`2-3 months` = sum(APE)/10^6)  

APE_Rookie_2_3_months_quarter <- APE_Rookie_2_3_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`2-3 months` = sum(`2-3 months`))  
APE_Rookie_2_3_months_ytd <- APE_Rookie_2_3_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`2-3 months` = sum(`2-3 months`))  

# APE Rookie 4,6 months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
APE_Rookie_4_6_months <- data %>%
  filter(MDIFF %in% c(4:6))

APE_Rookie_4_6_months_month <- APE_Rookie_4_6_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`4 - 6 mths` = sum(APE)/10^6)  

APE_Rookie_4_6_months_quarter <- APE_Rookie_4_6_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`4 - 6 mths` = sum(`4 - 6 mths`))  
APE_Rookie_4_6_months_ytd <- APE_Rookie_4_6_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`4 - 6 mths` = sum(`4 - 6 mths`))  

# APE Rookie 7,12 months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
APE_Rookie_7_12_months <- data %>%
  filter(MDIFF %in% c(7:12)) 

APE_Rookie_7_12_months_month <- APE_Rookie_7_12_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`7-12mth` = sum(APE)/10^6)  

APE_Rookie_7_12_months_quarter <- APE_Rookie_7_12_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`7-12mth` = sum(`7-12mth`))  
APE_Rookie_7_12_months_ytd <- APE_Rookie_7_12_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`7-12mth` = sum(`7-12mth`))  

# APE Rookie 13+ months -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
APE_Rookie_13_and_more_months <- data %>%
  filter(MDIFF >= 13) 

APE_Rookie_13_and_more_months_month <- APE_Rookie_13_and_more_months %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`13+mth` = sum(APE)/10^6)  

APE_Rookie_13_and_more_months_quarter <- APE_Rookie_13_and_more_months_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`13+mth` = sum(`13+mth`))  
APE_Rookie_13_and_more_months_ytd <- APE_Rookie_13_and_more_months_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`13+mth` = sum(`13+mth`))  

# APE SA agents -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
APE_SA_agents <- 
  get_ape_sa(last_day_of_months) 

APE_SA_agents_month <- APE_SA_agents %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`SA` = sum(APE)/10^6)  

APE_SA_agents_quarter <- APE_SA_agents_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SA` = sum(`SA`))  
APE_SA_agents_ytd <- APE_SA_agents_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SA` = sum(`SA`))  

final_APE_GenLion <-
  rbind(
    APE_GenLion_ytd,
    APE_GenLion_quarter,
    APE_GenLion_month
  )
final_APE_MDRT <-
  rbind(
    APE_MDRT_ytd,
    APE_MDRT_quarter,
    APE_MDRT_month
  )
final_APE_Rookie_in_month <-
  rbind(
    APE_Rookie_in_month_ytd,
    APE_Rookie_in_month_quarter,
    APE_Rookie_in_month_month
  )
final_APE_Rookie_last_month <-
  rbind(
    APE_Rookie_last_month_ytd,
    APE_Rookie_last_month_quarter,
    APE_Rookie_last_month_month
  )
final_APE_Rookie_2_3_months <-
  rbind(
    APE_Rookie_2_3_months_ytd,
    APE_Rookie_2_3_months_quarter,
    APE_Rookie_2_3_months_month
  )
final_APE_Rookie_4_6_months <-
  rbind(
    APE_Rookie_4_6_months_ytd,
    APE_Rookie_4_6_months_quarter,
    APE_Rookie_4_6_months_month
  )
final_APE_Rookie_7_12_months <-
  rbind(
    APE_Rookie_7_12_months_ytd,
    APE_Rookie_7_12_months_quarter,
    APE_Rookie_7_12_months_month
  )
final_APE_Rookie_13_and_more_months <-
  rbind(
    APE_Rookie_13_and_more_months_ytd,
    APE_Rookie_13_and_more_months_quarter,
    APE_Rookie_13_and_more_months_month
  )
final_APE_SA_agents <-
  rbind(
    APE_SA_agents_ytd,
    APE_SA_agents_quarter,
    APE_SA_agents_month
  )

dbDisconnect(my_database$con)