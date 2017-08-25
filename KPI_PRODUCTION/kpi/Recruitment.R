# Create a new connection to main_database.db -----------------------------------------------------------------------------------------------------------------------------------------------------
dbfile = 'KPI_PRODUCTION/main_database.db'
my_database<- src_sqlite(dbfile, create = TRUE)

# Total # New recruits ----------------------------------------------------
data = get_Manpower(last_day_of_months)

Total_New_recruits_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
      substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`Total # New recruits` = n()) 
Total_New_recruits_quarter <- Total_New_recruits_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total # New recruits` = sum(`Total # New recruits`)) 

Total_New_recruits_ytd <- Total_New_recruits_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total # New recruits` = sum(`Total # New recruits`))  

# Total_New_recruits_AG ---------------------------------------------------
data <- get_Manpower_AG(last_day_of_months)
Total_New_recruits_AG_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
    substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`AG` = n())  

Total_New_recruits_AG_quarter <- Total_New_recruits_AG_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`AG` = sum(`AG`))  
Total_New_recruits_AG_ytd <- Total_New_recruits_AG_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`AG` = sum(`AG`))  

# Total_New_recruits_US ---------------------------------------------------
data = get_Manpower_US(last_day_of_months)
Total_New_recruits_US_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
      substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%  
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`US` = n())  
Total_New_recruits_US_quarter <- Total_New_recruits_US_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`US` = sum(`US`))  
Total_New_recruits_US_ytd <- Total_New_recruits_US_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`US` = sum(`US`))  

# Total_New_recruits_UM ---------------------------------------------------
data = get_Manpower_UM(last_day_of_months)
Total_New_recruits_UM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
      substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`UM` = n())  
Total_New_recruits_UM_quarter <- Total_New_recruits_UM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`UM` = sum(`UM`))  
Total_New_recruits_UM_ytd <- Total_New_recruits_UM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`UM` = sum(`UM`))  

# Total_New_recruits_SUM ---------------------------------------------------
data = get_Manpower_SUM(last_day_of_months)
Total_New_recruits_SUM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
      substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`SUM` = n())  
Total_New_recruits_SUM_quarter <- Total_New_recruits_SUM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SUM` = sum(`SUM`))  
Total_New_recruits_SUM_ytd <- Total_New_recruits_SUM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SUM` = sum(`SUM`))  

# Total_New_recruits_BM ---------------------------------------------------
data = get_Manpower_BM(last_day_of_months)
Total_New_recruits_BM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
      substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`BM` = n()) 

Total_New_recruits_BM_quarter <- Total_New_recruits_BM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`BM` = sum(`BM`)) 

Total_New_recruits_BM_ytd <- Total_New_recruits_BM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`BM` = sum(`BM`)) 

# Total_New_recruits_SBM ---------------------------------------------------
data = get_Manpower_SBM(last_day_of_months)
Total_New_recruits_SBM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) |   
      substr(BUSSINESSDATE, 1, 7)==substr(REINSTATEMENT_DATE, 1, 7)
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`SBM` = n()) 

Total_New_recruits_SBM_quarter <- Total_New_recruits_SBM_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SBM` = sum(`SBM`)) 

Total_New_recruits_SBM_ytd <- Total_New_recruits_SBM_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`SBM` = sum(`SBM`)) 

# Total recruited AL ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
AL_US <- Total_New_recruits_US_month
AL_UM <- Total_New_recruits_UM_month
AL_SUM <- Total_New_recruits_SUM_month
AL_BM <- Total_New_recruits_BM_month
AL_SBM <- Total_New_recruits_SBM_month
names(AL_US) <- gsub('\\<US\\>','Total recruited AL', names(AL_US))
names(AL_UM) <- gsub('\\<UM\\>','Total recruited AL', names(AL_UM))
names(AL_SUM) <- gsub('\\<SUM\\>','Total recruited AL', names(AL_SUM))
names(AL_BM) <- gsub('\\<BM\\>','Total recruited AL', names(AL_BM))
names(AL_SBM) <- gsub('\\<SBM\\>','Total recruited AL', names(AL_SBM))

Total_recruited_AL_month <- rbind(AL_US , 
                                  AL_UM ,
                                  AL_SUM,
                                  AL_BM ,
                                  AL_SBM
                                  ) %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total recruited AL` = sum(`Total recruited AL`)) 

Total_recruited_AL_quarter <- Total_recruited_AL_month %>% 
  change_BUSSINESSDATE_Q() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total recruited AL` = sum(`Total recruited AL`))  
Total_recruited_AL_ytd <- Total_recruited_AL_month %>% 
  change_BUSSINESSDATE_YTD() %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`Total recruited AL` = sum(`Total recruited AL`))  

# final -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
final_Total_New_recruits <-
  rbind(
    Total_New_recruits_ytd,
    Total_New_recruits_quarter,
    Total_New_recruits_month
  )
final_Total_New_recruits_AG <-
  rbind(
    Total_New_recruits_AG_ytd,
    Total_New_recruits_AG_quarter,
    Total_New_recruits_AG_month
  )
final_Total_New_recruits_US <-
  rbind(
    Total_New_recruits_US_ytd,
    Total_New_recruits_US_quarter,
    Total_New_recruits_US_month
  )
final_Total_New_recruits_UM <-
  rbind(
    Total_New_recruits_UM_ytd,
    Total_New_recruits_UM_quarter,
    Total_New_recruits_UM_month
  )
final_Total_New_recruits_SUM <-
  rbind(
    Total_New_recruits_SUM_ytd,
    Total_New_recruits_SUM_quarter,
    Total_New_recruits_SUM_month
  )
final_Total_New_recruits_BM <-
  rbind(
    Total_New_recruits_BM_ytd,
    Total_New_recruits_BM_quarter,
    Total_New_recruits_BM_month
  )
final_Total_New_recruits_SBM <-
  rbind(
    Total_New_recruits_SBM_ytd,
    Total_New_recruits_SBM_quarter,
    Total_New_recruits_SBM_month
  )
final_Total_recruited_AL <-
  rbind(
    Total_recruited_AL_ytd,
    Total_recruited_AL_quarter,
    Total_recruited_AL_month
  )

dbDisconnect(my_database$con)