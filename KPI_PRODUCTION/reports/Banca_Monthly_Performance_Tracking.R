source('KPI_PRODUCTION/Common functions.R')

businessdate <- '2017-07-31'

office_ape <- get_ape_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
  dplyr::mutate(APE = APE/10^6)

office_rider_ape <- get_rider_ape_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
  dplyr::mutate(APE = APE/10^6)

office_main_case <- get_main_case_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db')
office_rider_case <- get_rider_case_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db')


office_casesize <- office_ape %>% 
  merge(x=., 
        y=office_main_case, 
        by.x=c('BUSSINESSDATE','OFFICE'),
        by.y=c('BUSSINESSDATE','OFFICE')) %>% 
  dplyr::mutate(CASESIZE=APE/CASECOUNT)

office_ape_group_designation <- get_ape_sum_by_office_designation(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
  dplyr::mutate(APE = APE/10^6)  