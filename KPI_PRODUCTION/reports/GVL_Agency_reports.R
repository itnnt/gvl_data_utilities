
# MAIN PROCESS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(interactive()) {
  source("KPI_PRODUCTION/set_environment.r")
  # Set study peiod ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  fr <- as.Date('2017-07-01')
  to <- as.Date('2017-07-31')
  # Generate a sequence of the fisrt day and last day of the month over 5 years ---------------------------------------------------------------------------------------------------------------------
  first_day_of_months <- seq(from=fr, to=to, by="1 month")
  last_day_of_months <- seq(from=seq(fr, length=2, by='1 month')[2], to=seq(to, length=2, by='1 month')[2], by="1 month") - 1
  
  # Generate a sequence of the last day of the quarter over 5 years ---------------------------------------------------------------------------------------------------------------------------------
  # last_day_of_quarters <- seq(from=seq(fr, length=2, by='1 quarter')[2], to=seq(to, length=2, by='1 quarter')[2], by="1 quarter") - 1
  # last_day_of_years <- seq(from=seq(fr, length=2, by='1 year')[2], to=seq(to, length=2, by='1 year')[2], by="1 year") - 1
  
  # Create table to save final result ---------------------------------------------------------------------------------------------------------------------------------------------------------------
  # Q ####
  # Q <- last_day_of_quarters %>% 
  #   dplyr::tbl_df() %>% # convert to data table
  #   stats::setNames(.,c('BUSSINESSDATE')) %>% # names the column
  #   dplyr::mutate(BUSSINESSDATE_FM1=quarters(BUSSINESSDATE)) %>%  # get the quarter of the bussiness date
  #   # converts the bussiness date to YYYY-QQ format
  #   dplyr::mutate(BUSSINESSDATE_FM1=paste(BUSSINESSDATE_FM1,strftime(BUSSINESSDATE,FORMAT_YEAR_y),sep="'")) 
  # M ####
  M <- last_day_of_months %>% 
    dplyr::tbl_df() %>% # convert a vector to data table
    stats::setNames(.,c('BUSSINESSDATE')) %>% 
    dplyr::mutate(BUSSINESSDATE_FM1=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) 
  # Y ####
  # Y <- last_day_of_years %>% 
  #   dplyr::tbl_df() %>% # convert a vector to data table
  #   stats::setNames(.,c('BUSSINESSDATE')) %>% 
  #   dplyr::mutate(BUSSINESSDATE_FM1=paste("YTD",strftime(BUSSINESSDATE, FORMAT_YEAR_y), sep = "'")) 
  # MoM ####
  # MoM <- last_day_of_months %>% # 
  #   dplyr::tbl_df() %>% # convert a vector to data table
  #   stats::setNames(.,c('BUSSINESSDATE')) %>% 
  #   dplyr::mutate(BUSSINESSDATE_FM1=sprintf("MoM-%s", strftime(BUSSINESSDATE, "%b-%y"))) 
  # # YoY ####
  # YoY <- last_day_of_years %>% # select the last 2 elements
  #   dplyr::tbl_df() %>% # convert a vector to data table
  #   stats::setNames(.,c('BUSSINESSDATE')) %>% 
  #   dplyr::mutate(BUSSINESSDATE_FM1=sprintf("YoY%s", strftime(BUSSINESSDATE, "%y"))) 
  # result_index ####
  
  # result_index <- rbind(Y, YoY, Q, M, MoM)  %>%  
  result_index <- M  %>%  
    mutate(id = row_number()) # mutates a new order col
  
  

  source("KPI_PRODUCTION/Model_Algorithms.R")
  source("KPI_PRODUCTION/Model_Algorithms_North.R")
  source("KPI_PRODUCTION/Model_Algorithms_South.R")
  
  excelFile="KPI_PRODUCTION/output/Agency Performance by Segmentation_2015_201707.xlsx"
  excelFile = replace_cellvalue2(North_results, 'North', rowNameColIndex = 1, headerRowIndex = 1, excelFile)
  excelFile = replace_cellvalue2(South_results, 'South', rowNameColIndex = 1, headerRowIndex = 1, excelFile)
}

