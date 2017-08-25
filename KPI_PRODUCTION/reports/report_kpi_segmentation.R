source('KPI_PRODUCTION/Common functions.R')

private_add_cols_changed_colnames_and_save <- function(df, kpiname) {
  names(df) <- gsub("\\<TERRITORY\\>", "territory", names(df))
  names(df) <- gsub("\\<BUSSINESSDATE\\>", "time_view", names(df))
  df <- dplyr::mutate(df, kpi=kpiname)
  df <- dplyr::mutate(df, yy= sub(".*(\\d+{2}).*$", "\\1", df$time_view))
  insert_or_replace_bulk(df, 'report_kpi_segmentation')
}


business_dates = c('2017-07-31')
# 
# APE_by_rookie_mdrt:MDRT ---------------------
# 
agent_ape_sum_bymonth = get_ape(business_dates)
Manpower_MDRT <- get_Manpower_MDRT(business_dates)
APE_MDRT <- Manpower_MDRT %>% 
  merge(x=., 
        y=agent_ape_sum_bymonth, 
        by.x=c('BUSSINESSDATE','AGENT_CODE'),
        by.y=c('BUSSINESSDATE','AGCODE'))
APE_MDRT_month <- APE_MDRT %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(value = sum(APE)/10^6)  %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(APE_MDRT_month, 'APE_by_rookie_mdrt:MDRT')

#
# APE_by_rookie_mdrt:Rookie in month ----
#
#
Manpower_excluded_sa <- get_Manpower_excluded_SA(business_dates, included_ter_ag = T)
data_excluded_mdrt <-  merge(x=Manpower_excluded_sa,
                             y=dplyr::select(Manpower_MDRT, c(AGENT_CODE,BUSSINESSDATE,MDRT)),
                             by.x=c('BUSSINESSDATE','AGENT_CODE'),
                             by.y=c('BUSSINESSDATE','AGENT_CODE'),
                             all.x=T) %>% 
  dplyr::filter(is.na(MDRT)) %>% 
  dplyr::select(-MDRT)
data <- data_excluded_mdrt %>% 
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
  filter(MDIFF==0) %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = sum(APE)/10^6) %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(APE_Rookie_in_month, 'APE_by_rookie_mdrt:Rookie in month')

#
# APE_by_rookie_mdrt:Rookie last month ----
#
APE_Rookie_last_month <- data %>%
  filter(MDIFF==1) %>% 
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = sum(APE)/10^6) %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(APE_Rookie_last_month, 'APE_by_rookie_mdrt:Rookie last month')

#
# RYP_by_rookie_mdrt:MDRT ---- DANG BI SAI ----
#
agent_ryp_sum_bymonth = get_ryp(business_dates)
Manpower_MDRT <- get_Manpower_MDRT(business_dates)
RYP_MDRT <- Manpower_MDRT %>% 
  merge(x=., 
        y=agent_ryp_sum_bymonth, 
        by.x=c('BUSSINESSDATE','AGENT_CODE'),
        by.y=c('BUSSINESSDATE','AGCODE'))
RYP_MDRT_month <- RYP_MDRT %>% 
  na.omit %>% # omit na values
  dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% # parsing to date
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(value = sum(RYP)/10^6)  %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(RYP_MDRT_month, 'RYP_by_rookie_mdrt:MDRT')

#
# Recruit_by_designation:AG ----
#
data <- get_Manpower_AG(business_dates, included_ter_ag = T)
Total_New_recruits_AG_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n())  %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_AG_month, 'Recruit_by_designation:AG')

#
# Recruit_by_designation:US ----
#
data = get_Manpower_US(business_dates)
Total_New_recruits_US_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%  
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_US_month, 'Recruit_by_designation:US')
#
# Recruit_by_designation:UM----
#
data = get_Manpower_UM(business_dates)
Total_New_recruits_UM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n())  %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_UM_month, 'Recruit_by_designation:UM')

#
# Recruit_by_designation:SUM ----
#
data = get_Manpower_SUM(business_dates)
Total_New_recruits_SUM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n())  %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_SUM_month, 'Recruit_by_designation:SUM')

#
# Recruit_by_designation:BM----
#
data = get_Manpower_BM(business_dates)
Total_New_recruits_BM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_BM_month, 'Recruit_by_designation:BM')

#
# Recruit_by_designation:SBM----
#
data = get_Manpower_SBM(business_dates)
Total_New_recruits_SBM_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>%   
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n())%>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_SBM_month, 'Recruit_by_designation:SBM')

#
# Recruit_by_designation:Total ----
#
data = get_Manpower(business_dates, included_ter_ag = T)
Total_New_recruits_month <- data %>% 
  # filter agent whose joining dates or reinstatement dates in the month of the business date
  dplyr::filter(
    substr(BUSSINESSDATE, 1, 7)==substr(JOINING_DATE, 1, 7) 
  ) %>% 
  convert_string_to_date('BUSSINESSDATE', FORMAT_BUSSINESSDATE) %>% 
  group_by(TERRITORY, BUSSINESSDATE=strftime(BUSSINESSDATE, FORMAT_MONTH_YEAR_y_b)) %>% # group data by month
  summarise(`value` = n()) %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Total_New_recruits_month, 'Recruit_by_designation:Total')

#
# Recruit_AL ----
#
Recruit_AL <- rbind(
  Total_New_recruits_US_month,
  Total_New_recruits_UM_month,
  Total_New_recruits_SUM_month,
  Total_New_recruits_BM_month,
  Total_New_recruits_SBM_month
) %>% 
  group_by(TERRITORY, BUSSINESSDATE) %>% # group data by month
  summarise(`value` = sum(value)) %>% 
  dplyr::ungroup(.)
private_add_cols_changed_colnames_and_save(Recruit_AL, kpiname = 'Recruit_AL')

#
# #active recruit leader: us, um, bm recruited atleast 1 agent in month----
# 
for (bsdt in generate_last_day_of_month(2017)) {
  rs <- active_recruit_leader(as.Date(bsdt))
  private_add_cols_changed_colnames_and_save(rs, kpiname = 'active_recruit_leader')
}


