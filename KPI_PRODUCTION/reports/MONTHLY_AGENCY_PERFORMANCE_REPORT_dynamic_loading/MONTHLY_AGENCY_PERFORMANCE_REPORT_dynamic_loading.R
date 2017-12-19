source('KPI_PRODUCTION/Common functions.R')
source('KPI_PRODUCTION/kpi_functions.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading__active_ratio.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading__casesize.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading__recruit.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading__ape.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading__active_agent.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading/MONTHLY_AGENCY_PERFORMANCE_REPORT_dynamic_loading__data_loading.R')
source('KPI_PRODUCTION/reports/PLAN_IMPORT/plan_north.R')
source('KPI_PRODUCTION/reports/PLAN_IMPORT/plan_south.R')
source('KPI_PRODUCTION/reports/PLAN_IMPORT/plan_country.R')




# get_group_manpower_monthly <- function(yyyy, ad) {
#   manpower = do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
#     get_manpower_individual(d, dbfile) %>%  
#       merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
#       dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#       merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
#       dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#       merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
#       dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#       merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
#       dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#       dplyr::filter(!is.na(TERRITORY))
#   })
#   ) %>% dplyr::mutate(SEG = ifelse(!is.na(SERVICING_AGENT) & toupper(SERVICING_AGENT)=='YES', 'SA', ifelse(!is.na(STAFFCD) & STAFFCD!='', STAFFCD, AGENT_DESIGNATION))) %>% 
#   count_group_by(., f1='time_view', f2='SEG', summarise_field_name='MANPOWER') 
# }

get_group_manpower_monthly <- function(yyyy, ad) {
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    manpower_designation_segment(d)
  })
  ) 
}

group_data_sum <- function(df, summarise_field_name) {
  df[,'value'] = df[, summarise_field_name]
  re = rbind(
    df %>% dplyr::group_by(time_view, segment = 'COUNTRY', group_level = 'COUNTRY', order_idx = 0) %>%
      dplyr::summarise(value = sum(value, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = TERRITORY, group_level = 'TERRITORY', order_idx = 1) %>%
      dplyr::summarise(value = sum(value, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = REGIONCD, group_level = 'REGION', order_idx = 2) %>%
      dplyr::summarise(value = sum(value, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = ZONECD, group_level = 'ZONE', order_idx = 3) %>%
      dplyr::summarise(value = sum(value, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = OFFICECD, group_level = 'OFFICE', order_idx = 4) %>%
      dplyr::summarise(value = sum(value, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = TEAMCD, group_level = 'TEAM', order_idx = 5) %>%
      dplyr::summarise(value = sum(value, na.rm=T)) %>%
      dplyr::ungroup(.)
  )
  re[,summarise_field_name]=re$value
  re %>% dplyr::select(-value)
}
group_data_detail_in_segment_sum <- function(df, summarise_field_name) {
  df[,'value'] = df[, summarise_field_name]
  re = rbind(
    df %>% dplyr::group_by(time_view, SEG, idx, segment = 'COUNTRY') %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = TERRITORY) %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = REGIONCD) %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = ZONECD) %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = OFFICECD) %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = TEAMCD) %>%
      dplyr::summarise(value = sum(value)) %>%
      dplyr::ungroup(.)
  )
  re[,summarise_field_name]=re$value
  re %>% dplyr::select(-value)
}
group_data_n <- function(df, summarise_field_name){
  re = rbind(
    df %>% dplyr::group_by(time_view, segment = 'COUNTRY', group_level = 'COUNTRY', order_idx = 0) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = TERRITORY, group_level = 'TERRITORY', order_idx = 1) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = REGIONCD, group_level = 'REGION', order_idx = 2) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = ZONECD, group_level = 'ZONE', order_idx = 3) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = OFFICECD, group_level = 'OFFICE', order_idx = 4) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, segment = TEAMCD, group_level = 'TEAM', order_idx = 5) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
  ) 
  re[,summarise_field_name]=re$value
  re %>% dplyr::select(-value)
}
group_data_detail_in_segment_n <- function(df, summarise_field_name){
  re = rbind(
    df %>% dplyr::group_by(time_view, SEG, idx, segment = 'COUNTRY') %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = TERRITORY) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = REGIONCD) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = ZONECD) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = OFFICECD) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
    ,
    df %>% dplyr::group_by(time_view, SEG, idx, segment = TEAMCD) %>%
      dplyr::summarise(value=n()) %>%
      dplyr::ungroup(.)
  )
  re[,summarise_field_name]=re$value
  re %>% dplyr::select(-value)
}
load_data_rookie_performance_in90days <- function(bssdt, rowindx) {
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  
  # rookie_performance_individual_in_first_90days
  # new_recruited_in90days = rookie_performance_individual_in_first_90days(bssdt, number_of_studied_months=11, dbfile)
  new_recruited_in90days = rookie_performance_individual_in_first_90days_fullyear(strftime(bssdt, '%Y'), dbfile)
  new_recruited_in90days[,'idx'] = 0
  try((new_recruited_in90days[new_recruited_in90days$SEG=='1case/15d',]$idx=1+rowindx), silent=T)
  try((new_recruited_in90days[new_recruited_in90days$SEG=='1case/30d',]$idx=2+rowindx), silent=T)
  try((new_recruited_in90days[new_recruited_in90days$SEG=='3case/60d',]$idx=3+rowindx), silent=T)
  try((new_recruited_in90days[new_recruited_in90days$SEG=='5case/90d',]$idx=4+rowindx), silent=T)
  
  # header = new_recruited_in90days %>% select(time_view, M) %>% unique()
  
  # %>% dplyr::mutate(time_view = strftime(as.Date(strptime(
  #     paste(time_view, '01', sep = ''), '%Y%m%d'
  #   )), '%b-%Y')) 
  
  re = new_recruited_in90days %>% 
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  # re = group_data_detail_in_segment_sum(re, 'CASE') %>% 
  re = group_data_detail_in_segment_sum(re, 'NUMBER_OF_CASE_MEET_CRITERIA') %>% 
    dplyr::filter(!is.na(segment)) %>% 
    dplyr::mutate(index = paste(segment, 'rookie90days',SEG, sep = '-')) 
    # merge(
    #   x = .,
    #   y = header,
    #   by.x = c('time_view'),
    #   by.y = c('time_view'),
    #   all.x = T
    # )
  rookie_per_in_90day_countcase_result = re %>% 
    # dplyr::select(-M) %>% 
    # tidyr::spread(time_view, CASE) %>% 
    tidyr::spread(time_view, NUMBER_OF_CASE_MEET_CRITERIA) %>% 
    dplyr::mutate(ROOKIE_PERFORMANCE_IN90DAYS_SEG = SEG) %>% 
    dplyr::select(-SEG, -idx)
  
  agent_total_in90days = rookie_agent_numberofday_joined_first90days_fullyear(strftime(bssdt, '%Y'), dbfile)
  agent_total_in90days[,'idx'] = 0
  re1 = agent_total_in90days %>% 
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  re1 = group_data_detail_in_segment_sum(re1, 'AGENT_COUNT') %>% 
    dplyr::filter(!is.na(segment)) %>% 
    dplyr::mutate(index = paste(segment, 'rookie90days',sprintf('%s%s','%',SEG), sep = '-')) %>% 
    dplyr::select(-idx)
  
  rookie_per_in_90day_countpercent_result = re %>% 
    dplyr::select(-index) %>% 
    merge(
      x = .,
      y = re1,
      by.x = c("time_view", "SEG", "segment" ),
      by.y = c("time_view", "SEG", "segment" )
    ) %>% 
    dplyr::mutate(PERCENTAGE_MEET_CRITERIA = NUMBER_OF_CASE_MEET_CRITERIA/AGENT_COUNT) %>% 
    dplyr::select(-NUMBER_OF_CASE_MEET_CRITERIA, -AGENT_COUNT, -idx) %>% 
    tidyr::spread(time_view, PERCENTAGE_MEET_CRITERIA) %>% 
    dplyr::mutate(ROOKIE_PERFORMANCE_IN90DAYS_SEG = SEG) %>% 
    dplyr::select(-SEG) 
  
  rbind(rookie_per_in_90day_countcase_result, rookie_per_in_90day_countpercent_result)
}

load_product_mix <- function(bssdt, offset, total_ape_rowidx) {
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  
  prd_mix = do.call(rbind, lapply(generate_last_day_of_month(strftime(bssdt, '%Y')), function(d) {
    product_mix_individual(d, dbfile) %>% dplyr::mutate(M=time_view)
  })
  ) %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    dplyr::mutate(PRODUCTCODE = ifelse(PRODUCT_TYPE=='BASEPRODUCT', PRODUCTCODE, PRODUCT_TYPE)) %>% # keep product code for base products only
    dplyr::mutate(PRODUCTNAME = ifelse(PRODUCT_TYPE=='BASEPRODUCT', PRODUCTNAME, PRODUCT_TYPE)) # keep product name for base products only
  
  group_prd_mix_total = rbind(
    prd_mix %>% dplyr::group_by(time_view, M, segment = 'COUNTRY') %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', segment = 'COUNTRY') %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, segment = TERRITORY) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', segment = TERRITORY) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, segment = REGIONCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', segment = REGIONCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, segment = ZONECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', segment = ZONECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, segment = OFFICECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', segment = OFFICECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, segment = TEAMCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', segment = TEAMCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
  ) %>% dplyr::filter(!is.na(segment))
  
  group_prd_mix = rbind(
    prd_mix %>% dplyr::group_by(time_view, M, SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = 'COUNTRY') %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = 'COUNTRY') %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = TERRITORY) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = TERRITORY) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = REGIONCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = REGIONCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = ZONECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = ZONECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = OFFICECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = OFFICECD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    prd_mix %>% dplyr::group_by(time_view, M, SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = TEAMCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
    ,
    # SUM SUM6M
    prd_mix %>% dplyr::group_by(time_view = 'SUM6M', M='SUM6M', SEG = PRODUCTCODE, PRODUCT_TYPE, PRODUCTNAME, segment = TEAMCD) %>%
      dplyr::summarise(APE = sum(APE, na.rm=T)) %>%
      dplyr::ungroup(.)
  ) %>%
    merge(
      x = .,
      y = group_prd_mix_total,
      by.x = c('time_view', 'M','segment'),
      by.y = c('time_view', 'M','segment')
    ) %>% 
    dplyr::filter(!is.na(segment)) %>% 
    dplyr::mutate(value = APE.x/APE.y) %>%  
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.)))   # remove columns that have sufix is ".x" or '.y' (if any)
  
  # offset = 26 
  prd_mix_data = group_prd_mix %>% select (-time_view) %>% 
    tidyr::spread(M, value) %>% 
    dplyr::arrange(segment, PRODUCT_TYPE, -SUM6M) %>% 
    dplyr::group_by(segment) %>% 
    dplyr::mutate(row_idx = offset+row_number()) %>% 
    dplyr::ungroup()
  

  prd_mix_totalape = group_prd_mix_total %>% select(-time_view) %>% 
    dplyr::mutate(APE = APE/10^6) %>% 
    tidyr::spread(M, APE) %>% 
    dplyr::mutate(row_idx = total_ape_rowidx) %>% 
    dplyr::mutate( SEG='APE (mil)', PRODUCT_TYPE='', PRODUCTNAME='')
  
  prdmixfinal = rbind(
    prd_mix_data
  ,
    prd_mix_totalape
  ) %>% 
    dplyr::mutate(index = paste(segment, 'ProductMix', row_idx, sep = '')) 
    
  prdmixfinal[,(sprintf('%s', strftime(bssdt, '%Y')))]=prdmixfinal[,'SUM6M'] 
  prdmixfinal %>% dplyr::select(-SUM6M)
    
}

create_report <- function (bssdt, genlion_final_dt, exceltemplate, excelFile){
  # bssdt = as.Date('2017-10-31')
  # genlion_final_dt = as.Date('2017-09-30')
  # excelFile = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_PERFORMANCE_REPORT_%s_dynamic.xlsx", strftime(bssdt,'%Y-%m-%d'))
  # exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\(new) template_MONTHLY_AGENCY_PERFORMANCE_REPORT_2017-10-31_dynamic.xlsx"
  bssdt_m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
  y1 = as.Date(sprintf('%s-12-31', as.numeric(strftime(bssdt, '%Y'))-1))
  write_from = 3
  
  fill_excel_cell(bssdt, 
                  sheetname = "Cover",
                  7, 
                  6, 
                  exceltemplate, excelFile)
  # fill_excel_cell(bssdt_m1, 
  #                 sheetname = "Cover",
  #                 7, 
  #                 7, 
  #                 excelFile, excelFile)
  sale_chart = ad_list_full_structure(dbfile) 
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  re = load_data(bssdt, rowindx = 7)
  write_from = fill_excel_column1.4_no_fmt(
    df = re,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  write_from = fill_excel_column1.4_no_fmt(
    df = ape_growth,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  write_from = fill_excel_column1.4_no_fmt(
    df = active_growth,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  # 
  write_from = fill_excel_column1.4_no_fmt(
    df = merge(
      x = casesize_growth_monthly, 
      y = casesize_growth_ytd,
      by.x = c('segment', 'index'),
      by.y = c('segment', 'index'),
      all = T
    ),
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  write_from = fill_excel_column1.4_no_fmt(
    df = merge(
      x = case_per_active_growth_monthly, 
      y = case_per_active_growth_ytd,
      by.x = c('segment', 'index'),
      by.y = c('segment', 'index'),
      all = T
    ),
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  prdmix = load_product_mix(bssdt, offset=7, total_ape_rowidx=29) %>% 
    dplyr::mutate(PRODUCT_MIX_SEG = SEG) %>% 
    dplyr::select(-SEG)
  write_from = fill_excel_column1.4_no_fmt(
    df = prdmix,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  )
  
  rookie90days = load_data_rookie_performance_in90days(bssdt, rowindx = 0)
  write_from = fill_excel_column1.4_no_fmt(
    df = rookie90days,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  group_recruit_monthly = get_group_recruit_monthly(strftime(bssdt, '%Y')) %>% 
    dplyr::mutate(index = paste(segment, 'New-Recruit', sep = '-'))
  group_recruit_monthly_y1 = get_group_recruit_monthly(strftime(y1, '%Y')) %>% 
    dplyr::mutate(index = paste(segment, 'New-Recruit', sep = '-'))
  write_from = fill_excel_column1.4_no_fmt(
    df = group_recruit_monthly  %>% 
      dplyr::mutate(index = paste(segment, 'rookie90days','recruit', sep = '-')),
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  write_from = fill_excel_column1.4_no_fmt(
    df = merge(
      x = group_recruit_monthly,
      y = group_recruit_monthly_y1,
      by.x = c('segment', 'index'),
      by.y = c('segment', 'index'),
      all.x = T
      ),
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  group_manpower_monthly = get_group_manpower_monthly(strftime(bssdt, '%Y'), ad)
  group_manpower_monthly = group_manpower_monthly %>%
  dplyr::filter(!is.na(segment)) %>% 
  tidyr::spread(time_view, MANPOWER) %>% 
  dplyr::mutate(index=paste(segment, 'Manpower', SEG, sep="-")) 
  write_from = fill_excel_column1.4_no_fmt(
    df = group_manpower_monthly,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  group_active_recruit_leader_monthly = get_group_active_recruit_leader_monthly(strftime(bssdt, '%Y'), ad)
  group_active_recruit_leader_monthly = group_active_recruit_leader_monthly %>%
  dplyr::filter(!is.na(segment))  
  write_from = fill_excel_column1.4_no_fmt(
    df = group_active_recruit_leader_monthly,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  group_recruitment_monthly = get_group_recruitment_monthly(strftime(bssdt, '%Y'), ad)
  group_recruitment_monthly = group_recruitment_monthly %>%
  dplyr::filter(!is.na(segment)) %>% 
  tidyr::spread(time_view, RECRUITMENT) %>% 
  dplyr::mutate(index=paste(segment, 'RECRUITMENT', SEG, sep="-")) 
  write_from = fill_excel_column1.4_no_fmt(
    df = group_recruitment_monthly,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  # DOING 20171115
  data_detail_in_segment = load_data_detail_in_segment(bssdt, genlion_final_dt)
  write_from = fill_excel_column1.4_no_fmt(
    df = data_detail_in_segment,
    sheetname = "Data",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template = excelFile,
    result_file = excelFile
  ) 
  
  
  
  detail_data_seg_chart = load_data_detail_in_segment_fullyear(2017, genlion_final_dt)
  dtl_seg = rbind(
    dplyr::select(detail_data_seg_chart, time_view, segment, SEG, MANPOWER) %>%
      dplyr::mutate(index = paste(segment, 'Segmentation', 'MP', SEG, sep = '-')) %>% 
      dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
      tidyr::spread(time_view, MANPOWER)
    ,
    dplyr::select(detail_data_seg_chart, time_view, segment, SEG, APE) %>% 
      dplyr::mutate(index = paste(segment, 'Segmentation', 'APE',SEG , sep = '-')) %>% 
      dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
      tidyr::spread(time_view, APE)
    ,
    dplyr::select(detail_data_seg_chart, time_view, segment, SEG, CASE) %>% 
      dplyr::mutate(index = paste(segment, 'Segmentation', 'CASE',SEG , sep = '-')) %>% 
      dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
      tidyr::spread(time_view, CASE)
    ,
    dplyr::select(detail_data_seg_chart, time_view, segment, SEG, ACTIVE_RATIO) %>% 
      dplyr::mutate(index = paste(segment, 'Segmentation', 'ACTIVE_RATIO',SEG , sep = '-')) %>% 
      dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
      tidyr::spread(time_view, ACTIVE_RATIO)
    ,
    dplyr::select(detail_data_seg_chart, time_view, segment, SEG, CASESIZE) %>% 
      dplyr::mutate(index = paste(segment, 'Segmentation', 'CASESIZE',SEG , sep = '-')) %>% 
      dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
      tidyr::spread(time_view, CASESIZE)
    ,
    dplyr::select(detail_data_seg_chart, time_view, segment, SEG, CASEPERACTIVE) %>% 
      dplyr::mutate(index = paste(segment, 'Segmentation', 'CASEPERACTIVE',SEG , sep = '-')) %>% 
      dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
      tidyr::spread(time_view, CASEPERACTIVE)
  )
  write_from = fill_excel_column1.4_no_fmt(
      df = dtl_seg,
      sheetname = "Data",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
      template = excelFile,
      result_file = excelFile
    )
  write_from = fill_excel_column1.4_no_fmt(
      df = plan_north_ape,
      sheetname = "Data",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
      template = excelFile,
      result_file = excelFile
    )
  write_from = fill_excel_column1.4_no_fmt(
      df = plan_south_ape,
      sheetname = "Data",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
      template = excelFile,
      result_file = excelFile
    )
  write_from = fill_excel_column1.4_no_fmt(
      df = plan_country_ape,
      sheetname = "Data",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
      template = excelFile,
      result_file = excelFile
    )
  
  grouplist = re %>% 
    dplyr::select(groupname=segment, order_idx) %>% 
    dplyr::filter(!is.na(order_idx) & !is.na(groupname)) %>% 
    unique() %>% 
    merge(
      x = .,
      y = sale_chart,
      by.x = 'groupname',
      by.y = 'ADNAME',
      all.x = T
    ) %>% 
    # dplyr::filter(!is.na(group_level)) %>% 
    dplyr::arrange(order_idx, groupname) %>% 
    dplyr::filter(groupname != 'DUMMY')
  grouplist[grouplist$groupname=='COUNTRY', ]$LEVEL = 'COUNTRY'  
  grouplist[grouplist$groupname=='COUNTRY', ]$ADHEADNAME = 'CEO'  
  
  grouplist[grouplist$groupname %in% c('NORTH', 'SOUTH'), ]$LEVEL = 'TERRITORY'  
  
  grouplist[grouplist$groupname %in% c('NORTH'), ]$ADHEADNAME = 'LỤC TÀI BA' 
  grouplist[grouplist$groupname %in% c('SOUTH'), ]$ADHEADNAME = 'NGUYỄN VĂN VŨ'  
  
  grouplist[grouplist$groupname %in% c('NORTH'), ]$TERRITORY = 'NORTH'  
  grouplist[grouplist$groupname %in% c('SOUTH'), ]$TERRITORY = 'SOUTH'  
  
  grouplist[grouplist$TERRITORY %in% c('NORTH'), 'TERRITORYHEAD'] = 'BD000088'
  grouplist[grouplist$TERRITORY %in% c('SOUTH'), 'TERRITORYHEAD'] = 'BD000049'
  
  grouplist = grouplist %>% 
    dplyr::filter(!is.na(LEVEL))
  # grouplist = data.frame(groupname = unique(re$segment))%>% dplyr::arrange(groupname)
  fill_excel_column1.4_no_fmt(
    df = grouplist,
    sheetname = "Sales Chart",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 2,
    template = excelFile,
    result_file = excelFile
  ) 
 
}
  
# does not show warnings
# options (warn=-1)
bssdt = as.Date('2017-11-30')
genlion_final_dt = as.Date('2017-09-30')
excelFile = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_PERFORMANCE_REPORT_%s_dynamic.xlsm", strftime(bssdt,'%Y-%m-%d'))
excelFile_m = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_PERFORMANCE_REPORT_%s_dynamic.xlsm", strftime(bssdt,'%Y-%m-%d'))
exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\template_MONTHLY_AGENCY_PERFORMANCE_REPORT_2017-11-16_dynamic_mcr.xlsm"
# exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\template_MONTHLY_AGENCY_PERFORMANCE_REPORT_2017-11-16_dynamic.xlsx"

create_report(bssdt, genlion_final_dt, exceltemplate, excelFile)
# rm(list=ls())
