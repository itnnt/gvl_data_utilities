load_data <- function(bssdt, rowindx) {
  ape_row_index = 7
  active_ratio_row_index = 7
  fyp_row_index = 8
  active_ratio_sa_excl_row_index = 8
  case_row_index = 9
  casesize_row_index = 9
  new_recruite_row_index = 10
  caseperactive_row_index = 10
  manpower_row_index = 11
  apeperactive_row_index = 11
  m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
  y1 = as.Date(sprintf('%s-12-31', as.numeric(strftime(bssdt, '%Y'))-1))
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  
  active_agents = active_individual(bssdt, dbfile) %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  active_agents_sa_excl = active_individual_sa_excl(bssdt, dbfile) %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  active_agents_ytd = active_individual_fullyear(strftime(bssdt, '%Y'), dbfile) 
  active_agents_ytd_y1 = active_individual_fullyear(strftime(y1, '%Y'), dbfile) 
  active_agents_ytd_sa_excl = active_individual_fullyear_sa_excl(strftime(bssdt, '%Y'), dbfile) 
  
  manpower = get_manpower_individual(bssdt, dbfile) 
  # %>%  
  #   merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  manpower_sa_excl = get_manpower_individual_sa_excl(bssdt, dbfile)  
  manpower_total_fullyear = get_manpower_total_fullyear(strftime(bssdt, '%Y'), dbfile) 
  manpower_total_fullyear_y1 = get_manpower_total_fullyear(strftime(y1, '%Y'), dbfile) 
  manpower_total_fullyear_sa_excl = get_manpower_total_fullyear_sa_excl(strftime(bssdt, '%Y'), dbfile) 
  
  manpower_M1 = get_manpower_individual(m1, dbfile) 
  # %>%  
  #   merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  manpower_Y1 = get_manpower_individual(y1, dbfile) 
  # %>%  
  #   merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  
  manpower_sa_excl_M1 = manpower_M1 %>% 
    dplyr::filter(!((!is.na(SERVICING_AGENT) & SERVICING_AGENT=='Yes') | STAFFCD=='SA'))
  manpower_sa_excl_Y1 = manpower_Y1 %>% 
    dplyr::filter(!((!is.na(SERVICING_AGENT) & SERVICING_AGENT=='Yes') | STAFFCD=='SA'))
  
  newrecruit_agents = new_recruit_individual(bssdt, dbfile)  
  newrecruit_agents_ytd = new_recruit_individual_fullyear(strftime(bssdt, '%Y'), dbfile) 
  
  ape = ape_individual_10per_topup_incl(bssdt, dbfile)  
  ape_ytd = ape_individual_10per_topup_incl_fullyear(strftime(bssdt, '%Y'), dbfile)  
  ape_ytd_y1 = ape_individual_10per_topup_incl_fullyear(as.numeric(strftime(bssdt, '%Y'))-1, dbfile)  
  fyp = fyp_individual_10per_topup_incl(bssdt, dbfile)  
  fyp_ytd = fyp_individual_10per_topup_incl_fullyear(strftime(bssdt, '%Y'), dbfile)  
  case = case_individual(bssdt, dbfile) %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  case_ytd = case_individual_fullyear(strftime(bssdt,'%Y'), dbfile)  
  case_ytd_y1 = case_individual_fullyear(as.numeric(strftime(bssdt, '%Y'))-1, dbfile) 
  
  group_manpower = group_data_n(manpower, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_total_fullyear = manpower_total_fullyear %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_n(., 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_monthly = manpower_total_fullyear %>% 
    group_data_n(., 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_total_fullyear_sa_excl = manpower_total_fullyear_sa_excl %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_n(., 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_sa_excl = group_data_n(manpower_sa_excl, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  
  group_manpower_m1 = group_data_n(manpower_M1, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_y1 = group_data_n(manpower_Y1, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_sa_excl_m1 = group_data_n(manpower_sa_excl_M1, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_sa_excl_y1 = group_data_n(manpower_sa_excl_Y1, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  group_manpower_monthly_y1 = manpower_total_fullyear_y1 %>% 
    group_data_n(., 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
  
  group_active_agents = group_data_n(active_agents, 'ACTIVE')
  group_active_agents_ytd = active_agents_ytd %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_n(., 'ACTIVE') %>% 
    dplyr::mutate(ACTIVE_YTD = ACTIVE) %>% 
    dplyr::select(-ACTIVE)
  group_active_agents_ytd_y1 = active_agents_ytd_y1 %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_n(., 'ACTIVE') %>% 
    dplyr::mutate(ACTIVE_YTD = ACTIVE) %>% 
    dplyr::select(-ACTIVE)
  group_active_agents_monthly = active_agents_ytd %>% 
    group_data_n(., 'ACTIVE') 
  group_active_agents_monthly_y1 = active_agents_ytd_y1 %>% 
    group_data_n(., 'ACTIVE') 
 
  active_growth <<- get_active_growth(group_active_agents_monthly, group_active_agents_monthly_y1)
  group_active_agents_sa_excl = group_data_n(active_agents_sa_excl, 'ACTIVE')
  group_active_agents_ytd_sa_excl = active_agents_ytd_sa_excl %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_n(., 'ACTIVE') %>% 
    dplyr::mutate(ACTIVE_YTD = ACTIVE) %>% 
    dplyr::select(-ACTIVE)
  
  group_active_ratio = get_group_active_ratio(group_active_agents, group_manpower, group_manpower_m1, active_ratio_row_index)
  group_active_ratio_ytd = get_group_active_ratio_ytd(group_active_agents_ytd, group_manpower_total_fullyear, group_manpower_y1, group_manpower, active_ratio_row_index)
  group_active_ratio_monthly = get_group_active_ratio_monthly(strftime(bssdt, '%Y'), ad, active_ratio_row_index, manpower_row_index)
  print('>>> group_active_ratio_monthly_y1 is running')
  group_active_ratio_monthly_y1 = get_group_active_ratio_monthly(as.numeric(strftime(bssdt, '%Y'))-1, ad, active_ratio_row_index, manpower_row_index)
  group_active_ratio_sa_excl = group_active_agents_sa_excl %>% 
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE') %>% 
    merge(
      x = .,
      y = group_manpower_sa_excl %>%  
        dplyr::select('time_view', 'segment', 'group_level', MANPOWER_M0 = 'MANPOWER'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    merge(
      x = .,
      y = group_manpower_sa_excl_m1 %>%  
        dplyr::select('segment', 'group_level', MANPOWER_M1 = 'MANPOWER'),
      by.x = c('segment', 'group_level'),
      by.y = c('segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(ACTIVE_RATIO_EXCL_SA = 2*ACTIVE/(MANPOWER_M0 + MANPOWER_M1)) %>% 
    dplyr::select(-ACTIVE, -MANPOWER_M0, -MANPOWER_M1) %>% 
    dplyr::mutate(index = paste(segment, active_ratio_sa_excl_row_index, sep = ''), row_idx = active_ratio_sa_excl_row_index)
  group_active_ratio_ytd_sa_excl = get_group_active_ratio_ytd_sa_excl(group_active_agents_ytd_sa_excl, group_manpower_total_fullyear_sa_excl, group_manpower_sa_excl_y1, group_manpower_sa_excl, active_ratio_sa_excl_row_index)
  group_newrecruit_agents = group_data_n(newrecruit_agents, 'NEWRECRUIT') %>% 
    dplyr::mutate(index = paste(segment, new_recruite_row_index, sep = ''), row_idx = new_recruite_row_index) #%>% `rownames<-` (.$index)
  group_newrecruit_agents_ytd = newrecruit_agents_ytd %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_n(., 'NEWRECRUIT') %>% 
    dplyr::mutate(index = paste(segment, new_recruite_row_index, sep = ''), row_idx = new_recruite_row_index) %>% 
    dplyr::mutate(NEWRECRUIT_YTD = NEWRECRUIT) %>% 
    dplyr::select(-NEWRECRUIT)
  #%>% `rownames<-` (.$index)
  
  group_ape = group_data_sum(ape, 'APE') %>%
    dplyr::mutate(APE = APE/10^6, index = paste(segment, ape_row_index, sep = ''), row_idx = ape_row_index)# %>% `rownames<-` (.$index)
  group_ape_ytd = ape_ytd %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_sum(., 'APE') %>%
    dplyr::mutate(APE_YTD = APE/10^6, index = paste(segment, ape_row_index, sep = ''), row_idx = ape_row_index) %>% 
    dplyr::select(-APE) # %>% `rownames<-` (.$index)
  group_ape_ytd_y1 = ape_ytd %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_sum(., 'APE') %>%
    dplyr::mutate(APE_YTD = APE/10^6, index = paste(segment, ape_row_index, sep = ''), row_idx = ape_row_index) %>% 
    dplyr::select(-APE) # %>% `rownames<-` (.$index)
  
  group_ape_monthly = group_data_sum(ape_ytd, 'APE') %>%
    dplyr::mutate(APE = APE/10^6, index = paste(segment, ape_row_index, sep = ''), row_idx = ape_row_index) %>% 
    dplyr::filter(!is.na(segment))# %>% `rownames<-` (.$index)
  group_ape_monthly_y1 = group_data_sum(ape_ytd_y1, 'APE') %>%
    dplyr::mutate(APE = APE/10^6, index = paste(segment, ape_row_index, sep = ''), row_idx = ape_row_index)# %>% `rownames<-` (.$index)
  
  ape_growth <<- get_ape_growth(group_ape_monthly, group_ape_monthly_y1)
  
  group_fyp = group_data_sum(fyp, 'FYP') %>% 
    dplyr::mutate(FYP = FYP/10^6, index = paste(segment, fyp_row_index, sep = ''), row_idx = fyp_row_index)# %>% `rownames<-` (.$index)
  group_fyp_ytd = fyp_ytd %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_sum(., 'FYP') %>% 
    dplyr::mutate(FYP_YTD = FYP/10^6, index = paste(segment, fyp_row_index, sep = ''), row_idx = fyp_row_index) %>% 
    dplyr::select(-FYP)
  # %>% `rownames<-` (.$index)
  
  group_case = group_data_sum(case, 'CASE') %>% 
    dplyr::mutate(index = paste(segment, case_row_index, sep = ''), row_idx = case_row_index)# %>% `rownames<-` (.$index)
  group_case_ytd = case_ytd %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_sum(., 'CASE') %>% 
    dplyr::mutate(index = paste(segment, case_row_index, sep = ''), row_idx = case_row_index) %>% 
    dplyr::mutate(CASE_YTD = CASE) %>% 
    dplyr::select(-CASE)
  # %>% `rownames<-` (.$index)
  group_case_ytd_y1 = case_ytd_y1 %>% 
    dplyr::mutate(time_view=strftime(bssdt, '%Y%m')) %>% 
    group_data_sum(., 'CASE') %>% 
    dplyr::mutate(index = paste(segment, case_row_index, sep = ''), row_idx = case_row_index) %>% 
    dplyr::mutate(CASE_YTD = CASE) %>% 
    dplyr::select(-CASE)
  # %>% `rownames<-` (.$index)
  group_case_monthly = case_ytd %>% 
    group_data_sum(., 'CASE') %>% 
    dplyr::mutate(index = paste(segment, case_row_index, sep = ''), row_idx = case_row_index) %>% 
    dplyr::filter(!is.na(segment))
  group_case_monthly_y1 = case_ytd_y1 %>% 
    group_data_sum(., 'CASE') %>% 
    dplyr::mutate(index = paste(segment, case_row_index, sep = ''), row_idx = case_row_index) %>% 
    dplyr::filter(!is.na(segment))
  
  group_casesize = get_group_casesize(group_ape, group_case, casesize_row_index)
  group_casesize_monthly = get_group_casesize_monthly(group_ape_monthly, group_case_monthly, casesize_row_index)
  group_casesize_monthly_y1 = get_group_casesize_monthly(group_ape_monthly_y1, group_case_monthly_y1, casesize_row_index)
  casesize_growth_monthly <<- get_casesize_growth(group_casesize_monthly, group_casesize_monthly_y1)
  group_casesize_ytd = get_group_casesize_ytd(group_ape_ytd, group_case_ytd, casesize_row_index)
  group_casesize_ytd_y1 = get_group_casesize_ytd(group_ape_ytd_y1, group_case_ytd_y1, casesize_row_index)
  casesize_growth_ytd <<- get_casesize_growth_ytd(group_casesize_ytd, group_casesize_ytd_y1)
  
  group_caseperactive = group_active_agents %>% 
    dplyr::select(time_view, segment, group_level, ACTIVE) %>% 
    merge(
      x = .,
      y = group_case %>% dplyr::select(time_view, segment, group_level, CASE),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(CASEPERACTIVE = CASE/ACTIVE) %>% 
    dplyr::select(-ACTIVE, -CASE) %>% 
    dplyr::mutate(index = paste(segment, caseperactive_row_index, sep = ''), row_idx = caseperactive_row_index)
  group_caseperactive_ytd = get_group_caseperactive_ytd(group_active_agents_ytd, group_case_ytd, caseperactive_row_index)
  group_caseperactive_ytd_y1 = get_group_caseperactive_ytd(group_active_agents_ytd_y1, group_case_ytd_y1, caseperactive_row_index)
  
  group_apeperactive = group_active_agents %>% 
    dplyr::select(time_view, segment, group_level, ACTIVE) %>% 
    merge(
      x = .,
      y = group_ape %>% dplyr::select(time_view, segment, group_level, APE),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(APEPERACTIVE = APE/ACTIVE) %>% 
    dplyr::select(-ACTIVE, -APE) %>% 
    dplyr::mutate(index = paste(segment, apeperactive_row_index, sep = ''), row_idx = apeperactive_row_index)
  group_apeperactive_ytd = group_active_agents_ytd %>% 
    dplyr::select(time_view, segment, group_level, ACTIVE_YTD) %>% 
    merge(
      x = .,
      y = group_ape_ytd %>% dplyr::select(time_view, segment, group_level, APE_YTD),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(APEPERACTIVE_YTD = APE_YTD/ACTIVE_YTD) %>% 
    dplyr::select(-ACTIVE_YTD, -APE_YTD) %>% 
    dplyr::mutate(index = paste(segment, apeperactive_row_index, sep = ''), row_idx = apeperactive_row_index)
  
  group_case_per_active_monthly = get_group_case_per_active_segment(group_case_monthly, group_active_agents_monthly, c('time_view','segment','group_level'))
  group_case_per_active_monthly_y1 = get_group_case_per_active_segment(group_case_monthly_y1, group_active_agents_monthly_y1, c('time_view','segment','group_level'))
  group_ape_per_active_monthly = get_group_ape_per_active_segment(group_ape_monthly, group_active_agents_monthly, c('time_view','segment','group_level'))
  group_ape_per_active_monthly_y1 = get_group_ape_per_active_segment(group_ape_monthly_y1, group_active_agents_monthly_y1, c('time_view','segment','group_level'))
  
  case_per_active_growth_monthly <<- get_case_per_active_growth(group_case_per_active_monthly, group_case_per_active_monthly_y1)
  
  # lam
  case_per_active_growth_ytd <<- get_case_per_active_growth_ytd(group_caseperactive_ytd, group_caseperactive_ytd_y1)
  
  re = group_ape %>% 
    merge(
      x = .,
      y = group_fyp,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_fyp_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_ape_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_case,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_case_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_newrecruit_agents,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_newrecruit_agents_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_manpower,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'order_idx', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_active_ratio,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_active_ratio_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_active_ratio_sa_excl,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_active_ratio_ytd_sa_excl,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_casesize,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_casesize_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_caseperactive,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_caseperactive_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_apeperactive,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_apeperactive_ytd,
      by.x = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      by.y = c('time_view', 'segment', 'index', 'group_level', 'row_idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = rbind(
        group_ape_monthly %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, APE) %>% 
          tidyr::spread(time_view, APE) %>% 
          dplyr::mutate(index=paste(segment, 'APE', sep = '-'))
        ,
        group_active_ratio_monthly %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::filter(!is.na(ACTIVE_RATIO)) %>%  
          dplyr::select(segment, time_view, ACTIVE_RATIO) %>% 
          tidyr::spread(time_view, ACTIVE_RATIO) %>% 
          dplyr::mutate(index=paste(segment, 'AR', sep = '-'))
        ,
        group_casesize_monthly %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, CASESIZE) %>% 
          tidyr::spread(time_view, CASESIZE) %>% 
          dplyr::mutate(index=paste(segment, 'CaseSize', sep = '-'))
        ,
        group_case_per_active_monthly %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, CASEPERACTIVE) %>% 
          tidyr::spread(time_view, CASEPERACTIVE) %>% 
          dplyr::mutate(index=paste(segment, 'CaseperActive', sep = '-'))
        ,
        group_ape_per_active_monthly %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, APEPERACTIVE) %>% 
          tidyr::spread(time_view, APEPERACTIVE) %>% 
          dplyr::mutate(index=paste(segment, 'APEperActive', sep = '-'))
        ,
        group_manpower_monthly %>% 
          dplyr::filter(!is.na(segment) & segment!='DUMMY') %>%  
          dplyr::select(segment, time_view, MANPOWER) %>% 
          tidyr::spread(time_view, MANPOWER) %>% 
          dplyr::mutate(index=paste(segment, 'MP', sep = '-'))
      ),
      by.x = c('segment', 'index'),
      by.y = c('segment', 'index'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = rbind(
        group_ape_monthly_y1 %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, APE) %>% 
          tidyr::spread(time_view, APE) %>% 
          dplyr::mutate(index=paste(segment, 'APE', sep = '-'))
        ,
        group_active_ratio_monthly_y1 %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::filter(!is.na(ACTIVE_RATIO)) %>%  
          dplyr::select(segment, time_view, ACTIVE_RATIO) %>% 
          tidyr::spread(time_view, ACTIVE_RATIO) %>% 
          dplyr::mutate(index=paste(segment, 'AR', sep = '-'))
        ,
        group_casesize_monthly_y1 %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, CASESIZE) %>% 
          tidyr::spread(time_view, CASESIZE) %>% 
          dplyr::mutate(index=paste(segment, 'CaseSize', sep = '-'))
        ,
        group_case_per_active_monthly_y1 %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, CASEPERACTIVE) %>% 
          tidyr::spread(time_view, CASEPERACTIVE) %>% 
          dplyr::mutate(index=paste(segment, 'CaseperActive', sep = '-'))
        ,
        group_ape_per_active_monthly_y1 %>% 
          dplyr::filter(!is.na(segment)) %>%  
          dplyr::select(segment, time_view, APEPERACTIVE) %>% 
          tidyr::spread(time_view, APEPERACTIVE) %>% 
          dplyr::mutate(index=paste(segment, 'APEperActive', sep = '-'))
        ,
        group_manpower_monthly_y1 %>% 
          dplyr::filter(!is.na(segment) & segment!='DUMMY') %>%  
          dplyr::select(segment, time_view, MANPOWER) %>% 
          tidyr::spread(time_view, MANPOWER) %>% 
          dplyr::mutate(index=paste(segment, 'MP', sep = '-'))
      ),
      by.x = c('segment', 'index'),
      by.y = c('segment', 'index'),
      all = T
    ) 
  re
}
load_data_detail_in_segment <- function(bssdt, genlion_final_dt) {
  m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  agent_insegment = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, genlion_final_dt, dbfile, index_from=7)
  agent_insegment_M1 = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(m1, genlion_final_dt, dbfile, index_from=7)
  
  active_agents = active_individual(bssdt, dbfile) %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=agent_insegment, by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  manpower = get_manpower_individual(bssdt, dbfile) %>%  
    # merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=agent_insegment, by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  manpower_M1 = get_manpower_individual(m1, dbfile) %>%  
    # merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=agent_insegment_M1, by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  newrecruit_agents = new_recruit_individual(bssdt, dbfile) %>% 
    merge(x=.,y=agent_insegment, by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T)
  
  # ape = ape_individual(bssdt, dbfile) %>%  
  #   merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
  #   merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
  #   dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
  #   merge(x=.,y=agent_insegment, by.x='AGCODE', by.y='AGENT_CODE', all.x=T)
   
  ape = ape_individual_10per_topup_incl(bssdt, dbfile) %>%
    merge(x=.,y=agent_insegment, by.x='AGCODE', by.y='AGENT_CODE', all.x=T)
  
  fyp = fyp_individual(bssdt, dbfile) %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=agent_insegment, by.x='AGCODE', by.y='AGENT_CODE', all.x=T)
  
  case = case_individual(bssdt, dbfile) %>%  
    merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=agent_insegment, by.x='AGCODE', by.y='AGENT_CODE', all.x=T)
  
  group_manpower = group_data_detail_in_segment_n(manpower, 'MANPOWER') %>%
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(index = paste(segment, 'Segmentation', idx, sep = '-'))# %>% `rownames<-` (.$index)
  
  group_manpower_m1 = group_data_detail_in_segment_n(manpower_M1, 'MANPOWER')%>%
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(index = paste(segment, 'Segmentation', idx, sep = '-'))# %>% `rownames<-` (.$index)
  
  group_active_agents = group_data_detail_in_segment_n(active_agents, 'ACTIVE') %>%
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(index = paste(segment, 'Segmentation', idx, sep = '-')) %>% `rownames<-` (.$index)
  
  group_newrecruit_agents = group_data_detail_in_segment_n(newrecruit_agents, 'NEWRECRUIT') %>%
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(index = paste(segment, 'Segmentation', idx, sep = '-')) %>% `rownames<-` (.$index)
  
  group_ape = group_data_detail_in_segment_sum(ape, 'APE') %>% 
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(APE = APE/10^6, index = paste(segment, 'Segmentation', idx, sep = '-')) %>% `rownames<-` (.$index)
  
  group_fyp = group_data_detail_in_segment_sum(fyp, 'FYP') %>% 
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(FYP = FYP/10^6, index = paste(segment, 'Segmentation', idx, sep = '-')) %>% `rownames<-` (.$index)
  
  group_case = group_data_detail_in_segment_sum(case, 'CASE') %>% 
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(index = paste(segment, 'Segmentation', idx, sep = '-')) %>% `rownames<-` (.$index)
  
  group_active_ratio_insegment = get_group_active_ratio_insegment(group_active_agents, group_manpower, group_manpower_m1)
  
  group_casesize = get_group_casesize_segment(group_ape, group_case, c('time_view', 'segment', 'SEG', 'index'))
  group_case_per_active = get_group_case_per_active_segment(group_case, group_active_agents, join_vars=c('time_view', 'segment', 'SEG', 'index'))
  
  re = group_ape %>% 
    merge(
      x = .,
      y = group_fyp,
      by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_case,
      by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_newrecruit_agents,
      by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_active_agents,
      by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_manpower,
      by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_active_ratio_insegment,
      by.x = c('time_view', 'segment', 'index', 'SEG'),
      by.y = c('time_view', 'segment', 'index', 'SEG'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_casesize,
      by.x = c('time_view', 'segment', 'index', 'SEG'),
      by.y = c('time_view', 'segment', 'index', 'SEG'),
      all = T
    ) %>% 
    merge(
      x = .,
      y = group_case_per_active,
      by.x = c('time_view', 'segment', 'index', 'SEG'),
      by.y = c('time_view', 'segment', 'index', 'SEG'),
      all = T
    ) 
  re %>% dplyr::filter(!is.na(segment) & !is.na(time_view))
}
load_data_detail_in_segment_fullyear <- function(yyyy, genlion_final_dt) {
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    # print(d)
    load_data_detail_in_segment(d, genlion_final_dt)
  })
  )
}
# delete ----
# load_data_detail_in_genlion <- function(bssdt, genlion_final_dt) {
#   territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
#   ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
#   agent_insegment = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, genlion_final_dt, dbfile, index_from=13)
#   
#   fyp = fyp_individual_10per_topup_incl(bssdt, dbfile) %>%  
#     merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
#     merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=agent_insegment, by.x='AGCODE', by.y='AGENT_CODE', all.x=T)
#   
#   case = case_individual_10per_topup_incl(bssdt, dbfile) %>%  
#     merge(x=.,y=territory, by.x='REGIONCD', by.y='REGION_CODE', all.x=T) %>% 
#     merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
#     dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
#     merge(x=.,y=agent_insegment, by.x='AGCODE', by.y='AGENT_CODE', all.x=T)
#   
#   group_fyp = group_data_detail_in_segment_sum(fyp, 'FYP') %>% 
#     dplyr::mutate(FYP = FYP/10^6, index = paste(segment, idx, sep = '')) %>% `rownames<-` (.$index)
#   
#   group_case = group_data_detail_in_segment_sum(case, 'CASE') %>% 
#     dplyr::mutate(index = paste(segment, idx, sep = '')) %>% `rownames<-` (.$index)
#   
#   re = group_ape %>% 
#     merge(
#       x = .,
#       y = group_fyp,
#       by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       all = T
#     ) %>% 
#     merge(
#       x = .,
#       y = group_case,
#       by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       all = T
#     ) %>% 
#     merge(
#       x = .,
#       y = group_newrecruit_agents,
#       by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       all = T
#     ) %>% 
#     merge(
#       x = .,
#       y = group_active_agents,
#       by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       all = T
#     ) %>% 
#     merge(
#       x = .,
#       y = group_manpower,
#       by.x = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       by.y = c('time_view', 'segment', 'index', 'SEG', 'idx'),
#       all = T
#     ) 
#   re
# }