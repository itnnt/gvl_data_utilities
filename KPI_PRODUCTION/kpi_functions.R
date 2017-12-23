manpower_in_segment <- function(bssdt) {
  m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
  # territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  agent_insegment = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, bssdt, dbfile, index_from=7)
  manpower = get_manpower_individual(bssdt, dbfile) %>%  
    # merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    # merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    # dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=agent_insegment, by.x='AGENT_CODE', by.y='AGENT_CODE', all.x=T) %>% 
    dplyr::filter(TEAMCD != 'DUMMY')
  group_manpower = group_data_detail_in_segment_n(manpower, 'MANPOWER') %>%
    dplyr::filter(!is.na(segment)) %>%
    dplyr::mutate(index = paste(segment, 'Segmentation', idx, sep = '-')) %>% 
    dplyr::filter(!is.na(segment) & !is.na(time_view) & segment != 'DUMMY')
  group_manpower
}


manpower_designation_segment <- function(bssdt) {
  m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  manpower = get_manpower_individual(bssdt, dbfile) 
  group_manpower = manpower %>% 
    dplyr::mutate(SEG = ifelse(
                              !is.na(SERVICING_AGENT) & toupper(SERVICING_AGENT)=='YES', 'SA', 
                              ifelse(!is.na(STAFFCD) & STAFFCD!='' & STAFFCD %in% c('SA','SUM', 'SBM'), STAFFCD, 
                                     AGENT_DESIGNATION)
                              )) %>% 
    count_group_by(., f1='time_view', f2='SEG', summarise_field_name='MANPOWER') %>% 
    dplyr::filter(!is.na(segment) & !is.na(time_view) & segment != 'DUMMY')
  group_manpower
}

get_manpower_individual <- function(bssdt, dbfile){
  if(!inherits(bssdt, "Date")) {
    message ("date parameter should be a Date type")
    stop()
  }
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  mp = get_Manpower(bssdt, included_ter_ag = F, dbfile) %>% 
    dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
    dplyr::mutate(MDIFF = as.integer(round((
      as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)
    ) * 12)))
  mp %>%  
    merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    dplyr::filter(is.na(TEAMCD) | TEAMCD != 'DUMMY')
}
