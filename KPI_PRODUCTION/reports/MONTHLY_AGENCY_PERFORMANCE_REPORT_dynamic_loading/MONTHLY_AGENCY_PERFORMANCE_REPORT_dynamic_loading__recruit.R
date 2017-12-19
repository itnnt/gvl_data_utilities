get_group_recruit_monthly <- function(yyyy) {
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  newrecruit_agents = do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    new_recruit_individual(d, dbfile)  
  })
  )
  group_data_n(newrecruit_agents, 'NEWRECRUIT') %>% 
    tidyr::spread(time_view, NEWRECRUIT)
}
get_group_active_recruit_leader_monthly <- function(yyyy, ad) {
  newrecruit_agents = do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    active_recruit_leader_individual(d, dbfile) %>%  
      merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
      merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
      merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
      merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
  })
  )
  count_group_by(df = newrecruit_agents, f1='time_view', summarise_field_name = 'NEWRECRUIT') %>% 
    dplyr::mutate(index = paste(segment, 'RECRUITMENT','#active leader', sep = '-')) %>% 
    tidyr::spread(time_view, NEWRECRUIT)
}
get_group_recruitment_monthly <- function(yyyy, ad) {
  newrecruit_agents = do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    new_recruit_individual(d, dbfile) 
    # dplyr::filter(!is.na(TERRITORY))
  })
  ) %>%  
    dplyr::mutate(SEG = ifelse(
    !is.na(SERVICING_AGENT) & toupper(SERVICING_AGENT)=='YES', 'SA', 
    ifelse(!is.na(STAFFCD) & STAFFCD!='' & STAFFCD %in% c('SA','SUM', 'SBM'), STAFFCD, 
           AGENT_DESIGNATION)
  )) %>%  
    count_group_by(., f1='time_view', f2='SEG', summarise_field_name='RECRUITMENT') 
}
