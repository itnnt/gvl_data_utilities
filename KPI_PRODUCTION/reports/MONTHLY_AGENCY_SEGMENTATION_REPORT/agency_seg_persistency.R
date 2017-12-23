get_group_persistency <- function(bssdt) {
  territory = territory_mapping(dbfile) %>% dplyr::select(-REGION_NAME)
  ad = AD_LIST(dbfile) %>% dplyr::select( ADCODE, ADNAME)
  group_persistency = rbind(
    # office
    get_persistency_office(bssdt, dbfile) %>%  
    merge(x=.,y=ad, by.x='segment', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(segment=ADNAME, level='OFFICE') %>% dplyr::select(-ADNAME) %>% 
    dplyr::filter(!is.na(segment) & segment != 'DUMMY')
    ,
    # region
    get_persistency_region(bssdt, dbfile) %>%  
    merge(x=.,y=ad, by.x='segment', by.y='ADCODE', all.x=T) %>% 
    dplyr::mutate(segment=ADNAME, level='REGION') %>% dplyr::select(-ADNAME) %>% 
    dplyr::filter(!is.na(segment) & segment != 'DUMMY')
    ,
    # territory
    get_persistency_region(bssdt, dbfile) %>%  
    merge(x=.,y=territory, by.x='segment', by.y='REGION_CODE') %>% 
    dplyr::mutate(segment=TERRITORY, level='TERRITORY') %>% dplyr::select(-TERRITORY) %>% 
    dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
    dplyr::group_by(segment, time_view, level) %>% 
    dplyr::summarise(current_ape = sum(current_ape, na.rm = T), 
                     original_ape = sum(original_ape, na.rm = T), 
                     value = sum(current_ape, na.rm = T)/sum(original_ape, na.rm = T)) %>% 
    dplyr::ungroup()
    ,
    # country
    get_persistency_region(bssdt, dbfile) %>%  
    merge(x=.,y=territory, by.x='segment', by.y='REGION_CODE') %>% 
    dplyr::mutate(segment=TERRITORY, level='COUNTRY') %>% dplyr::select(-TERRITORY) %>% 
    dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
    dplyr::group_by(segment = 'COUNTRY', time_view, level) %>% 
    dplyr::summarise(current_ape = sum(current_ape, na.rm = T), 
                     original_ape = sum(original_ape, na.rm = T), 
                     value = sum(current_ape, na.rm = T)/sum(original_ape, na.rm = T)) %>% 
    dplyr::ungroup()

  ) %>% 
    dplyr::mutate(time_view = strftime(as.Date(strptime(time_view, '%Y-%m-%d')), '%Y%m'))
  
# <<<<<<< HEAD
  group_persistency #%>% tidyr::spread(time_view, value)
# =======
#   group_persistency %>% tidyr::spread(time_view, value)
# >>>>>>> origin/master
}

get_segment_persistency <- function(bssdt) {
  agent_insegment = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, bssdt, dbfile, index_from=7)
  rbind(
  # territory
  get_persistency_individual(bssdt, dbfile) %>% 
    merge(x=.,y=agent_insegment, by.x='segment', by.y='AGENT_CODE', all.x=T) %>% 
    dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
    dplyr::filter(!is.na(territory) & territory != 'DUMMY') %>% 
    dplyr::group_by(time_view, territory, level=territory, idx, segment=SEG) %>% 
    dplyr::summarise(current_ape = sum(current_ape, na.rm = T), 
                     original_ape = sum(original_ape, na.rm = T), 
                     value = sum(current_ape, na.rm = T)/sum(original_ape, na.rm = T)) %>% 
    dplyr::ungroup()
  ,
  # country
  get_persistency_individual(bssdt, dbfile) %>% 
    merge(x=.,y=agent_insegment, by.x='segment', by.y='AGENT_CODE', all.x=T) %>% 
    dplyr::filter(!is.na(segment) & segment != 'DUMMY') %>% 
    dplyr::filter(!is.na(territory) & territory != 'DUMMY') %>% 
    dplyr::group_by(time_view, territory='COUNTRY', level='COUNTRY', idx, segment=SEG) %>% 
    dplyr::summarise(current_ape = sum(current_ape, na.rm = T), 
                     original_ape = sum(original_ape, na.rm = T), 
                     value = sum(current_ape, na.rm = T)/sum(original_ape, na.rm = T)) %>% 
    dplyr::ungroup()
  ) %>% 
    dplyr::mutate(time_view = strftime(as.Date(strptime(time_view, '%Y-%m-%d')), '%Y%m')) %>% 
    dplyr::select( segment, current_ape, original_ape, level, idx, time_view, value) %>% 
# <<<<<<< HEAD
    #tidyr::spread(time_view, value) %>% 
    dplyr::arrange(idx) 
# =======
#     tidyr::spread(time_view, value) %>% 
#     dplyr::arrange(idx) %>% 
#     dplyr::select(-idx)
# >>>>>>> origin/master
}

get_persistency_y1_office(bssdt, dbfile)
get_persistency_y2_office(bssdt, dbfile)


