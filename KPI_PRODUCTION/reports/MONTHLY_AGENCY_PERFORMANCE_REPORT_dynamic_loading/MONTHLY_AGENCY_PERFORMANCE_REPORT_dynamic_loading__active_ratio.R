get_group_active_ratio <- function(group_active_agents, group_manpower, group_manpower_m1, active_ratio_row_index){
  group_active_agents %>% 
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE') %>% 
    merge(
      x = .,
      y = group_manpower %>%  
        dplyr::select('time_view', 'segment', 'group_level', MANPOWER_M0 = 'MANPOWER'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    merge(
      x = .,
      y = group_manpower_m1 %>%  
        dplyr::select('segment', 'group_level', MANPOWER_M1 = 'MANPOWER'),
      by.x = c('segment', 'group_level'),
      by.y = c('segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(ACTIVE_RATIO = 2*ACTIVE/(MANPOWER_M0 + MANPOWER_M1)) %>% 
    dplyr::select(-ACTIVE, -MANPOWER_M0, -MANPOWER_M1) %>% 
    dplyr::mutate(index = paste(segment, active_ratio_row_index, sep = ''), row_idx = active_ratio_row_index)
}
get_group_active_ratio_ytd <- function(group_active_agents_ytd, group_manpower_total_fullyear, group_manpower_y1, group_manpower, active_ratio_row_index) {
  group_active_agents_ytd %>% 
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE_YTD') %>% 
    merge(
      x = .,
      y = group_manpower_total_fullyear %>%  
        dplyr::select('time_view', 'segment', 'group_level', MANPOWER_Y0 = 'MANPOWER'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    merge(
      x = .,
      y = group_manpower %>%  
        dplyr::select('time_view', 'segment', 'group_level', MANPOWER_M0 = 'MANPOWER'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    merge(
      x = .,
      y = group_manpower_y1 %>%  
        dplyr::select('segment', 'group_level', MANPOWER_Y1 = 'MANPOWER'),
      by.x = c('segment', 'group_level'),
      by.y = c('segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(ACTIVE_RATIO_YTD = 2*ACTIVE_YTD/(ifelse(is.na(MANPOWER_Y0), 0, MANPOWER_Y0)*2+ifelse(is.na(MANPOWER_Y1), 0, MANPOWER_Y1)-ifelse(is.na(MANPOWER_M0), 0, MANPOWER_M0))) %>% 
    dplyr::select(-ACTIVE_YTD, -MANPOWER_Y0, -MANPOWER_Y1, -MANPOWER_M0) %>% 
    dplyr::mutate(index = paste(segment, active_ratio_row_index, sep = ''), row_idx = active_ratio_row_index)
}
get_group_active_ratio_monthly <- function(yyyy, ad, active_ratio_row_index, manpower_row_index) {
  do.call(rbind, lapply(generate_last_day_of_month(yyyy), function(d) {
    active_agents = active_individual(d, dbfile) %>%  
      merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
      merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
      merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
      merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
      dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
    group_active_agents = group_data_n(active_agents, 'ACTIVE')
    manpower = get_manpower_individual(d, dbfile) 
    # %>%  
    #   merge(x=.,y=ad, by.x='TEAMCD', by.y='ADCODE', all.x=T) %>% 
    #   dplyr::mutate(TEAMCD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    #   merge(x=.,y=ad, by.x='OFFICECD', by.y='ADCODE', all.x=T) %>% 
    #   dplyr::mutate(OFFICECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    #   merge(x=.,y=ad, by.x='ZONECD', by.y='ADCODE', all.x=T) %>% 
    #   dplyr::mutate(ZONECD=ADNAME) %>% dplyr::select(-ADNAME) %>% 
    #   merge(x=.,y=ad, by.x='REGIONCD', by.y='ADCODE', all.x=T) %>% 
    #   dplyr::mutate(REGIONCD=ADNAME) %>% dplyr::select(-ADNAME) 
    group_manpower = group_data_n(manpower, 'MANPOWER')%>%
      dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
    m1 = get_last_day_of_m1(strftime(d, '%Y'), strftime(d, '%m'))
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
    group_manpower_m1 = group_data_n(manpower_M1, 'MANPOWER')%>%
      dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index)# %>% `rownames<-` (.$index)
    get_group_active_ratio(group_active_agents, group_manpower, group_manpower_m1, active_ratio_row_index)
  })
  )
}
get_group_active_ratio_ytd_sa_excl <- function(group_active_agents_ytd_sa_excl, group_manpower_total_fullyear_sa_excl, group_manpower_sa_excl_y1, group_manpower_sa_excl, active_ratio_sa_excl_row_index) {
  group_active_agents_ytd_sa_excl %>% 
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE_YTD') %>% 
    merge(
      x = .,
      y = group_manpower_total_fullyear_sa_excl %>%  
        dplyr::select('time_view', 'segment', 'group_level', MANPOWER_Y0 = 'MANPOWER'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
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
      y = group_manpower_sa_excl_y1 %>%  
        dplyr::select('segment', 'group_level', MANPOWER_Y1 = 'MANPOWER'),
      by.x = c('segment', 'group_level'),
      by.y = c('segment', 'group_level'),
      all = TRUE
    ) %>% 
    # dplyr::mutate(ACTIVE_RATIO_YTD_SA_EXCL = 2*ACTIVE_YTD/(MANPOWER_Y0*2+MANPOWER_Y1-MANPOWER_M0)) %>% 
    dplyr::mutate(ACTIVE_RATIO_YTD_SA_EXCL = 2*ACTIVE_YTD/(ifelse(is.na(MANPOWER_Y0), 0, MANPOWER_Y0)*2+ifelse(is.na(MANPOWER_Y1), 0, MANPOWER_Y1)-ifelse(is.na(MANPOWER_M0), 0, MANPOWER_M0))) %>% 
    dplyr::select(-ACTIVE_YTD, -MANPOWER_Y0, -MANPOWER_Y1, -MANPOWER_M0) %>% 
    dplyr::mutate(index = paste(segment, active_ratio_sa_excl_row_index, sep = ''), row_idx = active_ratio_sa_excl_row_index)
}
get_group_active_ratio_insegment <- function(group_active_agents, group_manpower, group_manpower_m1){
  group_active_agents %>% 
    dplyr::select('time_view', 'segment', 'SEG', 'index', 'ACTIVE') %>% 
    merge(
      x = .,
      y = group_manpower %>%  
        dplyr::select('time_view', 'segment', 'SEG', 'index', MANPOWER_M0 = 'MANPOWER'),
      by.x = c('time_view', 'segment', 'SEG', 'index'),
      by.y = c('time_view', 'segment', 'SEG', 'index'),
      all = TRUE
    ) %>% 
    merge(
      x = .,
      y = group_manpower_m1 %>%  
        dplyr::select('segment', 'SEG', 'index', MANPOWER_M1 = 'MANPOWER'),
      by.x = c('segment', 'SEG', 'index'),
      by.y = c('segment', 'SEG', 'index'),
      all = TRUE
    ) %>% 
    dplyr::mutate(ACTIVE_RATIO = 2*ACTIVE/(MANPOWER_M0 + MANPOWER_M1)) 
    # dplyr::select(-ACTIVE, -MANPOWER_M0, -MANPOWER_M1)
    # dplyr::mutate(index = paste(segment, active_ratio_row_index, sep = ''), row_idx = active_ratio_row_index)
}


# get_group_active_ratio_insegment(group_active_agents, group_manpower, group_manpower_m1) %>% dplyr::filter(segment == 'COUNTRY') %>%  View
# group_active_ratio_insegment %>% dplyr::filter(segment == 'COUNTRY') %>%  View
# group_manpower_m1 %>% dplyr::filter(segment=='COUNTRY') %>% View
