get_group_ape_per_active_segment <- function(group_ape, group_active_agents, join_vars){
  group_ape %>% 
    merge(
      x = .,
      y = group_active_agents,
      by.x = join_vars, #c('time_view', 'segment', 'group_level'),
      by.y = join_vars, #c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(APEPERACTIVE = APE/ACTIVE) %>% 
    dplyr::select(-ACTIVE, -APE) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) # remove columns that have sufix is ".x" or '.y' (if any)
}
get_ape_growth <- function(ape, ape1) {
  # ape = group_ape_monthly
  # ape1 = group_ape_monthly_y1
  ape_ytd = ape %>%
    dplyr::select('time_view', 'segment', 'group_level', 'APE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    dplyr::mutate(time_view = substr(time_view, 1,4)) 
   
  ape_ytd1 = ape1 %>%
    dplyr::select('time_view', 'segment', 'group_level', 'APE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    dplyr::mutate(time_view = substr(time_view, 1,4)) 
  
  growth_ytd = ape_ytd %>%
    merge(
      x = .,
      y = ape_ytd1,
      by.x = c('time_view_month' ,'segment', 'group_level'),
      by.y = c('time_view_month' ,'segment', 'group_level'),
      all.x = T
    ) %>% 
    dplyr::group_by(time_view = time_view.x, segment, group_level, time_view_month=time_view.x) %>%
    dplyr::summarise(APE.x = sum(APE.x, na.rm = T), APE.y = sum(APE.y, na.rm = T)) %>%
    dplyr::ungroup()
  
  growth = ape %>%
    dplyr::select('time_view', 'segment', 'group_level', 'APE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    merge(
      x = .,
      y = ape1 %>%
        dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
        dplyr::select('time_view_month', 'segment', 'group_level', 'APE'),
      by.x = c('time_view_month', 'segment', 'group_level'),
      by.y = c('time_view_month', 'segment', 'group_level'),
      all.x = T
    ) 
  
  rbind(growth,growth_ytd) %>% 
    dplyr::mutate('APE Growth' = 
                    ifelse(!is.na(APE.y) & APE.y != 0, APE.x/APE.y-1, 
                           ifelse(!is.na(APE.x) & APE.x != 0, 1, 0))) %>% 
    dplyr::select(segment, time_view, 'APE Growth') %>% 
    tidyr::spread(time_view, 'APE Growth') %>% 
    dplyr::mutate(index = paste(segment, 'APE Growth', sep='-'))
}
