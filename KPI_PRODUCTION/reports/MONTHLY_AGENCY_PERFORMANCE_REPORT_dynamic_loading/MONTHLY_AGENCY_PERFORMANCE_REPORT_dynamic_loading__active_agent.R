get_active_growth <- function(active, active1) {
  # active=group_active_agents_monthly
  # active1=group_active_agents_monthly_y1
  active_ytd = active %>%
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    dplyr::mutate(time_view = substr(time_view, 1,4)) 
  
  active_ytd1 = active1 %>%
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    dplyr::mutate(time_view = substr(time_view, 1,4)) 
  
  growth_ytd = active_ytd %>%
    merge(
      x = .,
      y = active_ytd1,
      by.x = c('time_view_month' ,'segment', 'group_level'),
      by.y = c('time_view_month' ,'segment', 'group_level'),
      all.x = T
    ) %>% 
    dplyr::group_by(time_view = time_view.x, segment, group_level, time_view_month=time_view.x) %>%
    dplyr::summarise(ACTIVE.x = sum(ACTIVE.x, na.rm = T), ACTIVE.y = sum(ACTIVE.y, na.rm = T)) %>%
    dplyr::ungroup()
  
  growth = active %>%
    dplyr::select('time_view', 'segment', 'group_level', 'ACTIVE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    merge(
      x = .,
      y = active1 %>%
        dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
        dplyr::select('time_view_month', 'segment', 'group_level', 'ACTIVE'),
      by.x = c('time_view_month', 'segment', 'group_level'),
      by.y = c('time_view_month', 'segment', 'group_level'),
      all.x = T
    )
  
  rbind(growth, growth_ytd) %>% 
    dplyr::mutate('Active Growth' = 
                    ifelse(!is.na(ACTIVE.y) & ACTIVE.y != 0, ACTIVE.x/ACTIVE.y-1, 
                           ifelse(!is.na(ACTIVE.x) & ACTIVE.x != 0, 1, 0))) %>% 
    dplyr::select(segment, time_view, 'Active Growth') %>% 
    dplyr::filter(!is.na(segment)) %>% 
    tidyr::spread(time_view, 'Active Growth') %>% 
    dplyr::mutate(index = paste(segment, 'Active Growth', sep='-'))
}
